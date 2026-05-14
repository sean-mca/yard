use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;

use super::dashboard::ApiState;
use super::error::ApiError;
use crate::types::{SearchHit, SearchResult};

/// Maximum results per entity group in search responses (T-44-02).
const MAX_RESULTS_PER_GROUP: usize = 10;

/// Maximum allowed query length (T-44-02).
const MAX_QUERY_LENGTH: usize = 100;

pub fn search_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/search", get(search_handler))
        .with_state(state)
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
}

/// GET /api/search?q=... - Returns grouped search results across environments, jobs, and DAGs.
async fn search_handler(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResult>, ApiError> {
    // T-44-02: Validate query parameter.
    if params.q.is_empty() {
        return Err(ApiError::BadRequest("search query must not be empty".into()));
    }
    if params.q.len() > MAX_QUERY_LENGTH {
        return Err(ApiError::BadRequest(
            format!("search query must not exceed {MAX_QUERY_LENGTH} characters"),
        ));
    }

    let query_lower = params.q.to_lowercase();

    // Fetch environments and jobs in parallel for search composition (D-09).
    let (envs_result, jobs_result) = tokio::join!(
        state.db.list_environments(),
        state.db.list_job_summaries_all()
    );

    let envs = envs_result
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list environments: {e}")))?;
    let all_jobs = jobs_result
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list job summaries: {e}")))?;

    // Filter environments matching query.
    let env_hits: Vec<SearchHit> = envs
        .iter()
        .filter(|e| e.name.to_lowercase().contains(&query_lower))
        .take(MAX_RESULTS_PER_GROUP)
        .map(|e| SearchHit {
            name: e.name.clone(),
            environment: None,
            region: None,
            entity_type: "environment".to_string(),
        })
        .collect();

    // Filter jobs (non-DAG) matching query.
    let job_hits: Vec<SearchHit> = all_jobs
        .iter()
        .filter(|j| j.job_type != "dag")
        .filter(|j| j.name.to_lowercase().contains(&query_lower))
        .take(MAX_RESULTS_PER_GROUP)
        .map(|j| SearchHit {
            name: j.name.clone(),
            environment: Some(j.env_name.clone()),
            region: Some(j.region_name.clone()),
            entity_type: "job".to_string(),
        })
        .collect();

    // Filter DAGs matching query.
    let dag_hits: Vec<SearchHit> = all_jobs
        .iter()
        .filter(|j| j.job_type == "dag")
        .filter(|j| j.name.to_lowercase().contains(&query_lower))
        .take(MAX_RESULTS_PER_GROUP)
        .map(|j| SearchHit {
            name: j.name.clone(),
            environment: Some(j.env_name.clone()),
            region: Some(j.region_name.clone()),
            entity_type: "dag".to_string(),
        })
        .collect();

    Ok(Json(SearchResult {
        environments: env_hits,
        jobs: job_hits,
        dags: dag_hits,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{test_support::InMemoryDb, Database, Environment, JobSummaryEntity};
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    fn test_state() -> Arc<ApiState> {
        use crate::secrets::test_support::InMemorySecretStore;
        let db = Arc::new(InMemoryDb::new());
        let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
        let secret_store: Arc<dyn crate::secrets::SecretStore> =
            Arc::new(InMemorySecretStore::new(std::collections::HashMap::new()));
        Arc::new(ApiState {
            github_token: "t".into(),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            db: db as Arc<dyn Database>,
            event_tx,
            secret_store,
        })
    }

    #[tokio::test]
    async fn search_empty_query_returns_bad_request() {
        let state = test_state();
        let result = search_handler(
            State(state),
            Query(SearchParams { q: String::new() }),
        )
        .await;
        assert!(result.is_err());
        let resp = result.unwrap_err().into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn search_too_long_query_returns_bad_request() {
        let state = test_state();
        let long_query = "a".repeat(MAX_QUERY_LENGTH + 1);
        let result = search_handler(
            State(state),
            Query(SearchParams { q: long_query }),
        )
        .await;
        assert!(result.is_err());
        let resp = result.unwrap_err().into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn search_valid_query_returns_grouped_results() {
        let state = test_state();

        // Seed an environment
        let env = Environment {
            name: "production".to_string(),
            regions: vec!["us-east-1".to_string()],
            job_count: 2,
            last_scanned: chrono::Utc::now(),
        };
        state.db.upsert_environment(&env).await.unwrap();

        // Seed a job and a DAG
        let job = JobSummaryEntity {
            env_name: "production".to_string(),
            region_name: "us-east-1".to_string(),
            name: "etl-production-pipeline".to_string(),
            job_type: "glue".to_string(),
        };
        state.db.upsert_job_summary("production", &job).await.unwrap();

        let dag = JobSummaryEntity {
            env_name: "production".to_string(),
            region_name: "us-east-1".to_string(),
            name: "production-dag".to_string(),
            job_type: "dag".to_string(),
        };
        state.db.upsert_job_summary("production", &dag).await.unwrap();

        let result = search_handler(
            State(state),
            Query(SearchParams { q: "production".to_string() }),
        )
        .await
        .unwrap();

        assert_eq!(result.0.environments.len(), 1);
        assert_eq!(result.0.environments[0].entity_type, "environment");
        assert_eq!(result.0.jobs.len(), 1);
        assert_eq!(result.0.jobs[0].entity_type, "job");
        assert_eq!(result.0.dags.len(), 1);
        assert_eq!(result.0.dags[0].entity_type, "dag");
    }

    #[tokio::test]
    async fn search_case_insensitive() {
        let state = test_state();

        let env = Environment {
            name: "Production".to_string(),
            regions: vec![],
            job_count: 0,
            last_scanned: chrono::Utc::now(),
        };
        state.db.upsert_environment(&env).await.unwrap();

        let result = search_handler(
            State(state),
            Query(SearchParams { q: "production".to_string() }),
        )
        .await
        .unwrap();

        assert_eq!(result.0.environments.len(), 1);
    }

    #[tokio::test]
    async fn search_no_matches_returns_empty_groups() {
        let state = test_state();
        let result = search_handler(
            State(state),
            Query(SearchParams { q: "nonexistent".to_string() }),
        )
        .await
        .unwrap();

        assert!(result.0.environments.is_empty());
        assert!(result.0.jobs.is_empty());
        assert!(result.0.dags.is_empty());
    }

    #[tokio::test]
    async fn search_router_compiles() {
        let state = test_state();
        let _router: Router = search_router(state);
    }
}
