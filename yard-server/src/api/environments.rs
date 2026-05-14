use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use std::sync::Arc;

use super::dashboard::ApiState;
use super::error::ApiError;
use crate::db::AccountHealth;
use crate::types::*;

pub fn environments_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/envs", get(get_environments))
        .route("/api/envs/health", get(get_health))
        .route("/api/envs/{env}/regions", get(get_regions))
        .with_state(state)
}

/// GET /api/envs - Returns environment summaries with health and drift aggregation.
async fn get_environments(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<EnvironmentListData>, ApiError> {
    let envs = state
        .db
        .list_environments()
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list environments: {e}")))?;

    let all_health = state
        .db
        .list_all_account_health()
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list account health: {e}")))?;

    // Load drift cache for per-env drift counts.
    let drift_data = load_drift_cache(&state).await;

    let mut summaries = Vec::with_capacity(envs.len());
    for env in &envs {
        let drift_count = drift_data
            .as_ref()
            .map(|d| {
                d.items
                    .iter()
                    .filter(|item| item.environment == env.name)
                    .count() as u32
            })
            .unwrap_or(0);

        let health_status = derive_env_health(&env.name, &all_health);

        summaries.push(EnvironmentSummary {
            name: env.name.clone(),
            regions: env.regions.clone(),
            job_count: env.job_count,
            drift_count,
            health_status,
        });
    }

    let total_environments = summaries.len() as u32;
    let total_accounts = all_health.len() as u32;
    let connected_accounts = all_health
        .iter()
        .filter(|h| h.status == "healthy")
        .count() as u32;

    Ok(Json(EnvironmentListData {
        environments: summaries,
        total_environments,
        connected_accounts,
        total_accounts,
    }))
}

/// GET /api/envs/:env/regions - Returns region details with job/DAG split and drift items.
async fn get_regions(
    State(state): State<Arc<ApiState>>,
    Path(env): Path<String>,
) -> Result<Json<Vec<RegionDetailData>>, ApiError> {
    // T-44-01: Reject env names containing '#' to prevent DynamoDB key injection.
    if env.contains('#') {
        return Err(ApiError::BadRequest("invalid environment name".into()));
    }

    let regions = state
        .db
        .list_regions(&env)
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list regions: {e}")))?;

    let all_jobs = state
        .db
        .list_job_summaries_all()
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list job summaries: {e}")))?;

    let drift_data = load_drift_cache(&state).await;

    let mut details = Vec::with_capacity(regions.len());
    for region in &regions {
        let jobs: Vec<_> = all_jobs
            .iter()
            .filter(|j| j.env_name == env && j.region_name == region.name && j.job_type != "dag")
            .cloned()
            .collect();

        let dags: Vec<_> = all_jobs
            .iter()
            .filter(|j| j.env_name == env && j.region_name == region.name && j.job_type == "dag")
            .cloned()
            .collect();

        let drift_items: Vec<DriftItem> = drift_data
            .as_ref()
            .map(|d| {
                d.items
                    .iter()
                    .filter(|item| item.environment == env && item.region == region.name)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        details.push(RegionDetailData {
            env_name: env.clone(),
            region_name: region.name.clone(),
            jobs,
            dags,
            drift_items,
        });
    }

    Ok(Json(details))
}

/// GET /api/envs/health - Returns all account health records.
async fn get_health(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<Vec<AccountHealth>>, ApiError> {
    let health = state
        .db
        .list_all_account_health()
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to list account health: {e}")))?;
    Ok(Json(health))
}

// ---- Helpers ----

/// Load and deserialize the drift cache. Returns None if cache is missing or corrupt
/// (non-fatal -- drift counts will be 0).
async fn load_drift_cache(state: &ApiState) -> Option<DriftData> {
    let cached = state.db.get_cache("drift").await.ok()??;
    serde_json::from_str(&cached).ok()
}

/// Derive a single health status string for an environment.
///
/// No env-to-account mapping exists in the DB schema yet, so we cannot
/// determine per-environment health. Return "unknown" rather than a
/// misleading global aggregate that would mark ALL environments as
/// "degraded" when any single account is unhealthy.
///
/// TODO: When env<->account mapping is added to the schema, filter
/// `all_health` by the environment's associated accounts and compute
/// the real per-env status here.
fn derive_env_health(env_name: &str, all_health: &[AccountHealth]) -> String {
    let _ = (env_name, all_health);
    "unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{test_support::InMemoryDb, Database};
    use axum::extract::State;

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
    async fn get_environments_empty_returns_empty_list() {
        let state = test_state();
        let result = get_environments(State(state)).await.unwrap();
        assert!(result.0.environments.is_empty());
        assert_eq!(result.0.total_environments, 0);
    }

    #[tokio::test]
    async fn get_health_empty_returns_empty_list() {
        let state = test_state();
        let result = get_health(State(state)).await.unwrap();
        assert!(result.0.is_empty());
    }

    #[tokio::test]
    async fn get_environments_with_data() {
        let state = test_state();
        let env = crate::db::Environment {
            name: "production".to_string(),
            regions: vec!["us-east-1".to_string()],
            job_count: 5,
            last_scanned: chrono::Utc::now(),
        };
        state.db.upsert_environment(&env).await.unwrap();

        let result = get_environments(State(state)).await.unwrap();
        assert_eq!(result.0.environments.len(), 1);
        assert_eq!(result.0.environments[0].name, "production");
        assert_eq!(result.0.environments[0].job_count, 5);
        assert_eq!(result.0.total_environments, 1);
    }

    #[tokio::test]
    async fn get_regions_rejects_hash_in_env_name() {
        let state = test_state();
        let result = get_regions(
            State(state),
            Path("bad#env".to_string()),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_regions_empty_env() {
        let state = test_state();
        let result = get_regions(
            State(state),
            Path("nonexistent".to_string()),
        )
        .await
        .unwrap();
        assert!(result.0.is_empty());
    }

    #[tokio::test]
    async fn environments_router_compiles() {
        let state = test_state();
        let _router: Router = environments_router(state);
    }
}
