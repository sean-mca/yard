use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{info, warn};

use super::error::ApiError;

use crate::db::{Database, PlanStatus};
use crate::types::*;

const MAX_CACHED_PRS: u8 = 50;

pub struct ApiState {
    pub github_token: String,
    pub repo_owner: String,
    pub repo_name: String,
    pub db: Arc<dyn Database>,
    // Subscribed by Plan 03's WS handler via `event_tx.subscribe()`; sent into by
    // Plan 02's poll loops. Staged here so the channel type-contract and ApiState
    // shape are locked before downstream waves land.
    #[allow(dead_code)]
    pub event_tx: tokio::sync::broadcast::Sender<crate::api::events::Event>,
}

pub fn dashboard_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/dashboard", get(get_dashboard))
        .route("/api/dashboard/cached", get(get_dashboard_cached))
        .with_state(state)
}

#[derive(Deserialize)]
struct PaginationParams {
    page: Option<u32>,
    per_page: Option<u32>,
}

// ---- Direct GitHub fetch (fallback) ----

async fn get_dashboard(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<DashboardData>, ApiError> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(15).min(50);
    let data = fetch_dashboard_data(&state, page, per_page)
        .await
        .map_err(ApiError::GitHubError)?;
    Ok(Json(data))
}

async fn fetch_dashboard_data(
    state: &ApiState,
    page: u32,
    per_page: u32,
) -> Result<DashboardData, String> {
    use octocrab::params;
    use octocrab::Octocrab;

    let octo = Octocrab::builder()
        .personal_token(state.github_token.clone())
        .build()
        .map_err(|e| format!("Failed to build octocrab: {e}"))?;

    let owner = &state.repo_owner;
    let repo = &state.repo_name;

    let open_prs = octo
        .pulls(owner, repo)
        .list()
        .state(params::State::Open)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch open PRs: {e}"))?;

    let open_count = open_prs.items.len() as u32;

    let all_prs = octo
        .pulls(owner, repo)
        .list()
        .state(params::State::All)
        .sort(octocrab::params::pulls::Sort::Updated)
        .direction(octocrab::params::Direction::Descending)
        .per_page(per_page as u8)
        .page(page)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch PRs: {e}"))?;

    let has_more = all_prs.items.len() == per_page as usize;

    let rows = build_pr_rows(state.db.as_ref(), &all_prs.items).await;

    let jobs_tracked = count_job_files(&octo, owner, repo).await.unwrap_or(0);

    info!(
        open_prs = open_count,
        jobs = jobs_tracked,
        total_returned = rows.len(),
        page = page,
        "Dashboard data fetched"
    );

    Ok(DashboardData {
        prs: rows,
        open_prs: open_count,
        jobs_tracked,
        page,
        per_page,
        has_more,
    })
}

// ---- Cache refresh (called by background task + webhook) ----

/// Fetch the most recent 50 PRs from GitHub and store in DynamoDB cache.
pub async fn refresh_dashboard_cache(state: &ApiState) -> Result<DashboardCache, String> {
    use octocrab::params;
    use octocrab::Octocrab;

    let octo = Octocrab::builder()
        .personal_token(state.github_token.clone())
        .build()
        .map_err(|e| format!("Failed to build octocrab: {e}"))?;

    let owner = &state.repo_owner;
    let repo = &state.repo_name;

    let open_prs = octo
        .pulls(owner, repo)
        .list()
        .state(params::State::Open)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch open PRs: {e}"))?;

    let open_count = open_prs.items.len() as u32;

    let all_prs = octo
        .pulls(owner, repo)
        .list()
        .state(params::State::All)
        .sort(octocrab::params::pulls::Sort::Updated)
        .direction(octocrab::params::Direction::Descending)
        .per_page(MAX_CACHED_PRS)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch PRs: {e}"))?;

    let rows = build_pr_rows(state.db.as_ref(), &all_prs.items).await;

    let jobs_tracked = count_job_files(&octo, owner, repo).await.unwrap_or(0);

    let cache = DashboardCache {
        prs: rows,
        open_prs: open_count,
        jobs_tracked,
    };

    // Store in DynamoDB
    let serialized =
        serde_json::to_string(&cache).map_err(|e| format!("Failed to serialize cache: {e}"))?;
    if let Err(e) = state.db.set_cache("dashboard", &serialized).await {
        warn!(error = %e, "Failed to store dashboard cache");
    }

    info!(
        open_prs = open_count,
        jobs = jobs_tracked,
        total_prs = cache.prs.len(),
        "Dashboard cache refreshed"
    );

    Ok(cache)
}

// ---- Cached read (paginated on read) ----

async fn get_dashboard_cached(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<DashboardData>, ApiError> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(15).min(50);

    let cached = state
        .db
        .get_cache("dashboard")
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Cache read failed: {e}")))?
        .ok_or_else(|| ApiError::CacheUnavailable("Dashboard cache not yet populated".into()))?;
    let cache: DashboardCache = serde_json::from_str(&cached)
        .map_err(|_| ApiError::CacheUnavailable("Dashboard cache data is corrupt".into()))?;
    let data = cache.paginate(page, per_page);
    Ok(Json(data))
}

// ---- Shared helpers ----

async fn build_pr_rows(
    db: &dyn Database,
    prs: &[octocrab::models::pulls::PullRequest],
) -> Vec<PrRow> {
    let mut rows = Vec::new();
    for pr in prs {
        let pr_state = if pr.merged_at.is_some() {
            PrState::Merged
        } else if pr.closed_at.is_some() {
            PrState::Closed
        } else {
            PrState::Open
        };

        let updated = pr
            .updated_at
            .map(format_relative_time)
            .unwrap_or_else(|| "unknown".to_string());

        let plan_result = match db.get_latest_plan_result(pr.number).await {
            Ok(Some(row)) => match row.status {
                PlanStatus::Success => PlanResult::Pass,
                PlanStatus::Failure => PlanResult::Fail,
                PlanStatus::Pending => PlanResult::Pending,
            },
            Ok(None) => PlanResult::None,
            Err(e) => {
                warn!(pr = pr.number, error = %e, "Failed to fetch plan result");
                PlanResult::None
            }
        };

        rows.push(PrRow {
            number: pr.number,
            title: pr.title.clone().unwrap_or_default(),
            author: pr
                .user
                .as_ref()
                .map(|u| u.login.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            state: pr_state,
            plan_result,
            updated,
            url: pr
                .html_url
                .as_ref()
                .map(|u| u.to_string())
                .unwrap_or_default(),
        });
    }
    rows
}

async fn count_job_files(
    octo: &octocrab::Octocrab,
    owner: &str,
    repo: &str,
) -> Result<u32, octocrab::Error> {
    let commits = octo
        .repos(owner, repo)
        .list_commits()
        .per_page(1)
        .send()
        .await?;

    if let Some(latest) = commits.items.first() {
        let sha = &latest.sha;
        let tree_resp: serde_json::Value = octo
            .get(
                format!("/repos/{owner}/{repo}/git/trees/{sha}?recursive=1"),
                Option::<&()>::None,
            )
            .await?;

        if let Some(files) = tree_resp["tree"].as_array() {
            let count = files
                .iter()
                .filter_map(|f| f["path"].as_str())
                .filter(|p| {
                    p.ends_with(".yaml")
                        && !p.contains("yard.yaml")
                        && !p.contains("account.yaml")
                        && !p.contains("region.yaml")
                })
                .count();
            return Ok(count as u32);
        }
    }

    Ok(0)
}

fn format_relative_time(dt: chrono::DateTime<chrono::Utc>) -> String {
    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(dt);

    if duration.num_minutes() < 1 {
        "just now".to_string()
    } else if duration.num_minutes() < 60 {
        format!("{} min ago", duration.num_minutes())
    } else if duration.num_hours() < 24 {
        let h = duration.num_hours();
        format!("{h} hour{} ago", if h == 1 { "" } else { "s" })
    } else {
        let d = duration.num_days();
        format!("{d} day{} ago", if d == 1 { "" } else { "s" })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, test_support::InMemoryDb};
    use crate::types::DashboardCache;
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    fn test_state() -> Arc<ApiState> {
        let db = Arc::new(InMemoryDb::new());
        let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
        Arc::new(ApiState {
            github_token: "t".into(),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            db: db as Arc<dyn Database>,
            event_tx,
        })
    }

    #[tokio::test]
    async fn test_get_dashboard_cached_empty_returns_503() {
        let state = test_state();
        let params = Query(PaginationParams { page: None, per_page: None });
        let result = get_dashboard_cached(State(state), params).await;
        assert!(result.is_err());
        let resp = result.unwrap_err().into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_get_dashboard_cached_with_data_returns_200() {
        let state = test_state();
        let cache = DashboardCache { prs: vec![], open_prs: 0, jobs_tracked: 0 };
        let cached = serde_json::to_string(&cache).unwrap();
        state.db.set_cache("dashboard", &cached).await.unwrap();

        let params = Query(PaginationParams { page: None, per_page: None });
        let result = get_dashboard_cached(State(state), params).await.unwrap();
        assert_eq!(result.0.open_prs, 0);
    }
}
