use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};

use super::dashboard::ApiState;
use crate::types::*;

pub fn jobs_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/jobs", get(get_jobs))
        .route("/api/jobs/file", get(get_job_file))
        .with_state(state)
}

async fn get_jobs(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match fetch_jobs(&state).await {
        Ok(data) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            error!("Failed to fetch jobs: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

async fn fetch_jobs(state: &ApiState) -> Result<JobsData, String> {
    use octocrab::Octocrab;

    let octo = Octocrab::builder()
        .personal_token(state.github_token.clone())
        .build()
        .map_err(|e| format!("Failed to build octocrab: {e}"))?;

    let owner = &state.repo_owner;
    let repo = &state.repo_name;

    let commits = octo
        .repos(owner, repo)
        .list_commits()
        .per_page(1)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch commits: {e}"))?;

    let Some(latest) = commits.items.first() else {
        info!("No commits found in {owner}/{repo}");
        return Ok(JobsData { jobs: vec![] });
    };

    let sha = &latest.sha;
    let tree_resp: serde_json::Value = octo
        .get(
            format!("/repos/{owner}/{repo}/git/trees/{sha}?recursive=1"),
            Option::<&()>::None,
        )
        .await
        .map_err(|e| format!("Failed to fetch repo tree: {e}"))?;

    let jobs: Vec<JobInfo> = tree_resp["tree"]
        .as_array()
        .map(|files| {
            files
                .iter()
                .filter_map(|f| f["path"].as_str())
                .filter(|p| {
                    p.ends_with(".yaml")
                        && !p.contains("yard.yaml")
                        && !p.contains("account.yaml")
                        && !p.contains("region.yaml")
                        && !p.contains("transforms.yaml")
                })
                .map(|p| {
                    let segments: Vec<&str> = p.split('/').collect();
                    let name = segments
                        .last()
                        .unwrap_or(&p)
                        .trim_end_matches(".yaml")
                        .to_string();
                    // Path convention: <provider>/<env>/<region>/job.yaml
                    let environment = segments.get(1).unwrap_or(&"—").to_string();
                    let region = segments.get(2).unwrap_or(&"—").to_string();
                    JobInfo {
                        name,
                        path: p.to_string(),
                        environment,
                        region,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    info!(count = jobs.len(), "Jobs fetched");

    Ok(JobsData { jobs })
}

#[derive(Deserialize)]
struct FileParams {
    path: String,
}

async fn get_job_file(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<FileParams>,
) -> impl IntoResponse {
    match fetch_file_content(&state, &params.path).await {
        Ok(content) => (StatusCode::OK, content).into_response(),
        Err(e) => {
            error!("Failed to fetch file: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

async fn fetch_file_content(state: &ApiState, path: &str) -> Result<String, String> {
    use octocrab::Octocrab;

    let octo = Octocrab::builder()
        .personal_token(state.github_token.clone())
        .build()
        .map_err(|e| format!("Failed to build octocrab: {e}"))?;

    let owner = &state.repo_owner;
    let repo = &state.repo_name;

    let resp: serde_json::Value = octo
        .get(
            format!("/repos/{owner}/{repo}/contents/{path}"),
            Option::<&()>::None,
        )
        .await
        .map_err(|e| format!("Failed to fetch file: {e}"))?;

    let encoded = resp["content"]
        .as_str()
        .ok_or("No content field in response")?
        .replace('\n', "");

    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&encoded)
        .map_err(|e| format!("Failed to decode base64: {e}"))?;

    String::from_utf8(bytes).map_err(|e| format!("Invalid UTF-8: {e}"))
}
