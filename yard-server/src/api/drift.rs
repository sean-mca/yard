use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::dashboard::ApiState;
use crate::db::DriftSnapshot;
use crate::github::git_ops::{WorkdirGuard, clone_at_sha};
use crate::types::*;

pub fn drift_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/drift", get(get_drift))
        .route("/api/drift/cached", get(get_drift_cached))
        .route("/api/drift/summary", get(get_drift_summary))
        .with_state(state)
}

// ---- Full drift check ----

async fn get_drift(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match run_drift_check(&state).await {
        Ok(data) => (StatusCode::OK, Json(data)).into_response(),
        Err(e) => {
            error!("Drift check failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

pub async fn run_drift_check(state: &ApiState) -> Result<DriftData, String> {
    // 1. Get latest commit SHA
    let sha = get_head_sha(state).await?;

    // 2. Clone repo — token is passed separately so it never appears in URLs
    let clone_url = format!(
        "https://github.com/{}/{}.git",
        state.repo_owner, state.repo_name
    );

    let workdir = WorkdirGuard::new(
        clone_at_sha(&clone_url, &sha, Some(&state.github_token)).await?,
    );

    // 3. Resolve project, calculate diff, and verify resources
    let project = yard_core::resolve::resolve_project(workdir.path())
        .await
        .map_err(|e| format!("Failed to resolve project: {e}"))?;
    let diffs = yard_core::calculate_diff(&project.manifest, &project.current_state)
        .map_err(|e| format!("Failed to calculate diff: {e}"))?;

    // Verify that deployed resources still exist in AWS
    let resource_statuses = yard_core::verify_deployed_resources(
        &project.manifest,
        &project.current_state,
    )
    .await
    .map_err(|e| format!("Failed to verify resources: {e}"))?;

    let job_files = discover_job_files_with_content(workdir.path());
    drop(workdir);

    // 4. Build DriftItems from hash-based diffs
    let mut items = Vec::new();
    for diff in &diffs {
        let (environment, region, new_config) = match job_files.get(&diff.name) {
            Some(info) => (
                info.environment.clone(),
                info.region.clone(),
                Some(info.content.clone()),
            ),
            None => ("unknown".to_string(), "unknown".to_string(), None),
        };

        let drift_type = match &diff.diff_type {
            yard_structs::DiffType::Create => DriftType::New,
            yard_structs::DiffType::Modify { .. } => DriftType::Modified,
            yard_structs::DiffType::Delete => DriftType::Deleted,
        };

        let fields_changed = match &diff.diff_type {
            yard_structs::DiffType::Modify { changes } => {
                changes.keys().cloned().collect()
            }
            _ => vec![],
        };

        items.push(DriftItem {
            name: diff.name.clone(),
            environment,
            region,
            drift_type,
            fields_changed,
            old_config: None,
            new_config,
        });
    }

    // 4b. Add drift items for jobs whose resources are missing in AWS
    //     (these may be "in sync" by hash but have out-of-band deletions)
    let drift_job_names: std::collections::HashSet<&str> =
        diffs.iter().map(|d| d.name.as_str()).collect();

    for (job_name, statuses) in &resource_statuses {
        let missing: Vec<&str> = statuses
            .iter()
            .filter(|s| !s.exists)
            .map(|s| s.resource.r#type.as_str())
            .collect();

        if !missing.is_empty() && !drift_job_names.contains(job_name.as_str()) {
            let (environment, region) = match job_files.get(job_name) {
                Some(info) => (info.environment.clone(), info.region.clone()),
                None => ("unknown".to_string(), "unknown".to_string()),
            };

            items.push(DriftItem {
                name: job_name.clone(),
                environment,
                region,
                drift_type: DriftType::ResourceMissing,
                fields_changed: missing.iter().map(|s| s.to_string()).collect(),
                old_config: None,
                new_config: None,
            });
        }
    }

    let drifted = items.len() as u32;
    let total_jobs = job_files.len() as u32;
    let in_sync = total_jobs.saturating_sub(drifted);

    // 5. Store drift snapshots in DynamoDB
    for item in &items {
        let snapshot = DriftSnapshot {
            id: uuid::Uuid::new_v4().to_string(),
            job_name: item.name.clone(),
            repo_hash: sha.clone(),
            state_hash: format!("{:?}", item.drift_type),
            drifted: true,
            checked_at: Utc::now(),
        };
        if let Err(e) = state.db.insert_drift_snapshot(&snapshot).await {
            warn!(job = %item.name, error = %e, "Failed to persist drift snapshot");
        }
    }

    // Store in-sync jobs
    for name in job_files.keys() {
        if !diffs.iter().any(|d| &d.name == name) {
            let snapshot = DriftSnapshot {
                id: uuid::Uuid::new_v4().to_string(),
                job_name: name.clone(),
                repo_hash: sha.clone(),
                state_hash: "in_sync".to_string(),
                drifted: false,
                checked_at: Utc::now(),
            };
            if let Err(e) = state.db.insert_drift_snapshot(&snapshot).await {
                warn!(job = %name, error = %e, "Failed to persist drift snapshot");
            }
        }
    }

    info!(drifted = drifted, in_sync = in_sync, "Drift check complete");

    let drift_data = DriftData {
        items,
        in_sync,
        drifted,
    };

    // Cache the full drift result so the UI can poll without triggering a full check
    let cached = serde_json::to_string(&drift_data).unwrap_or_default();
    if let Err(e) = state.db.set_cache("drift", &cached).await {
        warn!(error = %e, "Failed to cache drift data");
    }

    Ok(drift_data)
}

async fn get_head_sha(state: &ApiState) -> Result<String, String> {
    let octo = octocrab::Octocrab::builder()
        .personal_token(state.github_token.clone())
        .build()
        .map_err(|e| format!("Failed to build octocrab: {e}"))?;

    let commits = octo
        .repos(&state.repo_owner, &state.repo_name)
        .list_commits()
        .per_page(1)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch commits: {e}"))?;

    commits
        .items
        .first()
        .map(|c| c.sha.clone())
        .ok_or_else(|| "No commits found".to_string())
}

// ---- Cached drift data (populated by background task or full check) ----

async fn get_drift_cached(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match state.db.get_cache("drift").await {
        Ok(Some(cached)) => match serde_json::from_str::<DriftData>(&cached) {
            Ok(data) => (StatusCode::OK, Json(data)).into_response(),
            Err(_) => (
                StatusCode::OK,
                Json(DriftData {
                    items: vec![],
                    in_sync: 0,
                    drifted: 0,
                }),
            )
                .into_response(),
        },
        _ => (
            StatusCode::OK,
            Json(DriftData {
                items: vec![],
                in_sync: 0,
                drifted: 0,
            }),
        )
            .into_response(),
    }
}

// ---- Lightweight summary from DynamoDB ----

#[derive(Serialize)]
struct DriftSummary {
    drifted: u32,
    in_sync: u32,
}

async fn get_drift_summary(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let all = match state.db.list_drift_snapshots(false, 500).await {
        Ok(items) => items,
        Err(e) => {
            error!("Failed to fetch drift snapshots: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DriftSummary {
                    drifted: 0,
                    in_sync: 0,
                }),
            )
                .into_response();
        }
    };

    // Deduplicate: keep latest per job (list is sorted by time desc from GSI)
    let mut seen = std::collections::HashSet::new();
    let mut drifted = 0u32;
    let mut in_sync = 0u32;
    for snapshot in &all {
        if seen.insert(snapshot.job_name.clone()) {
            if snapshot.drifted {
                drifted += 1;
            } else {
                in_sync += 1;
            }
        }
    }

    (StatusCode::OK, Json(DriftSummary { drifted, in_sync })).into_response()
}

// ---- Job file discovery from cloned repo ----

struct JobFileInfo {
    environment: String,
    region: String,
    content: String,
}

fn discover_job_files_with_content(workdir: &Path) -> HashMap<String, JobFileInfo> {
    let mut jobs = HashMap::new();
    walk_for_jobs(workdir, workdir, &mut jobs);
    jobs
}

fn walk_for_jobs(dir: &Path, workdir: &Path, jobs: &mut HashMap<String, JobFileInfo>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && !name.starts_with('.')
            {
                walk_for_jobs(&path, workdir, jobs);
            }
        } else if let Some(ext) = path.extension()
            && ext == "yaml"
            && let Some(file_name) = path.file_name().and_then(|n| n.to_str())
        {
            if matches!(
                file_name,
                "yard.yaml" | "account.yaml" | "region.yaml" | "transforms.yaml"
            ) {
                continue;
            }

            let job_name = file_name.trim_end_matches(".yaml").to_string();
            let (environment, region) = extract_env_region(&path, workdir);
            let content = std::fs::read_to_string(&path).unwrap_or_default();

            jobs.insert(
                job_name,
                JobFileInfo {
                    environment,
                    region,
                    content,
                },
            );
        }
    }
}

fn extract_env_region(job_path: &Path, workdir: &Path) -> (String, String) {
    let relative = job_path.strip_prefix(workdir).unwrap_or(job_path);

    let segments: Vec<&str> = relative.iter().filter_map(|s| s.to_str()).collect();

    // Path convention: <provider>/<env>/<region>/job.yaml
    let offset = if segments.first() == Some(&"jobs") {
        1
    } else {
        0
    };

    let environment = segments
        .get(1 + offset)
        .unwrap_or(&"unknown")
        .to_string();
    let region = segments
        .get(2 + offset)
        .unwrap_or(&"unknown")
        .to_string();

    (environment, region)
}
