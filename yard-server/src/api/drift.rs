use axum::{
    extract::State,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tracing::{info, warn};

use super::error::ApiError;

use super::dashboard::ApiState;
use crate::db::DriftSnapshot;
use crate::github::git_ops::{WorkdirGuard, clone_at_sha};
use crate::types::*;

// SRV-05 / D-11: byte-stable canonical encoding of DriftType for the
// DriftSnapshot.state_hash column. Returns the EXISTING Debug-derived
// strings (PRES-05 / D-24) — DDB rows are byte-identical pre/post.
impl DriftType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DriftType::Modified => "Modified",
            DriftType::New => "New",
            DriftType::Deleted => "Deleted",
            DriftType::ResourceMissing => "ResourceMissing",
        }
    }
}

/// state_hash sentinel value for in-sync drift snapshot rows.
///
/// Distinct from `DriftType::as_str()` because in-sync rows are an absence of
/// drift, not a drift kind. Promoting "in_sync" to a const (D-13) prevents
/// typo regressions and gives the value a single source of truth.
pub const STATE_HASH_IN_SYNC: &str = "in_sync";

pub fn drift_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/drift", get(get_drift))
        .route("/api/drift/cached", get(get_drift_cached))
        .route("/api/drift/summary", get(get_drift_summary))
        .with_state(state)
}

// ---- Full drift check ----

async fn get_drift(State(state): State<Arc<ApiState>>) -> Result<Json<DriftData>, ApiError> {
    let data = run_drift_check(&state)
        .await
        .map_err(ApiError::Internal)?;
    Ok(Json(data))
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
            _ => DriftType::Modified,
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
            state_hash: item.drift_type.as_str().to_string(),
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
                state_hash: STATE_HASH_IN_SYNC.to_string(),
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

    // Cache the full drift result so the UI can poll without triggering a full check.
    // F-SRV-001: On serialization failure, skip cache write entirely to avoid
    // corrupting DynamoDB with an empty string that poisons subsequent reads.
    let cached = match serde_json::to_string(&drift_data) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "Failed to serialize drift data -- skipping cache update");
            return Ok(drift_data);
        }
    };
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

async fn get_drift_cached(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<DriftData>, ApiError> {
    let cached = state
        .db
        .get_cache("drift")
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Cache read failed: {e}")))?
        .ok_or_else(|| ApiError::CacheUnavailable("Drift cache not yet populated".into()))?;
    let data: DriftData = serde_json::from_str(&cached)
        .map_err(|_| ApiError::CacheUnavailable("Drift cache data is corrupt".into()))?;
    Ok(Json(data))
}

// ---- Lightweight summary from DynamoDB ----

#[derive(Serialize)]
struct DriftSummary {
    drifted: u32,
    in_sync: u32,
}

async fn get_drift_summary(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<DriftSummary>, ApiError> {
    let all = state
        .db
        .list_drift_snapshots(false, 500)
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to fetch drift snapshots: {e}")))?;

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

    Ok(Json(DriftSummary { drifted, in_sync }))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, test_support::InMemoryDb};
    use crate::types::DriftData;
    use axum::extract::State;
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
            shutdown_token: tokio_util::sync::CancellationToken::new(),
        })
    }

    #[tokio::test]
    async fn test_get_drift_cached_empty_returns_503() {
        let state = test_state();
        let result = get_drift_cached(State(state)).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_get_drift_cached_with_data_returns_200() {
        let state = test_state();
        let drift_data = DriftData { items: vec![], in_sync: 5, drifted: 0 };
        let cached = serde_json::to_string(&drift_data).unwrap();
        state.db.set_cache("drift", &cached).await.unwrap();

        let result = get_drift_cached(State(state)).await.unwrap();
        assert_eq!(result.0.in_sync, 5);
        assert_eq!(result.0.drifted, 0);
    }

    #[tokio::test]
    async fn test_get_drift_cached_corrupt_returns_503() {
        let state = test_state();
        state.db.set_cache("drift", "not valid json{{{").await.unwrap();

        let result = get_drift_cached(State(state)).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_get_drift_summary_empty() {
        let state = test_state();
        let result = get_drift_summary(State(state)).await.unwrap();
        assert_eq!(result.0.drifted, 0);
        assert_eq!(result.0.in_sync, 0);
    }

    // SRV-05 / D-16: literal-table determinism for DriftType::as_str.
    // Future variants force a test edit (correctness over silence).
    #[test]
    fn drift_type_as_str_is_stable() {
        use crate::types::DriftType;
        assert_eq!(DriftType::Modified.as_str(), "Modified");
        assert_eq!(DriftType::New.as_str(), "New");
        assert_eq!(DriftType::Deleted.as_str(), "Deleted");
        assert_eq!(DriftType::ResourceMissing.as_str(), "ResourceMissing");
    }

    #[test]
    fn state_hash_in_sync_const_is_stable() {
        assert_eq!(STATE_HASH_IN_SYNC, "in_sync");
    }

    // F-SRV-001: Verify the cache serialization path no longer uses
    // unwrap_or_default which would write an empty string to DynamoDB
    // on serialization failure, corrupting subsequent cache reads.
    #[test]
    fn drift_cache_serialization_no_unwrap_or_default() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let target = format!("{manifest_dir}/src/api/drift.rs");
        let contents = std::fs::read_to_string(&target)
            .expect("failed to read api/drift.rs for serialization guard");

        // Extract production code only (before #[cfg(test)])
        let production_only: String = contents
            .lines()
            .take_while(|line| !line.trim_start().starts_with("#[cfg(test)]"))
            .collect::<Vec<_>>()
            .join("\n");

        // The old pattern was: serde_json::to_string(&drift_data).unwrap_or_default()
        // Verify it is gone from production code.
        assert!(
            !production_only.contains("to_string(&drift_data).unwrap_or_default()"),
            "regression: unwrap_or_default on drift_data serialization reintroduced"
        );

        // Verify the replacement pattern exists: the match-based early return.
        assert!(
            production_only.contains("Failed to serialize drift data -- skipping cache update"),
            "F-SRV-001 fix must include the skip-cache warning message"
        );
    }

    #[test]
    fn drift_cache_serialization_happy_path() {
        // Verify that serde_json::to_string on a realistic DriftData produces
        // a non-empty string (the happy path that the fix preserves).
        let data = DriftData {
            items: vec![],
            in_sync: 5,
            drifted: 0,
        };
        let serialized = serde_json::to_string(&data);
        assert!(serialized.is_ok(), "serialization must succeed for valid data");
        assert!(
            !serialized.unwrap().is_empty(),
            "serialized output must not be empty"
        );
    }

    // SRV-05 / D-15: machine-enforced grep gate to prevent regression of
    // format!("{:?}", ...) writes into DDB state_hash columns.
    //
    // Implementation note: yard-server does NOT depend on `regex` (PRES-03
    // forbids adding crate deps). This gate uses stdlib line-by-line
    // `.contains` filtering only.
    //
    // Filter rule: cordon off everything from `#[cfg(test)]` onward (the
    // test module is, by definition, not production code). Inside the
    // production region, skip pure-comment lines (lines whose first
    // non-whitespace bytes are `//`).
    #[test]
    fn no_debug_format_in_state_hash_path() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let target = format!("{manifest_dir}/src/api/drift.rs");
        let contents = std::fs::read_to_string(&target)
            .expect("failed to read api/drift.rs for grep gate");

        let production_only: String = contents
            .lines()
            .take_while(|line| !line.trim_start().starts_with("#[cfg(test)]"))
            .collect::<Vec<_>>()
            .join("\n");

        let offending: Vec<(usize, &str)> = production_only
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim_start().starts_with("//"))
            .filter(|(_, line)| line.contains("format!(\"{:?}\""))
            .collect();

        assert!(
            offending.is_empty(),
            "regression: format!(\"{{:?}}\", ...) reintroduced in state_hash production path:\n{}",
            offending
                .iter()
                .map(|(n, l)| format!("  line {}: {}", n + 1, l))
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
}
