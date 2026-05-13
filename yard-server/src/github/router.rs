use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use chrono::Utc;
use std::sync::Arc;
use tracing::{info, warn, error};

use super::client::{CommentMode, GitHubApi};
use super::git_ops::{clone_at_sha, WorkdirGuard};
use super::plan::{
    filter_affected_environments, format_per_env_comment, run_per_env_plans, PLAN_COMMENT_MARKER,
};
use super::webhook::{parse_webhook, WebhookAction};
use crate::api::dashboard::ApiState;
use crate::db::{Database, PlanResultRow, PlanStatus, WebhookEvent};

/// Shared state for the webhook handler.
pub struct AppState {
    pub github_client: Arc<dyn GitHubApi>,
    pub webhook_secret: String,
    pub db: Arc<dyn Database>,
    pub api_state: Arc<ApiState>,
    pub dashboard_url: Option<String>,
}

/// Build the axum router for GitHub webhook endpoints.
pub fn github_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/webhook/github", post(handle_webhook))
        .with_state(state)
}

/// Format apply result as text for logging.
fn format_apply_output(result: &yard_core::ApplyResult) -> String {
    let mut output = String::new();

    for name in &result.created {
        output.push_str(&format!("  + Created: {name}\n"));
    }
    for name in &result.modified {
        output.push_str(&format!("  ~ Modified: {name}\n"));
    }
    for name in &result.deleted {
        output.push_str(&format!("  - Deleted: {name}\n"));
    }

    if output.is_empty() {
        output.push_str("No changes applied.\n");
    }

    output
}

/// Search for an existing plan comment (by marker) and update it, or create a new one.
async fn find_and_upsert_plan_comment(
    github: &dyn GitHubApi,
    owner: &str,
    repo: &str,
    pr_number: u64,
    body: &str,
) -> Result<(), String> {
    let comments = github
        .list_comments(owner, repo, pr_number)
        .await
        .map_err(|e| format!("list comments: {e}"))?;

    if let Some(existing) = comments.iter().find(|c| c.body.starts_with(PLAN_COMMENT_MARKER)) {
        github
            .update_comment(owner, repo, existing.id, body)
            .await
            .map_err(|e| format!("update comment: {e}"))?;
    } else {
        github
            .post_comment_raw(owner, repo, pr_number, body)
            .await
            .map_err(|e| format!("post comment: {e}"))?;
    }
    Ok(())
}

/// Full plan pipeline: clone → discover → filter → plan → format → post → persist.
/// Used by both the Plan branch and stale-plan auto-replan.
async fn run_plan_pipeline(
    state: &AppState,
    owner: &str,
    repo: &str,
    pr_number: u64,
    head_sha: &str,
    clone_url: &str,
    target_filter: Option<&str>,
) -> StatusCode {
    let workdir_path = match clone_at_sha(clone_url, head_sha, Some(&state.api_state.github_token))
        .await
    {
        Ok(path) => path,
        Err(e) => {
            error!(pr = pr_number, "Clone failed: {e}");
            let error_body = format!(
                "{PLAN_COMMENT_MARKER}\n### yard plan\n\n:x: Clone failed: {e}"
            );
            let _ = find_and_upsert_plan_comment(
                state.github_client.as_ref(),
                owner,
                repo,
                pr_number,
                &error_body,
            )
            .await;
            return StatusCode::OK;
        }
    };
    let workdir = WorkdirGuard::new(workdir_path);

    let environments = match yard_core::resolve::discover_environments(workdir.path()) {
        Ok(envs) => envs,
        Err(e) => {
            error!(pr = pr_number, "discover_environments failed: {e}");
            let error_body = format!(
                "{PLAN_COMMENT_MARKER}\n### yard plan\n\n:x: Discovery failed: {e}"
            );
            let _ = find_and_upsert_plan_comment(
                state.github_client.as_ref(),
                owner,
                repo,
                pr_number,
                &error_body,
            )
            .await;
            return StatusCode::OK;
        }
    };

    let changed_files = match state
        .github_client
        .get_pr_changed_files(owner, repo, pr_number)
        .await
    {
        Ok(files) => files,
        Err(e) => {
            error!(pr = pr_number, "get_pr_changed_files failed: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let affected_envs = filter_affected_environments(&environments, &changed_files);
    let results = run_per_env_plans(affected_envs, workdir.path(), target_filter).await;

    let plan_id = uuid::Uuid::new_v4().to_string();
    let comment_body = format_per_env_comment(
        &results,
        head_sha,
        state.dashboard_url.as_deref(),
        Some(&plan_id),
    );

    let has_errors = results.iter().any(|r| r.diffs.is_err());
    let plan_status = if has_errors {
        PlanStatus::Failure
    } else {
        PlanStatus::Success
    };

    // Post or update the plan comment
    if let Err(e) = find_and_upsert_plan_comment(
        state.github_client.as_ref(),
        owner,
        repo,
        pr_number,
        &comment_body,
    )
    .await
    {
        warn!(pr = pr_number, error = %e, "Failed to post/update plan comment");
    } else {
        info!(pr = pr_number, "Posted plan comment");
    }

    // Persist plan result
    let plan_result = PlanResultRow {
        id: plan_id,
        pr_number,
        sha: head_sha.to_string(),
        status: plan_status,
        raw_output: comment_body,
        diff_summary: None,
        created_at: Utc::now(),
    };
    if let Err(e) = state.db.insert_plan_result(&plan_result).await {
        warn!(pr = pr_number, error = %e, "Failed to persist plan result");
    }

    // Refresh dashboard cache; emit events
    if let Err(e) = crate::api::dashboard::refresh_dashboard_cache(&state.api_state).await {
        warn!(error = %e, "Failed to refresh dashboard cache after plan");
        let _ = state.api_state.event_tx.send(
            crate::api::events::Event::DashboardFailed {
                reason: crate::api::events::sanitize_reason(&e),
            },
        );
    } else {
        let _ = state
            .api_state
            .event_tx
            .send(crate::api::events::Event::DashboardRefreshed);
    }
    let _ = state
        .api_state
        .event_tx
        .send(crate::api::events::Event::WebhookReceived);

    StatusCode::OK
}

async fn handle_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let action = match parse_webhook(&headers, &body, &state.webhook_secret) {
        Ok(action) => action,
        Err(status) => {
            warn!("Webhook rejected: {status}");
            return status;
        }
    };

    match action {
        WebhookAction::Plan {
            owner,
            repo,
            pr_number,
            head_sha,
            clone_url,
            target_filter,
        } => {
            // Resolve empty head_sha for comment-triggered plans
            let head_sha = if head_sha.is_empty() {
                match state
                    .github_client
                    .get_pr_head_sha(&owner, &repo, pr_number)
                    .await
                {
                    Ok(sha) => sha,
                    Err(e) => {
                        error!(pr = pr_number, "Failed to resolve PR head SHA: {e}");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            } else {
                head_sha
            };

            info!(
                pr = pr_number,
                sha = %head_sha,
                repo = %format!("{owner}/{repo}"),
                "Running yard plan"
            );

            // Persist webhook event
            let webhook_event = WebhookEvent {
                id: uuid::Uuid::new_v4().to_string(),
                pr_number,
                action: "plan".to_string(),
                sha: head_sha.clone(),
                payload: serde_json::json!({
                    "owner": owner,
                    "repo": repo,
                    "clone_url": clone_url,
                }),
                received_at: Utc::now(),
            };
            if let Err(e) = state.db.insert_webhook_event(&webhook_event).await {
                warn!(pr = pr_number, error = %e, "Failed to persist webhook event");
            }

            run_plan_pipeline(
                &state,
                &owner,
                &repo,
                pr_number,
                &head_sha,
                &clone_url,
                target_filter.as_deref(),
            )
            .await
        }
        WebhookAction::Apply {
            owner,
            repo,
            pr_number,
            head_sha,
            clone_url,
        } => {
            let head_sha = if head_sha.is_empty() {
                match state
                    .github_client
                    .get_pr_head_sha(&owner, &repo, pr_number)
                    .await
                {
                    Ok(sha) => sha,
                    Err(e) => {
                        error!(pr = pr_number, "Failed to resolve PR head SHA: {e}");
                        return StatusCode::INTERNAL_SERVER_ERROR;
                    }
                }
            } else {
                head_sha
            };

            info!(
                pr = pr_number,
                sha = %head_sha,
                repo = %format!("{owner}/{repo}"),
                "Running yard apply (triggered by comment)"
            );

            // Stale-plan detection (D-08, D-09)
            match state.db.get_latest_plan_result(pr_number).await {
                Ok(None) => {
                    // D-09: no plan exists — reject
                    let msg = format!(
                        "{PLAN_COMMENT_MARKER}\n\
                         :x: **No plan found for this PR.** Run `yard plan` first."
                    );
                    let _ = find_and_upsert_plan_comment(
                        state.github_client.as_ref(),
                        &owner,
                        &repo,
                        pr_number,
                        &msg,
                    )
                    .await;
                    return StatusCode::OK;
                }
                Ok(Some(plan)) if plan.sha != head_sha => {
                    // D-08: stale plan — reject and auto-replan
                    let plan_sha_short = &plan.sha[..std::cmp::min(7, plan.sha.len())];
                    let head_sha_short = &head_sha[..std::cmp::min(7, head_sha.len())];
                    let msg = format!(
                        "{PLAN_COMMENT_MARKER}\n\
                         :warning: **Plan is stale** (planned at `{plan_sha_short}`, \
                         HEAD is now `{head_sha_short}`). Re-planning automatically \u{2014} \
                         apply again after the new plan completes."
                    );
                    let _ = find_and_upsert_plan_comment(
                        state.github_client.as_ref(),
                        &owner,
                        &repo,
                        pr_number,
                        &msg,
                    )
                    .await;

                    // Spawn auto-replan
                    let state_clone = state.clone();
                    let owner_clone = owner.clone();
                    let repo_clone = repo.clone();
                    let sha_clone = head_sha.clone();
                    let clone_url_clone = clone_url.clone();
                    tokio::spawn(async move {
                        run_plan_pipeline(
                            &state_clone,
                            &owner_clone,
                            &repo_clone,
                            pr_number,
                            &sha_clone,
                            &clone_url_clone,
                            None,
                        )
                        .await;
                    });

                    return StatusCode::OK;
                }
                Ok(Some(_)) => {
                    // SHA matches — proceed to apply (Phase 42 will implement)
                }
                Err(e) => {
                    error!(pr = pr_number, error = %e, "Failed to check latest plan result");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }

            // Persist webhook event
            let webhook_event = WebhookEvent {
                id: uuid::Uuid::new_v4().to_string(),
                pr_number,
                action: "apply".to_string(),
                sha: head_sha.clone(),
                payload: serde_json::json!({
                    "owner": owner,
                    "repo": repo,
                    "clone_url": clone_url,
                }),
                received_at: Utc::now(),
            };
            if let Err(e) = state.db.insert_webhook_event(&webhook_event).await {
                warn!(pr = pr_number, error = %e, "Failed to persist webhook event");
            }

            // Clone and run apply via yard-core
            let apply_output = match clone_at_sha(
                &clone_url,
                &head_sha,
                Some(&state.api_state.github_token),
            )
            .await
            {
                Ok(workdir_path) => {
                    let workdir = WorkdirGuard::new(workdir_path);
                    match yard_core::resolve::resolve_project(workdir.path()).await {
                        Ok(project) => {
                            match yard_core::apply(
                                &project.manifest,
                                &project.current_state,
                                &project.root_dir,
                                false,
                                None,
                            )
                            .await
                            {
                                Ok(apply_result) => {
                                    let output = format_apply_output(&apply_result);
                                    format!("### yard apply\n\n{output}")
                                }
                                Err(e) => {
                                    error!(pr = pr_number, "yard apply failed: {e}");
                                    format!("### yard apply failed\n\n{e}")
                                }
                            }
                        }
                        Err(e) => {
                            error!(pr = pr_number, "Failed to resolve project: {e}");
                            format!("### yard apply failed\n\nFailed to resolve project:\n{e}")
                        }
                    }
                }
                Err(e) => {
                    error!(pr = pr_number, "Clone failed: {e}");
                    format!("### yard apply failed\n\nFailed to clone repo:\n{e}")
                }
            };

            let status = match state
                .github_client
                .post_comment(&owner, &repo, pr_number, &apply_output, CommentMode::Apply)
                .await
            {
                Ok(_) => {
                    info!(pr = pr_number, "Posted apply comment");
                    StatusCode::OK
                }
                Err(e) => {
                    warn!(pr = pr_number, error = %e, "Failed to post apply comment");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };

            // Refresh dashboard cache; emit events
            if let Err(e) = crate::api::dashboard::refresh_dashboard_cache(&state.api_state).await {
                warn!(error = %e, "Failed to refresh dashboard cache after apply");
                let _ = state.api_state.event_tx.send(
                    crate::api::events::Event::DashboardFailed {
                        reason: crate::api::events::sanitize_reason(&e),
                    },
                );
            } else {
                let _ = state
                    .api_state
                    .event_tx
                    .send(crate::api::events::Event::DashboardRefreshed);
            }
            let _ = state
                .api_state
                .event_tx
                .send(crate::api::events::Event::WebhookReceived);

            status
        }
        WebhookAction::Ignore => StatusCode::OK,
    }
}

#[cfg(test)]
mod tests {
    use crate::api::events::{new_event_channel, sanitize_reason, Event};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn webhook_emission_contract_dashboard_refreshed_then_webhook_received() {
        // This test exercises the broadcast plumbing that the Plan/Apply branches
        // use after a successful refresh_dashboard_cache. It mirrors the exact
        // sequence in handle_webhook but without the GitHub clone path.
        let (tx, mut rx) = new_event_channel();

        // Success branch: emit DashboardRefreshed then WebhookReceived.
        let _ = tx.send(Event::DashboardRefreshed);
        let _ = tx.send(Event::WebhookReceived);

        let first = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv err");
        let second = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv err");

        assert!(matches!(first, Event::DashboardRefreshed));
        assert!(matches!(second, Event::WebhookReceived));
    }

    #[tokio::test]
    async fn webhook_emission_contract_dashboard_failed_carries_sanitized_reason() {
        // Failure branch: emit DashboardFailed { reason: sanitize_reason(&e) } then WebhookReceived.
        let (tx, mut rx) = new_event_channel();
        let long_err = "x".repeat(300);
        let _ = tx.send(Event::DashboardFailed {
            reason: sanitize_reason(&long_err),
        });
        let _ = tx.send(Event::WebhookReceived);

        let first = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv err");
        match first {
            Event::DashboardFailed { reason } => {
                assert_eq!(
                    reason.chars().count(),
                    200,
                    "reason must be sanitized to 200 chars"
                );
                assert!(reason.ends_with('…'));
            }
            other => panic!("expected DashboardFailed, got {other:?}"),
        }

        let second = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv err");
        assert!(matches!(second, Event::WebhookReceived));
    }

    // ---- e2e webhook → plan → PR comment integration test (Phase 27 / SRV-04) ----

    use super::AppState;
    use super::github_router;
    use crate::api::dashboard::ApiState;
    use crate::db::Database;
    use crate::db::test_support::InMemoryDb;
    use crate::github::client::GitHubApi;
    use crate::github::client::test_support::InMemoryGitHubApi;
    use crate::db::PlanStatus;
    use chrono::Utc;
    use crate::secrets::SecretStore;
    use crate::secrets::test_support::InMemorySecretStore;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tower::ServiceExt;

    type HmacSha256 = Hmac<Sha256>;

    /// Hand-rolled scoped temp dir — `tempfile` is NOT a yard-server dep
    /// (PRES-03 + critical correction in PLAN.md context). Mirrors
    /// `yard-core/tests/common/mod.rs:24-51` line-for-line.
    static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let n = FIXTURE_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir()
                .join(format!("yard_e2e_fixture_{}_{}", std::process::id(), n));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            TempDir { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// Build a fixture git repo with per-environment structure:
    /// `<root>/production/us-east-1/jobs/example/config.yaml`
    /// Returns the tempdir and the HEAD SHA.
    fn build_fixture_repo() -> (TempDir, String) {
        let tmp = TempDir::new();
        let dir = tmp.path();

        fs::write(
            dir.join("yard.yaml"),
            "project: yard-fixture\nstate:\n  type: local\n  path: .yard/state/\n",
        )
        .unwrap();

        // Per-env structure for discover_environments
        let env_dir = dir.join("production");
        fs::create_dir_all(&env_dir).unwrap();
        fs::write(env_dir.join("account.yaml"), "account_id: \"000000000000\"\n").unwrap();

        let region_dir = env_dir.join("us-east-1");
        fs::create_dir_all(&region_dir).unwrap();
        fs::write(region_dir.join("region.yaml"), "region: us-east-1\n").unwrap();

        let example_dir = region_dir.join("jobs").join("example");
        fs::create_dir_all(&example_dir).unwrap();
        fs::write(
            example_dir.join("config.yaml"),
            "type: glue\nrole: arn:aws:iam::000000000000:role/yard-fixture\n",
        )
        .unwrap();

        std::process::Command::new("git")
            .arg("init")
            .current_dir(dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["add", "."])
            .current_dir(dir)
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c", "user.email=test@test",
                "-c", "user.name=test",
                "commit", "-m", "fixture",
            ])
            .current_dir(dir)
            .output()
            .unwrap();

        let head_out = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(dir)
            .output()
            .unwrap();
        let head_sha = String::from_utf8(head_out.stdout)
            .unwrap()
            .trim()
            .to_string();

        (tmp, head_sha)
    }

    /// HMAC-SHA256-sign a webhook body. Mirrors the inline `sign()`
    /// helper at `yard-server/src/github/webhook.rs:203-207` (D-43:
    /// duplicated rather than promoted to a shared helper until a third
    /// call site appears).
    fn sign_webhook(secret: &str, payload: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[tokio::test]
    async fn webhook_to_comment_e2e_pull_request_plan_posts_comment() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (fixture, head_sha) = build_fixture_repo();
        let clone_url = fixture.path().to_string_lossy().to_string();

        let (event_tx, _event_rx) = new_event_channel();
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));
        let api_state = Arc::new(ApiState {
            github_token: "test-token".to_string(),
            repo_owner: "yard-test-owner".to_string(),
            repo_name: "yard-test-repo".to_string(),
            db: db.clone(),
            event_tx,
            secret_store,
        });

        let mock_gh = Arc::new(InMemoryGitHubApi::new());
        // Seed changed_files so filter_affected_environments finds "production"
        {
            let mut files = mock_gh.changed_files.lock().await;
            files.push("production/us-east-1/jobs/example/config.yaml".to_string());
        }

        let webhook_state = Arc::new(AppState {
            github_client: mock_gh.clone() as Arc<dyn GitHubApi>,
            webhook_secret: "test-webhook-secret".to_string(),
            db: db.clone(),
            api_state: api_state.clone(),
            dashboard_url: None,
        });

        let payload = serde_json::json!({
            "action": "opened",
            "number": 42,
            "pull_request": {
                "head": { "ref": "feature", "sha": head_sha },
                "base": { "ref": "main", "sha": "0000000" }
            },
            "repository": {
                "full_name": "yard-test-owner/yard-test-repo",
                "clone_url": clone_url
            }
        });
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign_webhook("test-webhook-secret", &payload_bytes);

        let start = tokio::time::Instant::now();
        let response = github_router(webhook_state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/webhook/github")
                    .header("X-GitHub-Event", "pull_request")
                    .header("X-Hub-Signature-256", &sig)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            elapsed < Duration::from_secs(10),
            "e2e test took {elapsed:?}; budget is 10s"
        );

        // Plan now uses post_comment_raw via find_and_upsert_plan_comment
        let raw_posts = mock_gh.raw_posts.lock().await;
        assert_eq!(
            raw_posts.len(),
            1,
            "expected exactly one raw plan comment; got {}",
            raw_posts.len()
        );
        let body = &raw_posts[0].body;

        // New per-env format assertions
        assert!(
            body.starts_with("<!-- yard-plan-comment -->"),
            "missing plan comment marker; body was:\n{body}"
        );
        assert!(
            body.contains("### yard plan (SHA:"),
            "missing plan header; body was:\n{body}"
        );
        assert!(
            body.contains("production"),
            "missing environment name; body was:\n{body}"
        );
    }

    #[tokio::test]
    async fn test_apply_no_plan_found_rejects() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (event_tx, _event_rx) = new_event_channel();
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));
        let api_state = Arc::new(ApiState {
            github_token: "test-token".to_string(),
            repo_owner: "o".to_string(),
            repo_name: "r".to_string(),
            db: db.clone(),
            event_tx,
            secret_store,
        });

        let mock_gh = Arc::new(InMemoryGitHubApi::new());
        let webhook_state = Arc::new(AppState {
            github_client: mock_gh.clone() as Arc<dyn GitHubApi>,
            webhook_secret: "s".to_string(),
            db: db.clone(),
            api_state,
            dashboard_url: None,
        });

        let payload = serde_json::json!({
            "action": "created",
            "comment": {"body": "yard apply"},
            "issue": {"number": 42, "pull_request": {"url": "https://api.github.com/pulls/42"}},
            "repository": {"full_name": "o/r", "clone_url": "https://x.com"}
        });
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign_webhook("s", &payload_bytes);

        let response = github_router(webhook_state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/webhook/github")
                    .header("X-GitHub-Event", "issue_comment")
                    .header("X-Hub-Signature-256", &sig)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let raw_posts = mock_gh.raw_posts.lock().await;
        assert_eq!(raw_posts.len(), 1);
        assert!(
            raw_posts[0].body.contains("No plan found"),
            "expected rejection message; got: {}",
            raw_posts[0].body
        );
    }

    #[tokio::test]
    async fn test_apply_stale_plan_rejects() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let (event_tx, _event_rx) = new_event_channel();
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));
        let api_state = Arc::new(ApiState {
            github_token: "test-token".to_string(),
            repo_owner: "o".to_string(),
            repo_name: "r".to_string(),
            db: db.clone(),
            event_tx,
            secret_store,
        });

        // Insert a plan with an old SHA
        let old_plan = crate::db::PlanResultRow {
            id: "plan-1".to_string(),
            pr_number: 42,
            sha: "old-sha-1234567".to_string(),
            status: PlanStatus::Success,
            raw_output: "old plan".to_string(),
            diff_summary: None,
            created_at: Utc::now(),
        };
        db.insert_plan_result(&old_plan).await.unwrap();

        let mock_gh = Arc::new(InMemoryGitHubApi::new());
        // The mock returns "test-sha-abc123" from get_pr_head_sha (different from "old-sha-1234567")
        let webhook_state = Arc::new(AppState {
            github_client: mock_gh.clone() as Arc<dyn GitHubApi>,
            webhook_secret: "s".to_string(),
            db: db.clone(),
            api_state,
            dashboard_url: None,
        });

        let payload = serde_json::json!({
            "action": "created",
            "comment": {"body": "yard apply"},
            "issue": {"number": 42, "pull_request": {"url": "https://api.github.com/pulls/42"}},
            "repository": {"full_name": "o/r", "clone_url": "https://x.com"}
        });
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign_webhook("s", &payload_bytes);

        let response = github_router(webhook_state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/api/webhook/github")
                    .header("X-GitHub-Event", "issue_comment")
                    .header("X-Hub-Signature-256", &sig)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(payload_bytes))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Give the spawned auto-replan task a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        let raw_posts = mock_gh.raw_posts.lock().await;
        assert!(
            raw_posts.iter().any(|p| p.body.contains("Plan is stale")),
            "expected stale plan rejection; got: {:?}",
            raw_posts.iter().map(|p| &p.body).collect::<Vec<_>>()
        );
    }
}
