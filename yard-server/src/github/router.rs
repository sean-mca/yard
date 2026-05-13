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
use super::webhook::{parse_webhook, WebhookAction};
use crate::api::dashboard::ApiState;
use crate::db::{Database, PlanResultRow, PlanStatus, WebhookEvent};

/// Shared state for the webhook handler.
pub struct AppState {
    pub github_client: Arc<dyn GitHubApi>,
    pub webhook_secret: String,
    pub db: Arc<dyn Database>,
    pub api_state: Arc<ApiState>,
}

/// Build the axum router for GitHub webhook endpoints.
pub fn github_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/webhook/github", post(handle_webhook))
        .with_state(state)
}

/// Format diffs as text for a PR comment.
/// GitHub comment body limit is 65,536 characters.
const GITHUB_COMMENT_MAX_LEN: usize = 65_536;
const TRUNCATION_NOTICE: &str =
    "\n\n---\n**Output truncated.** Full plan had more changes than can fit in a GitHub comment.\n";
/// Byte overhead of the wrapping template (header + details + fences + footer)
/// in `client.rs::post_comment`. Must be subtracted from the truncation budget
/// so the final assembled comment fits within GITHUB_COMMENT_MAX_LEN.
/// Actual template is ~140-160 bytes depending on mode; 200 is generous headroom.
const COMMENT_TEMPLATE_OVERHEAD: usize = 200;

fn format_plan_output(diffs: &[yard_structs::JobDiff], project_name: &str) -> String {
    let mut output = format!("### yard plan for {project_name}\n\n");

    if diffs.is_empty() {
        output.push_str("No changes. Infrastructure is up to date.\n");
        return output;
    }

    let summary = format!("{} job(s) changed.\n\n", diffs.len());
    output.push_str(&summary);

    let max_body = GITHUB_COMMENT_MAX_LEN
        .saturating_sub(TRUNCATION_NOTICE.len())
        .saturating_sub(COMMENT_TEMPLATE_OVERHEAD);

    for diff in diffs {
        let entry = match &diff.diff_type {
            yard_structs::DiffType::Create => {
                format!("  + Create job [{}]\n", diff.name)
            }
            yard_structs::DiffType::Modify { changes } => {
                let mut s = format!("  ~ Modify job [{}]\n", diff.name);
                for (key, (old, new)) in changes {
                    s.push_str(&format!("      {key} : {old} -> {new}\n"));
                }
                s
            }
            yard_structs::DiffType::Delete => {
                format!("  - Delete job [{}]\n", diff.name)
            }
        };

        if output.len() + entry.len() > max_body {
            output.push_str(TRUNCATION_NOTICE);
            return output;
        }

        output.push_str(&entry);
    }

    output
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
            target_filter: _target_filter,
        } => {
            info!(
                pr = pr_number,
                sha = %head_sha,
                repo = %format!("{owner}/{repo}"),
                "Running yard plan"
            );

            // Persist the webhook event
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

            // Clone and run plan via yard-core — token passed via env, not in URL
            let plan_result: Result<String, String> = match clone_at_sha(
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
                            match yard_core::calculate_diff(
                                &project.manifest,
                                &project.current_state,
                            ) {
                                Ok(diffs) => Ok(format_plan_output(&diffs, &project.manifest.project)),
                                Err(e) => {
                                    error!(pr = pr_number, "yard plan failed: {e}");
                                    Err(format!("yard plan failed:\n{e}"))
                                }
                            }
                        }
                        Err(e) => {
                            error!(pr = pr_number, "yard plan failed: {e}");
                            Err(format!("yard plan failed:\n{e}"))
                        }
                    }
                }
                Err(e) => {
                    error!(pr = pr_number, "Clone failed: {e}");
                    Err(format!("Failed to clone repo:\n{e}"))
                }
            };

            let status = if plan_result.is_ok() {
                PlanStatus::Success
            } else {
                PlanStatus::Failure
            };
            let plan_output = match plan_result {
                Ok(output) | Err(output) => output,
            };

            // Persist the plan result
            let plan_result = PlanResultRow {
                id: uuid::Uuid::new_v4().to_string(),
                pr_number,
                sha: head_sha.clone(),
                status,
                raw_output: plan_output.clone(),
                diff_summary: None,
                created_at: Utc::now(),
            };
            if let Err(e) = state.db.insert_plan_result(&plan_result).await {
                warn!(pr = pr_number, error = %e, "Failed to persist plan result");
            }

            let status = match state
                .github_client
                .post_comment(&owner, &repo, pr_number, &plan_output, CommentMode::Plan)
                .await
            {
                Ok(_) => {
                    info!(pr = pr_number, "Posted plan comment");
                    StatusCode::OK
                }
                Err(e) => {
                    warn!(pr = pr_number, error = %e, "Failed to post plan comment");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };

            // Refresh dashboard cache; emit events for the outcome.
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

            status
        }
        WebhookAction::Apply {
            owner,
            repo,
            pr_number,
            head_sha,
            clone_url,
        } => {
            // Resolve head SHA if not provided (issue_comment events don't include it).
            // WR-04: use GitHubApi trait method instead of free function so the
            // Apply path goes through the same auth/mock surface as Plan.
            let head_sha = if head_sha.is_empty() {
                match state.github_client.get_pr_head_sha(
                    &owner, &repo, pr_number,
                ).await {
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

            // Persist the webhook event
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

            // Clone and run apply via yard-core — token passed via env, not in URL
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

            // Post apply result as PR comment
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

            // Refresh dashboard cache; emit events for the outcome.
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
    use crate::github::client::{CommentMode, GitHubApi};
    use crate::github::client::test_support::InMemoryGitHubApi;
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

    /// Build a fixture git repo on disk: a single Glue job at
    /// `<root>/jobs/example/config.yaml` in a project named `yard-fixture`
    /// with `state.type: local` (empty state dir) — yields a Create-only
    /// diff per D-07/D-08/D-09 with discoverable job_name `example-config`
    /// (post-verification corrected per resolve.rs:237-256). Returns the
    /// tempdir (caller holds it for lifetime) and the HEAD SHA from
    /// `git rev-parse HEAD` (D-02 — runtime-computed, not hardcoded).
    fn build_fixture_repo() -> (TempDir, String) {
        let tmp = TempDir::new();
        let dir = tmp.path();

        // Minimal yard.yaml — local state, no providers needed for the
        // PLAN code path.
        fs::write(
            dir.join("yard.yaml"),
            "project: yard-fixture\nstate:\n  type: local\n  path: .yard/state/\n",
        )
        .unwrap();

        // account.yaml + region.yaml are REQUIRED by yard-core's
        // `find_and_parse_context` (resolve.rs:445-447). Both files are
        // searched up from the job directory and must exist somewhere up
        // the tree. Place at the project root with minimal stub content
        // so the cascade has something to flatten without provider-specific
        // requirements.
        fs::write(dir.join("account.yaml"), "account_id: \"000000000000\"\n").unwrap();
        fs::write(dir.join("region.yaml"), "region: us-east-1\n").unwrap();

        // Single Glue job at <root>/jobs/example/config.yaml so
        // resolve_project's job_name = folder + base_name = "example-config"
        // (per yard-core/src/resolve.rs:237-256 — folder is unconditionally
        // appended when present, so the fixture must accept the joined
        // form; D-21 substring set is adjusted to "+ Create job [example-config]"
        // per the inline note in the plan's <action> block).
        let example_dir = dir.join("jobs").join("example");
        fs::create_dir_all(&example_dir).unwrap();
        fs::write(
            example_dir.join("config.yaml"),
            "type: glue\nrole: arn:aws:iam::000000000000:role/yard-fixture\n",
        )
        .unwrap();

        // Initialize git repo + commit fixture. Inline -c flags so the
        // test is independent of the runner's ~/.gitconfig (D-03).
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
                "-c",
                "user.email=test@test",
                "-c",
                "user.name=test",
                "commit",
                "-m",
                "fixture",
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
        // Install rustls crypto provider before any TLS clients are created.
        // Mirrors main.rs:83 — required because refresh_dashboard_cache
        // builds an octocrab client (which uses reqwest → rustls). Without
        // this, the dashboard-refresh code path panics on first TLS use
        // instead of fail-and-warning per D-24. Best-effort install (idempotent
        // across concurrent tests sharing a process — `let _ =` swallows
        // the AlreadyInstalled error).
        let _ = rustls::crypto::ring::default_provider().install_default();

        // ---- Build the fixture git repo + read its HEAD SHA (D-01..D-06).
        let (fixture, head_sha) = build_fixture_repo();
        let clone_url = fixture.path().to_string_lossy().to_string();

        // ---- Construct ApiState by hand (D-32). Mirrors the precedent
        // at api/events.rs:250-272 (events_router_compiles_with_api_state).
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

        // ---- Construct AppState (D-34). Hold mock_gh separately so the
        // test can read posted comments after the request completes (D-16).
        let mock_gh = Arc::new(InMemoryGitHubApi::new());
        let webhook_state = Arc::new(AppState {
            github_client: mock_gh.clone() as Arc<dyn GitHubApi>,
            webhook_secret: "test-webhook-secret".to_string(),
            db: db.clone(),
            api_state: api_state.clone(),
        });

        // ---- Construct synthetic pull_request.opened payload (D-36 +
        // critical correction: Repository uses full_name, not owner.login
        // + name, per webhook.rs:80-84).
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

        // ---- Send the request through github_router (D-17, D-19) +
        // measure wall-clock for the SC #4 timing assertion (D-29..D-31).
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

        // ---- Assert handler returned 200 (D-25: status comes from
        // post_comment outcome, NOT from refresh_dashboard_cache —
        // D-24 allows the dashboard refresh to fail-and-warn).
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "handle_webhook should return 200 OK for a successful Plan path"
        );

        // ---- Assert SC #4 timing budget (D-29..D-31). 10s ceiling
        // matches Phase 27 SC #4 exactly.
        assert!(
            elapsed < Duration::from_secs(10),
            "e2e test took {elapsed:?}; SC #4 budget is 10s"
        );

        // ---- Assert the mock GitHubApi captured exactly one comment.
        let posts = mock_gh.posts.lock().await;
        assert_eq!(
            posts.len(),
            1,
            "expected exactly one post_comment call; got {}",
            posts.len()
        );
        let body = &posts[0].body;

        // ---- Assert the four D-21 substrings (post-verification
        // correction: the discoverable job_name is "example-config",
        // not pure "example", per resolve.rs:237-256 — see the inline
        // comment in build_fixture_repo).
        assert!(
            body.contains("### yard plan for "),
            "missing formatter header substring; body was:\n{body}"
        );
        assert!(
            body.contains("yard-fixture"),
            "missing project-name substring (exercises manifest resolution); body was:\n{body}"
        );
        assert!(
            body.contains("1 job(s) changed."),
            "missing diff-count substring; body was:\n{body}"
        );
        assert!(
            body.contains("+ Create job [example-config]"),
            "missing Create-branch substring; body was:\n{body}"
        );

        // ---- Assert the comment was posted with Plan mode (WR-01 regression).
        assert!(
            matches!(posts[0].mode, CommentMode::Plan),
            "expected CommentMode::Plan for Plan-path comment"
        );
    }
}
