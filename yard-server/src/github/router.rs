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

use super::client::GitHubClient;
use super::git_ops::{clone_at_sha, WorkdirGuard};
use super::webhook::{parse_webhook, WebhookAction};
use crate::api::dashboard::ApiState;
use crate::db::{DynamoDatabase, PlanResultRow, PlanStatus, WebhookEvent};
use crate::yard_runner;

/// Shared state for the webhook handler.
pub struct AppState {
    pub github_client: GitHubClient,
    pub webhook_secret: String,
    pub db: Arc<DynamoDatabase>,
    pub api_state: Arc<ApiState>,
}

/// Build the axum router for GitHub webhook endpoints.
pub fn github_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/webhook/github", post(handle_webhook))
        .with_state(state)
}

/// Format diffs as text for a PR comment.
fn format_plan_output(diffs: &[yard_structs::JobDiff], project_name: &str) -> String {
    let mut output = format!("### yard plan for {project_name}\n\n");

    if diffs.is_empty() {
        output.push_str("No changes. Infrastructure is up to date.\n");
        return output;
    }

    for diff in diffs {
        match &diff.diff_type {
            yard_structs::DiffType::Create => {
                output.push_str(&format!("  + Create job [{}]\n", diff.name));
            }
            yard_structs::DiffType::Modify { changes } => {
                output.push_str(&format!("  ~ Modify job [{}]\n", diff.name));
                for (key, (old, new)) in changes {
                    output.push_str(&format!("      {key} : {old} -> {new}\n"));
                }
            }
            yard_structs::DiffType::Delete => {
                output.push_str(&format!("  - Delete job [{}]\n", diff.name));
            }
        }
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

async fn resolve_pr_head_sha(api_state: &ApiState, pr_number: u64) -> Result<String, String> {
    let octo = octocrab::Octocrab::builder()
        .personal_token(api_state.github_token.clone())
        .build()
        .map_err(|e| format!("Failed to build octocrab: {e}"))?;

    let pr = octo
        .pulls(&api_state.repo_owner, &api_state.repo_name)
        .get(pr_number)
        .await
        .map_err(|e| format!("Failed to fetch PR #{pr_number}: {e}"))?;

    Ok(pr.head.sha)
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
            let plan_output = match clone_at_sha(
                &clone_url,
                &head_sha,
                Some(&state.api_state.github_token),
            )
            .await
            {
                Ok(workdir_path) => {
                    let workdir = WorkdirGuard::new(workdir_path);
                    match yard_runner::resolve_project(workdir.path()).await {
                        Ok(project) => {
                            match yard_core::calculate_diff(
                                &project.manifest,
                                &project.current_state,
                            ) {
                                Ok(diffs) => format_plan_output(&diffs, &project.manifest.project),
                                Err(e) => {
                                    error!(pr = pr_number, "yard plan failed: {e}");
                                    format!("yard plan failed:\n{e}")
                                }
                            }
                        }
                        Err(e) => {
                            error!(pr = pr_number, "yard plan failed: {e}");
                            format!("yard plan failed:\n{e}")
                        }
                    }
                }
                Err(e) => {
                    error!(pr = pr_number, "Clone failed: {e}");
                    format!("Failed to clone repo:\n{e}")
                }
            };

            // Determine plan status
            let plan_failed = plan_output.contains("failed");
            let status = if plan_failed {
                PlanStatus::Failure
            } else {
                PlanStatus::Success
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
                .post_plan_comment(&owner, &repo, pr_number, &plan_output)
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

            // Refresh dashboard cache
            if let Err(e) = crate::api::dashboard::refresh_dashboard_cache(&state.api_state).await {
                warn!(error = %e, "Failed to refresh dashboard cache after plan");
            }

            status
        }
        WebhookAction::Apply {
            owner,
            repo,
            pr_number,
            head_sha,
            clone_url,
        } => {
            // Resolve head SHA if not provided (issue_comment events don't include it)
            let head_sha = if head_sha.is_empty() {
                match resolve_pr_head_sha(&state.api_state, pr_number).await {
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
                    match yard_runner::resolve_project(workdir.path()).await {
                        Ok(project) => {
                            match yard_core::apply(
                                &project.manifest,
                                &project.current_state,
                                &project.root_dir,
                                false,
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
                .post_plan_comment(&owner, &repo, pr_number, &apply_output)
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

            // Refresh dashboard cache
            if let Err(e) = crate::api::dashboard::refresh_dashboard_cache(&state.api_state).await {
                warn!(error = %e, "Failed to refresh dashboard cache after apply");
            }

            status
        }
        WebhookAction::Ignore => StatusCode::OK,
    }
}
