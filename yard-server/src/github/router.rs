use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use std::sync::Arc;
use tracing::{info, warn};

use super::client::GitHubClient;
use super::webhook::{parse_webhook, WebhookAction};

/// Shared state for the webhook handler.
pub struct AppState {
    pub github_client: GitHubClient,
    pub webhook_secret: String,
}

/// Build the axum router for GitHub webhook endpoints.
pub fn github_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/webhook/github", post(handle_webhook))
        .with_state(state)
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

            // TODO: clone repo at head_sha, detect changed job files,
            // run yard plan via yard-core, collect output
            let plan_output = format!(
                "Plan for {owner}/{repo}#{pr_number} at {}\n\n(plan execution not yet wired)",
                &head_sha[..8]
            );

            match state
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
            }
        }
        WebhookAction::Apply {
            owner,
            repo,
            pr_number,
            head_sha,
            ..
        } => {
            info!(
                pr = pr_number,
                sha = %head_sha,
                repo = %format!("{owner}/{repo}"),
                "Running yard apply"
            );

            // TODO: clone repo at head_sha, detect changed job files,
            // run yard apply via yard-core
            StatusCode::OK
        }
        WebhookAction::Ignore => StatusCode::OK,
    }
}
