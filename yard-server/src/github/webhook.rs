use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use serde::Deserialize;

type HmacSha256 = Hmac<Sha256>;

/// Verify the GitHub webhook signature (X-Hub-Signature-256).
pub fn verify_signature(secret: &str, payload: &[u8], signature_header: &str) -> bool {
    let Some(hex_sig) = signature_header.strip_prefix("sha256=") else {
        return false;
    };

    let Ok(sig_bytes) = hex::decode(hex_sig) else {
        return false;
    };

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };

    mac.update(payload);
    mac.verify_slice(&sig_bytes).is_ok()
}

/// Subset of a GitHub pull request event payload.
#[derive(Debug, Deserialize)]
pub struct PullRequestEvent {
    pub action: String,
    pub number: u64,
    pub pull_request: PullRequest,
    pub repository: Repository,
}

/// Subset of a GitHub issue_comment event payload.
#[derive(Debug, Deserialize)]
pub struct IssueCommentEvent {
    pub action: String,
    pub comment: Comment,
    pub issue: Issue,
    pub repository: Repository,
}

#[derive(Debug, Deserialize)]
pub struct Comment {
    pub body: String,
}

#[derive(Debug, Deserialize)]
pub struct Issue {
    pub number: u64,
    pub pull_request: Option<IssuePullRequest>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct IssuePullRequest {
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct PullRequest {
    pub head: GitRef,
    pub base: GitRef,
    pub merged: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct GitRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
    pub sha: String,
}

#[derive(Debug, Deserialize)]
pub struct Repository {
    pub full_name: String,
    pub clone_url: Option<String>,
}

impl Repository {
    pub fn owner_repo(&self) -> Option<(&str, &str)> {
        let (owner, repo) = self.full_name.split_once('/')?;
        if owner.is_empty() || repo.is_empty() {
            return None;
        }
        Some((owner, repo))
    }
}

/// The action to take based on the webhook event.
#[derive(Debug)]
pub enum WebhookAction {
    /// PR opened or updated — run yard plan
    Plan {
        owner: String,
        repo: String,
        pr_number: u64,
        head_sha: String,
        clone_url: String,
        target_filter: Option<String>,
    },
    /// User commented "yard apply" — run yard apply
    Apply {
        owner: String,
        repo: String,
        pr_number: u64,
        head_sha: String,
        clone_url: String,
    },
    /// Event we don't care about
    Ignore,
}

/// Parse the webhook payload and determine what action to take.
pub fn parse_webhook(
    headers: &HeaderMap,
    body: &Bytes,
    webhook_secret: &str,
) -> Result<WebhookAction, StatusCode> {
    // Verify signature
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    if !verify_signature(webhook_secret, body, signature) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Check event type
    let event_type = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::BAD_REQUEST)?;

    match event_type {
        "pull_request" => parse_pull_request_event(body),
        "issue_comment" => parse_issue_comment_event(body),
        _ => Ok(WebhookAction::Ignore),
    }
}

fn parse_pull_request_event(body: &Bytes) -> Result<WebhookAction, StatusCode> {
    let event: PullRequestEvent =
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (owner, repo) = event.repository.owner_repo().ok_or(StatusCode::BAD_REQUEST)?;
    let clone_url = event.repository.clone_url.clone().unwrap_or_default();

    match event.action.as_str() {
        "opened" | "synchronize" => Ok(WebhookAction::Plan {
            owner: owner.to_string(),
            repo: repo.to_string(),
            pr_number: event.number,
            head_sha: event.pull_request.head.sha.clone(),
            clone_url,
            target_filter: None,
        }),
        // Merge does not trigger apply — use "yard apply" comment instead
        _ => Ok(WebhookAction::Ignore),
    }
}

/// Parse `--target <name>` from a command string. Returns None if not present
/// or if the target name contains invalid characters.
fn parse_target_flag(input: &str) -> Option<String> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let pos = parts.iter().position(|&p| p == "--target")?;
    let target = parts.get(pos + 1)?;
    if target.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        Some(target.to_string())
    } else {
        None
    }
}

fn parse_issue_comment_event(body: &Bytes) -> Result<WebhookAction, StatusCode> {
    let event: IssueCommentEvent =
        serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;

    if event.action != "created" {
        return Ok(WebhookAction::Ignore);
    }

    if event.issue.pull_request.is_none() {
        return Ok(WebhookAction::Ignore);
    }

    let body_raw = event.comment.body.trim();
    let body_lower = body_raw.to_lowercase();

    let (owner, repo) = event.repository.owner_repo().ok_or(StatusCode::BAD_REQUEST)?;
    let clone_url = event.repository.clone_url.clone().unwrap_or_default();

    // Check "yard plan" before "yard apply" to avoid prefix collisions
    if body_lower.starts_with("yard plan") {
        let target_filter = parse_target_flag(body_raw);
        // Reject if --target was present but had invalid characters
        if body_lower.contains("--target") && target_filter.is_none() {
            return Ok(WebhookAction::Ignore);
        }
        return Ok(WebhookAction::Plan {
            owner: owner.to_string(),
            repo: repo.to_string(),
            pr_number: event.issue.number,
            head_sha: String::new(), // resolved by handler
            clone_url,
            target_filter,
        });
    }

    if body_lower == "yard apply" {
        return Ok(WebhookAction::Apply {
            owner: owner.to_string(),
            repo: repo.to_string(),
            pr_number: event.issue.number,
            head_sha: String::new(), // resolved by handler
            clone_url,
        });
    }

    Ok(WebhookAction::Ignore)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, payload: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn webhook_headers(event_type: &str, signature: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-github-event", event_type.parse().unwrap());
        headers.insert("x-hub-signature-256", signature.parse().unwrap());
        headers
    }

    #[test]
    fn test_pr_opened_routes_to_plan() {
        let secret = "test-secret";
        let payload = serde_json::json!({
            "action": "opened",
            "number": 42,
            "pull_request": {
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"}
            },
            "repository": {
                "full_name": "owner/repo",
                "clone_url": "https://github.com/owner/repo.git"
            }
        });
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("pull_request", &sig);

        let result = parse_webhook(&headers, &body_bytes.into(), secret);
        assert!(result.is_ok());
        match result.unwrap() {
            WebhookAction::Plan { pr_number, head_sha, target_filter, .. } => {
                assert_eq!(pr_number, 42);
                assert_eq!(head_sha, "abc123");
                assert!(target_filter.is_none());
            }
            other => panic!("Expected Plan, got {:?}", other),
        }
    }

    #[test]
    fn test_pr_closed_routes_to_ignore() {
        let secret = "s";
        let payload = serde_json::json!({
            "action": "closed",
            "number": 1,
            "pull_request": {
                "head": {"ref": "f", "sha": "a"},
                "base": {"ref": "m", "sha": "b"}
            },
            "repository": {"full_name": "o/r", "clone_url": "https://x.com"}
        });
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("pull_request", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        assert!(matches!(result, WebhookAction::Ignore));
    }

    #[test]
    fn test_issue_comment_yard_apply_routes_to_apply() {
        let secret = "s";
        let payload = serde_json::json!({
            "action": "created",
            "comment": {"body": "yard apply"},
            "issue": {"number": 10, "pull_request": {"url": "https://api.github.com/pulls/10"}},
            "repository": {"full_name": "o/r", "clone_url": "https://x.com"}
        });
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("issue_comment", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        match result {
            WebhookAction::Apply { pr_number, .. } => assert_eq!(pr_number, 10),
            other => panic!("Expected Apply, got {:?}", other),
        }
    }

    #[test]
    fn test_issue_comment_unrelated_routes_to_ignore() {
        let secret = "s";
        let payload = serde_json::json!({
            "action": "created",
            "comment": {"body": "looks good to me"},
            "issue": {"number": 10, "pull_request": {"url": "https://api.github.com/pulls/10"}},
            "repository": {"full_name": "o/r", "clone_url": "https://x.com"}
        });
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("issue_comment", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        assert!(matches!(result, WebhookAction::Ignore));
    }

    #[test]
    fn test_unknown_event_type_routes_to_ignore() {
        let secret = "s";
        let payload = b"{}";
        let sig = sign(secret, payload);
        let headers = webhook_headers("ping", &sig);
        let result = parse_webhook(&headers, &payload.to_vec().into(), secret).unwrap();
        assert!(matches!(result, WebhookAction::Ignore));
    }

    #[test]
    fn test_invalid_signature_rejected() {
        let headers = webhook_headers("pull_request", "sha256=invalid");
        let result = parse_webhook(&headers, &b"{}".to_vec().into(), "secret");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_signature_valid() {
        let secret = "test-secret";
        let payload = b"hello world";

        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload);
        let result = mac.finalize();
        let sig = format!("sha256={}", hex::encode(result.into_bytes()));

        assert!(verify_signature(secret, payload, &sig));
    }

    #[test]
    fn test_verify_signature_invalid() {
        assert!(!verify_signature("secret", b"payload", "sha256=deadbeef"));
    }

    #[test]
    fn test_verify_signature_bad_prefix() {
        assert!(!verify_signature("secret", b"payload", "sha1=abc"));
    }

    fn issue_comment_payload(action: &str, body: &str, is_pr: bool) -> serde_json::Value {
        let pr_field = if is_pr {
            serde_json::json!({"url": "https://api.github.com/pulls/10"})
        } else {
            serde_json::Value::Null
        };
        serde_json::json!({
            "action": action,
            "comment": {"body": body},
            "issue": {"number": 10, "pull_request": pr_field},
            "repository": {"full_name": "o/r", "clone_url": "https://x.com"}
        })
    }

    #[test]
    fn test_issue_comment_yard_plan_routes_to_plan() {
        let secret = "s";
        let payload = issue_comment_payload("created", "yard plan", true);
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("issue_comment", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        match result {
            WebhookAction::Plan { pr_number, target_filter, .. } => {
                assert_eq!(pr_number, 10);
                assert!(target_filter.is_none());
            }
            other => panic!("Expected Plan, got {:?}", other),
        }
    }

    #[test]
    fn test_issue_comment_yard_plan_target_routes_to_plan_with_filter() {
        let secret = "s";
        let payload = issue_comment_payload("created", "yard plan --target my-job", true);
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("issue_comment", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        match result {
            WebhookAction::Plan { target_filter, .. } => {
                assert_eq!(target_filter, Some("my-job".to_string()));
            }
            other => panic!("Expected Plan with target_filter, got {:?}", other),
        }
    }

    #[test]
    fn test_issue_comment_yard_plan_not_on_pr_ignored() {
        let secret = "s";
        let payload = issue_comment_payload("created", "yard plan", false);
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("issue_comment", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        assert!(matches!(result, WebhookAction::Ignore));
    }

    #[test]
    fn test_issue_comment_yard_plan_edited_ignored() {
        let secret = "s";
        let payload = issue_comment_payload("edited", "yard plan", true);
        let body_bytes = serde_json::to_vec(&payload).unwrap();
        let sig = sign(secret, &body_bytes);
        let headers = webhook_headers("issue_comment", &sig);
        let result = parse_webhook(&headers, &body_bytes.into(), secret).unwrap();
        assert!(matches!(result, WebhookAction::Ignore));
    }

    #[test]
    fn test_parse_target_flag_valid() {
        assert_eq!(parse_target_flag("yard plan --target foo"), Some("foo".to_string()));
    }

    #[test]
    fn test_parse_target_flag_missing() {
        assert_eq!(parse_target_flag("yard plan"), None);
    }

    #[test]
    fn test_parse_target_flag_invalid_chars() {
        assert_eq!(parse_target_flag("yard plan --target foo;rm"), None);
    }
}
