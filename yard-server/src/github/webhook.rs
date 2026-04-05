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

#[derive(Debug, Deserialize)]
pub struct PullRequest {
    pub head: GitRef,
    pub base: GitRef,
    pub merged: Option<bool>,
}

#[derive(Debug, Deserialize)]
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
    pub fn owner_repo(&self) -> (&str, &str) {
        let (owner, repo) = self.full_name.split_once('/').unwrap_or(("", ""));
        (owner, repo)
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
    },
    /// PR merged — run yard apply
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

    if event_type != "pull_request" {
        return Ok(WebhookAction::Ignore);
    }

    // Parse payload
    let event: PullRequestEvent = serde_json::from_slice(body)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let (owner, repo): (&str, &str) = event.repository.owner_repo();
    let clone_url = event.repository.clone_url
        .clone()
        .unwrap_or_default();

    match event.action.as_str() {
        "opened" | "synchronize" => Ok(WebhookAction::Plan {
            owner: owner.to_string(),
            repo: repo.to_string(),
            pr_number: event.number,
            head_sha: event.pull_request.head.sha.clone(),
            clone_url,
        }),
        "closed" if event.pull_request.merged == Some(true) => Ok(WebhookAction::Apply {
            owner: owner.to_string(),
            repo: repo.to_string(),
            pr_number: event.number,
            head_sha: event.pull_request.head.sha.clone(),
            clone_url,
        }),
        _ => Ok(WebhookAction::Ignore),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
