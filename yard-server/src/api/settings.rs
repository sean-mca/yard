use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use super::error::ApiError;

use super::dashboard::ApiState;

/// Validate a setting key/value pair against the known allowlist.
fn validate_setting(key: &str, value: &str) -> Result<(), String> {
    match key {
        // test_slack_webhook is an action trigger — value is ignored.
        "test_slack_webhook" => Ok(()),
        "theme" => match value {
            "light" | "dark" | "system" => Ok(()),
            _ => Err(format!(
                "invalid theme '{value}': must be light, dark, or system"
            )),
        },
        "drift_interval" => match value {
            "1" | "3" | "5" | "10" => Ok(()),
            _ => Err(format!(
                "invalid drift_interval '{value}': must be 1, 3, 5, or 10"
            )),
        },
        "dashboard_interval" => match value.parse::<u64>() {
            Ok(n) if n >= 1 => Ok(()),
            _ => Err(format!(
                "invalid dashboard_interval '{value}': must be a positive integer >= 1"
            )),
        },
        "slack_enabled" => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "invalid slack_enabled '{value}': must be true or false"
            )),
        },
        "slack_webhook_url" => Err(
            "slack_webhook_url is read-only; configure slack_webhook_secret_arn (a Secrets Manager ARN) instead. See docs/server.md."
                .to_string(),
        ),
        "slack_webhook_secret_arn" => {
            // Empty value is the documented "operator may clear" path.
            // Otherwise require a Secrets Manager ARN prefix; reject the
            // common mistake of pasting a Slack URL directly.
            if value.is_empty() || value.starts_with("arn:aws:secretsmanager:") {
                Ok(())
            } else if value.starts_with("https://hooks.slack.com/") {
                Err("slack_webhook_secret_arn must be a Secrets Manager ARN, not a Slack URL. \
                     Create a secret holding the URL and supply its ARN. See docs/server.md."
                    .to_string())
            } else {
                Err(format!(
                    "invalid slack_webhook_secret_arn '{value}': must be an arn:aws:secretsmanager:* ARN"
                ))
            }
        }
        "alert_drift_threshold" => match value.parse::<u32>() {
            Ok(n) if n >= 1 => Ok(()),
            _ => Err(format!(
                "invalid alert_drift_threshold '{value}': must be a positive integer >= 1"
            )),
        },
        "alert_cooldown_minutes" => match value.parse::<u64>() {
            Ok(n) if n >= 1 => Ok(()),
            _ => Err(format!(
                "invalid alert_cooldown_minutes '{value}': must be a positive integer >= 1"
            )),
        },
        "alert_last_sent_at" => Ok(()),
        _ => Err(format!("unknown setting '{key}'")),
    }
}

pub fn settings_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/settings", get(get_settings))
        .route("/api/settings", post(post_settings))
        .with_state(state)
}

#[derive(Serialize)]
struct SettingsResponse {
    settings: HashMap<String, String>,
}

async fn get_settings(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<SettingsResponse>, ApiError> {
    let items = state
        .db
        .list_settings()
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to fetch settings: {e}")))?;
    let settings: HashMap<String, String> =
        items.into_iter().map(|s| (s.key, s.value)).collect();
    info!(count = settings.len(), "Fetched settings");
    Ok(Json(SettingsResponse { settings }))
}

#[derive(Deserialize)]
struct SettingsPayload {
    settings: HashMap<String, String>,
}

async fn post_settings(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<SettingsPayload>,
) -> Result<StatusCode, ApiError> {
    // Validate all settings before writing any
    for (key, value) in &payload.settings {
        if let Err(msg) = validate_setting(key, value) {
            return Err(ApiError::BadRequest(msg));
        }
    }

    // Handle test_slack_webhook action trigger separately — never persisted.
    if payload.settings.contains_key("test_slack_webhook") {
        if payload.settings.len() > 1 {
            return Err(ApiError::BadRequest(
                "test_slack_webhook cannot be combined with other settings".into(),
            ));
        }
        return handle_test_slack_webhook(&state).await;
    }

    for (key, value) in &payload.settings {
        state
            .db
            .set_setting(key, value)
            .await
            .map_err(|e| ApiError::DatabaseError(format!("Failed to save setting '{key}': {e}")))?;
    }

    info!(count = payload.settings.len(), "Saved settings");
    Ok(StatusCode::OK)
}

/// Resolve the Slack webhook URL from Secrets Manager via the configured ARN,
/// then send a test message. Returns 200 on success, 400/500 on failure.
async fn handle_test_slack_webhook(state: &ApiState) -> Result<StatusCode, ApiError> {
    // Look up the stored ARN from settings.
    let items = state
        .db
        .list_settings()
        .await
        .map_err(|e| ApiError::DatabaseError(format!("Failed to fetch settings: {e}")))?;
    let settings: HashMap<String, String> =
        items.into_iter().map(|s| (s.key, s.value)).collect();

    let arn = settings
        .get("slack_webhook_secret_arn")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            ApiError::BadRequest(
                "No Slack webhook Secret ARN configured. Set the Secret ARN in Notifications settings first."
                    .to_string(),
            )
        })?;

    // Resolve the ARN to the actual webhook URL via SecretStore.
    let webhook_url = state
        .secret_store
        .resolve(arn)
        .await
        .map_err(|e| {
            ApiError::Internal(format!(
                "Failed to resolve Slack webhook secret: {e}"
            ))
        })?;

    // Send a test message.
    let test_payload = serde_json::json!({
        "blocks": [
            {
                "type": "header",
                "text": { "type": "plain_text", "text": "yard-server test notification" }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "This is a test message from yard-server. If you see this, your Slack webhook integration is working correctly."
                }
            }
        ]
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| ApiError::Internal(format!("Failed to create HTTP client: {e}")))?;

    let resp = client
        .post(&webhook_url)
        .header("Content-Type", "application/json")
        .json(&test_payload)
        .send()
        .await
        .map_err(|e| ApiError::Internal(format!("Slack webhook request failed: {e}")))?;

    if !resp.status().is_success() {
        let status = resp.status();
        // WR-05: log the full response body server-side for debugging, but do
        // not forward it to the browser — Slack's error body is untrusted
        // external content.
        let body = resp.text().await.unwrap_or_default();
        tracing::warn!(
            http_status = %status,
            response_body = %body,
            "Slack webhook test returned non-success status"
        );
        return Err(ApiError::Internal(format!(
            "Slack webhook returned HTTP {status}"
        )));
    }

    info!("Slack webhook test message sent successfully");
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, test_support::InMemoryDb};
    use crate::api::dashboard::ApiState;
    use axum::extract::State;
    use axum::response::IntoResponse;

    fn test_api_state() -> Arc<ApiState> {
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
        })
    }

    #[tokio::test]
    async fn test_get_settings_empty() {
        let state = test_api_state();
        let result = get_settings(State(state)).await.unwrap();
        assert!(result.0.settings.is_empty());
    }

    #[tokio::test]
    async fn test_post_settings_valid() {
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [("theme".to_string(), "dark".to_string())].into_iter().collect(),
        };
        let result = post_settings(State(state), Json(payload)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_post_settings_invalid_returns_400() {
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [("theme".to_string(), "neon".to_string())].into_iter().collect(),
        };
        let result = post_settings(State(state), Json(payload)).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_post_then_get_settings_roundtrip() {
        let state = test_api_state();
        // POST
        let payload = SettingsPayload {
            settings: [
                ("theme".to_string(), "dark".to_string()),
                ("drift_interval".to_string(), "5".to_string()),
            ].into_iter().collect(),
        };
        post_settings(State(state.clone()), Json(payload)).await.unwrap();
        // GET
        let result = get_settings(State(state)).await.unwrap();
        assert_eq!(result.0.settings.get("theme").unwrap(), "dark");
        assert_eq!(result.0.settings.get("drift_interval").unwrap(), "5");
    }

    #[test]
    fn rejects_unknown_key() {
        let result = validate_setting("bogus_key", "whatever");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown setting"));
    }

    #[test]
    fn rejects_invalid_theme() {
        assert!(validate_setting("theme", "neon").is_err());
    }

    #[test]
    fn accepts_valid_themes() {
        for val in &["light", "dark", "system"] {
            assert!(validate_setting("theme", val).is_ok());
        }
    }

    #[test]
    fn rejects_invalid_drift_interval() {
        assert!(validate_setting("drift_interval", "7").is_err());
        assert!(validate_setting("drift_interval", "abc").is_err());
    }

    #[test]
    fn accepts_valid_drift_intervals() {
        for val in &["1", "3", "5", "10"] {
            assert!(validate_setting("drift_interval", val).is_ok());
        }
    }

    #[test]
    fn rejects_invalid_dashboard_interval() {
        assert!(validate_setting("dashboard_interval", "abc").is_err());
        assert!(validate_setting("dashboard_interval", "-1").is_err());
    }

    #[test]
    fn accepts_valid_dashboard_interval() {
        assert!(validate_setting("dashboard_interval", "5").is_ok());
        assert!(validate_setting("dashboard_interval", "60").is_ok());
    }

    #[test]
    fn rejects_invalid_slack_enabled() {
        assert!(validate_setting("slack_enabled", "yes").is_err());
    }

    #[test]
    fn accepts_valid_slack_enabled() {
        assert!(validate_setting("slack_enabled", "true").is_ok());
        assert!(validate_setting("slack_enabled", "false").is_ok());
    }

    #[test]
    fn rejects_slack_webhook_url_with_redirect_message() {
        let result = validate_setting("slack_webhook_url", "https://hooks.slack.com/services/foo");
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("is read-only"), "expected 'is read-only' in: {msg}");
        assert!(
            msg.contains("slack_webhook_secret_arn"),
            "expected 'slack_webhook_secret_arn' in: {msg}"
        );
        assert!(
            msg.contains("docs/server.md"),
            "expected 'docs/server.md' in: {msg}"
        );
    }

    #[test]
    fn accepts_slack_webhook_secret_arn() {
        let result = validate_setting(
            "slack_webhook_secret_arn",
            "arn:aws:secretsmanager:us-east-1:123456789012:secret:yard/slack-webhook-AbCdEf",
        );
        assert!(result.is_ok());
        // Empty value also accepted — operator may clear the setting.
        assert!(validate_setting("slack_webhook_secret_arn", "").is_ok());
    }

    #[test]
    fn rejects_slack_url_pasted_as_secret_arn_with_redirect_message() {
        // Operator pastes a Slack hooks URL into the field labeled "Secret ARN".
        // Reject with a clear redirect message rather than persisting garbage.
        let result = validate_setting(
            "slack_webhook_secret_arn",
            "https://hooks.slack.com/services/T0/B0/abc",
        );
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(
            msg.contains("must be a Secrets Manager ARN"),
            "expected 'must be a Secrets Manager ARN' in: {msg}"
        );
        assert!(msg.contains("docs/server.md"), "expected docs link in: {msg}");
    }

    #[test]
    fn rejects_garbage_slack_webhook_secret_arn() {
        // A plain identifier (secret name without the full ARN), random text,
        // or any non-ARN value is rejected so the failure surfaces at write
        // time rather than hours later in the drift poll loop.
        assert!(validate_setting("slack_webhook_secret_arn", "yard/slack-webhook").is_err());
        assert!(validate_setting("slack_webhook_secret_arn", "random text").is_err());
        // ARNs for other AWS services are also rejected — the field must be a
        // Secrets Manager ARN specifically.
        assert!(validate_setting("slack_webhook_secret_arn", "arn:aws:s3:::bucket").is_err());
    }

    #[test]
    fn rejects_invalid_alert_drift_threshold() {
        assert!(validate_setting("alert_drift_threshold", "0").is_err());
        assert!(validate_setting("alert_drift_threshold", "-1").is_err());
        assert!(validate_setting("alert_drift_threshold", "abc").is_err());
    }

    #[test]
    fn accepts_valid_alert_drift_threshold() {
        assert!(validate_setting("alert_drift_threshold", "1").is_ok());
        assert!(validate_setting("alert_drift_threshold", "100").is_ok());
    }

    #[test]
    fn rejects_invalid_alert_cooldown_minutes() {
        assert!(validate_setting("alert_cooldown_minutes", "0").is_err());
        assert!(validate_setting("alert_cooldown_minutes", "-5").is_err());
        assert!(validate_setting("alert_cooldown_minutes", "abc").is_err());
    }

    #[test]
    fn accepts_valid_alert_cooldown_minutes() {
        assert!(validate_setting("alert_cooldown_minutes", "1").is_ok());
        assert!(validate_setting("alert_cooldown_minutes", "10").is_ok());
        assert!(validate_setting("alert_cooldown_minutes", "1440").is_ok());
    }

    #[test]
    fn accepts_any_alert_last_sent_at() {
        // Server-written key — lenient pass-through like slack_webhook_url.
        assert!(validate_setting("alert_last_sent_at", "2026-04-20T12:00:00Z").is_ok());
        assert!(validate_setting("alert_last_sent_at", "arbitrary-string").is_ok());
        assert!(validate_setting("alert_last_sent_at", "").is_ok());
    }

    #[tokio::test]
    async fn post_settings_rejects_invalid_alert_threshold_with_400() {
        // End-to-end test: post_settings handler catches validate_setting's Err
        // and wraps it in ApiError::BadRequest → HTTP 400 via IntoResponse.
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [("alert_drift_threshold".to_string(), "0".to_string())]
                .into_iter()
                .collect(),
        };
        let result = post_settings(State(state), Json(payload)).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn post_settings_rejects_legacy_slack_webhook_url_with_400() {
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [(
                "slack_webhook_url".to_string(),
                "https://hooks.slack.com/services/T0/B0/abc".to_string(),
            )]
            .into_iter()
            .collect(),
        };
        let result = post_settings(State(state), Json(payload)).await;
        assert!(result.is_err());
        let resp = result.unwrap_err().into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn post_settings_accepts_slack_webhook_secret_arn() {
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [(
                "slack_webhook_secret_arn".to_string(),
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:yard/slack-webhook-AbCdEf"
                    .to_string(),
            )]
            .into_iter()
            .collect(),
        };
        let result = post_settings(State(state.clone()), Json(payload)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::OK);

        // Round-trip: GET returns the ARN, never a URL.
        let got = get_settings(State(state)).await.unwrap();
        let arn = got
            .0
            .settings
            .get("slack_webhook_secret_arn")
            .expect("slack_webhook_secret_arn must be present in GET response");
        assert!(
            arn.starts_with("arn:aws:secretsmanager:"),
            "expected ARN, got: {arn}"
        );
    }

    #[tokio::test]
    async fn get_settings_response_excludes_plaintext_slack_url() {
        // SC #3 hard contract: serialized response payload must NEVER contain
        // a Slack hooks URL. Canonical happy-path: only ARN flows through GET.
        let state = test_api_state();
        // Write the canonical ARN.
        state
            .db
            .set_setting(
                "slack_webhook_secret_arn",
                "arn:aws:secretsmanager:us-east-1:000000000000:secret:yard/slack-webhook-X",
            )
            .await
            .unwrap();
        let resp = get_settings(State(state)).await.unwrap();
        let serialized = serde_json::to_string(&resp.0).unwrap();
        assert!(
            !serialized.contains("https://hooks.slack.com/"),
            "GET response leaked plaintext Slack URL: {serialized}"
        );
        assert!(
            serialized.contains("slack_webhook_secret_arn"),
            "GET response must include the ARN reference: {serialized}"
        );
    }

    // ---- test_slack_webhook action trigger tests ----

    #[test]
    fn validates_test_slack_webhook_key() {
        // test_slack_webhook is an action trigger — any value is accepted.
        assert!(validate_setting("test_slack_webhook", "").is_ok());
        assert!(validate_setting("test_slack_webhook", "ignored").is_ok());
    }

    #[tokio::test]
    async fn test_slack_webhook_rejects_when_no_arn_configured() {
        // When no slack_webhook_secret_arn is stored, the test action should
        // return a 400 telling the operator to configure the ARN first.
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [("test_slack_webhook".to_string(), String::new())]
                .into_iter()
                .collect(),
        };
        let result = post_settings(State(state), Json(payload)).await;
        assert!(result.is_err());
        let resp = result.unwrap_err().into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_slack_webhook_does_not_persist_key() {
        // The test_slack_webhook key is an action trigger and must never be
        // stored in DynamoDB. Verify GET /api/settings does not contain it
        // after a test_slack_webhook POST (even if the POST itself fails).
        let state = test_api_state();
        let payload = SettingsPayload {
            settings: [("test_slack_webhook".to_string(), String::new())]
                .into_iter()
                .collect(),
        };
        // POST will fail (no ARN configured), but that's fine — we're testing
        // that the key is not persisted regardless.
        let _ = post_settings(State(state.clone()), Json(payload)).await;
        let resp = get_settings(State(state)).await.unwrap();
        assert!(
            !resp.0.settings.contains_key("test_slack_webhook"),
            "test_slack_webhook must not be persisted to settings"
        );
    }

    #[tokio::test]
    async fn test_slack_webhook_resolves_secret_and_posts() {
        // End-to-end test with a fake Slack HTTP responder, mirroring the
        // pattern in alerting/slack.rs::resolve_and_post_integration.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use crate::secrets::test_support::InMemorySecretStore;

        // Bind a kernel-chosen port for the fake Slack webhook.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let webhook_url = format!("http://{addr}/test-webhook");

        let captured = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new()));
        let captured_clone = captured.clone();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.unwrap();
            captured_clone.lock().await.extend_from_slice(&buf[..n]);
            let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
            stream.write_all(resp).await.unwrap();
            stream.shutdown().await.ok();
        });

        let arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:yard/test-slack-X";
        let mut entries = std::collections::HashMap::new();
        entries.insert(arn.to_string(), webhook_url);
        let secret_store: Arc<dyn crate::secrets::SecretStore> =
            Arc::new(InMemorySecretStore::new(entries));

        let db = Arc::new(InMemoryDb::new());
        let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
        let state = Arc::new(ApiState {
            github_token: "t".into(),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            db: db.clone() as Arc<dyn Database>,
            event_tx,
            secret_store,
        });

        // Store the ARN in settings.
        db.set_setting("slack_webhook_secret_arn", arn)
            .await
            .unwrap();

        let payload = SettingsPayload {
            settings: [("test_slack_webhook".to_string(), String::new())]
                .into_iter()
                .collect(),
        };
        let result = post_settings(State(state), Json(payload)).await;
        assert!(result.is_ok(), "test_slack_webhook should succeed: {result:?}");
        assert_eq!(result.unwrap(), StatusCode::OK);

        server.await.unwrap();
        let captured_bytes = captured.lock().await.clone();
        let captured_str = String::from_utf8_lossy(&captured_bytes);

        // Verify the test payload arrived at the fake webhook.
        assert!(
            captured_str.starts_with("POST /test-webhook"),
            "expected POST /test-webhook, got: {}",
            &captured_str[..captured_str.len().min(80)]
        );
        assert!(
            captured_str.contains("yard-server test notification"),
            "test payload must contain the yard-server test notification text"
        );
    }
}
