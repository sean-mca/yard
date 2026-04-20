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
        "dashboard_interval" => value
            .parse::<u64>()
            .map(|_| ())
            .map_err(|_| {
                format!("invalid dashboard_interval '{value}': must be a positive integer")
            }),
        "slack_enabled" => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "invalid slack_enabled '{value}': must be true or false"
            )),
        },
        "slack_webhook_url" => Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, test_support::InMemoryDb};
    use crate::api::dashboard::ApiState;
    use axum::extract::State;
    use axum::response::IntoResponse;

    fn test_api_state() -> Arc<ApiState> {
        let db = Arc::new(InMemoryDb::new());
        let (event_tx, _rx) = tokio::sync::broadcast::channel(16);
        Arc::new(ApiState {
            github_token: "t".into(),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            db: db as Arc<dyn Database>,
            event_tx,
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
    fn accepts_any_slack_webhook_url() {
        assert!(validate_setting("slack_webhook_url", "https://hooks.slack.com/services/foo").is_ok());
        assert!(validate_setting("slack_webhook_url", "").is_ok());
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
}
