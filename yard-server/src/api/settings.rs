use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

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

async fn get_settings(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match state.db.list_settings().await {
        Ok(items) => {
            let settings: HashMap<String, String> =
                items.into_iter().map(|s| (s.key, s.value)).collect();
            info!(count = settings.len(), "Fetched settings");
            (StatusCode::OK, Json(SettingsResponse { settings })).into_response()
        }
        Err(e) => {
            error!("Failed to fetch settings: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

#[derive(Deserialize)]
struct SettingsPayload {
    settings: HashMap<String, String>,
}

async fn post_settings(
    State(state): State<Arc<ApiState>>,
    Json(payload): Json<SettingsPayload>,
) -> impl IntoResponse {
    // Validate all settings before writing any
    for (key, value) in &payload.settings {
        if let Err(msg) = validate_setting(key, value) {
            return (StatusCode::BAD_REQUEST, msg).into_response();
        }
    }

    for (key, value) in &payload.settings {
        if let Err(e) = state.db.set_setting(key, value).await {
            error!(key = %key, "Failed to save setting: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }

    info!(count = payload.settings.len(), "Saved settings");
    StatusCode::OK.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
