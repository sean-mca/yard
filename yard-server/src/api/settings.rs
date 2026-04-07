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
    for (key, value) in &payload.settings {
        if let Err(e) = state.db.set_setting(key, value).await {
            error!(key = %key, "Failed to save setting: {e}");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    }

    info!(count = payload.settings.len(), "Saved settings");
    StatusCode::OK.into_response()
}
