use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    DatabaseError(String),
    GitHubError(String),
    NotFound(String),
    BadRequest(String),
    CacheUnavailable(String),
    Internal(String),
}

impl ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::DatabaseError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::GitHubError(_) => StatusCode::BAD_GATEWAY,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::CacheUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn message(&self) -> &str {
        match self {
            ApiError::DatabaseError(m)
            | ApiError::GitHubError(m)
            | ApiError::NotFound(m)
            | ApiError::BadRequest(m)
            | ApiError::CacheUnavailable(m)
            | ApiError::Internal(m) => m,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = json!({
            "error": self.message(),
            "status": status.as_u16(),
        });
        (status, Json(body)).into_response()
    }
}
