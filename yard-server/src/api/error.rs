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
    #[allow(dead_code)]
    NotFound(String),
    BadRequest(String),
    Unauthorized(String),
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
            ApiError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
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
            | ApiError::Unauthorized(m)
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    async fn error_response(err: ApiError) -> (StatusCode, serde_json::Value) {
        let resp = err.into_response();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        (status, body)
    }

    #[tokio::test]
    async fn test_database_error_returns_500() {
        let (status, body) = error_response(ApiError::DatabaseError("db down".into())).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["status"], 500);
        assert_eq!(body["error"], "db down");
    }

    #[tokio::test]
    async fn test_github_error_returns_502() {
        let (status, body) = error_response(ApiError::GitHubError("rate limited".into())).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["status"], 502);
    }

    #[tokio::test]
    async fn test_not_found_returns_404() {
        let (status, body) = error_response(ApiError::NotFound("no such item".into())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["status"], 404);
    }

    #[tokio::test]
    async fn test_bad_request_returns_400() {
        let (status, body) = error_response(ApiError::BadRequest("invalid input".into())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
    }

    #[tokio::test]
    async fn test_unauthorized_returns_401() {
        let (status, body) = error_response(ApiError::Unauthorized(
            "missing or malformed Authorization header".into(),
        ))
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["status"], 401);
        assert_eq!(body["error"], "missing or malformed Authorization header");
    }

    #[tokio::test]
    async fn test_cache_unavailable_returns_503() {
        let (status, body) = error_response(ApiError::CacheUnavailable("not populated".into())).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], 503);
    }

    #[tokio::test]
    async fn test_internal_error_returns_500() {
        let (status, body) = error_response(ApiError::Internal("unexpected".into())).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["status"], 500);
    }
}
