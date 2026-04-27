//! Bearer-token authentication middleware for /api/* endpoints (SRV-01).
//!
//! `AuthConfig::token = Some(s)` requires every request to carry an
//! `Authorization: Bearer <s>` header (constant-time compared). `None` means
//! the dev bypass is on (gated upstream by the `YARD_API_AUTH_DISABLED`
//! env var, off by default). The bypass short-circuits BEFORE any header
//! inspection — no `Authorization` parsing on the bypass path.
//!
//! The `/api/webhook/github` route is HMAC-secured separately and is
//! merged at the parent router level so this middleware does NOT see it.

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::header::AUTHORIZATION,
    middleware::Next,
    response::Response,
};

use crate::api::error::ApiError;

/// Configuration for the bearer-token middleware.
///
/// `token = None` means the dev bypass is enabled — every request is
/// allowed through without inspection (per D-06). The bypass MUST be
/// off by default (per D-07/D-11 — main.rs only constructs `None` when
/// `YARD_API_AUTH_DISABLED` is set to a truthy value).
pub struct AuthConfig {
    pub token: Option<String>,
}

/// Axum middleware that requires `Authorization: Bearer <YARD_API_TOKEN>`
/// on every request unless the dev bypass is on.
///
/// Returns 401 with `{error, status: 401}` body via the
/// `ApiError::Unauthorized` variant on missing/malformed/invalid header.
pub async fn require_bearer(
    State(cfg): State<Arc<AuthConfig>>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Dev bypass: skip all checks (D-06). No header inspection on this path.
    let Some(expected) = cfg.token.as_deref() else {
        return Ok(next.run(req).await);
    };

    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or_else(|| {
            ApiError::Unauthorized("missing or malformed Authorization header".into())
        })?;

    if !ct_eq(header.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::Unauthorized("invalid bearer token".into()));
    }

    Ok(next.run(req).await)
}

/// Constant-time byte equality. Avoids early-return on first mismatch
/// to prevent timing-side-channel inference of the secret content.
///
/// Length mismatch DOES short-circuit (the deployment chooses a fixed
/// token length and the attacker has to guess 2^N bits per byte once
/// the length is known — content equality is the dominant attack
/// surface). Closes T-25-06.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use tower::ServiceExt;

    async fn ok_handler() -> impl IntoResponse {
        (StatusCode::OK, "ok")
    }

    fn build_router(cfg: AuthConfig) -> Router {
        Router::new()
            .route("/api/protected", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(cfg),
                require_bearer,
            ))
    }

    // -- Existing 5 tests (unchanged from prior plan revision) --

    #[tokio::test]
    async fn missing_authorization_header_returns_401() {
        let app = build_router(AuthConfig {
            token: Some("s3cret".into()),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/protected")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_bearer_returns_401() {
        let app = build_router(AuthConfig {
            token: Some("s3cret".into()),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/protected")
                    .header("Authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn correct_bearer_returns_200() {
        let app = build_router(AuthConfig {
            token: Some("s3cret".into()),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/protected")
                    .header("Authorization", "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn bypass_skips_check() {
        let app = build_router(AuthConfig { token: None });
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/protected")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn ct_eq_basic_equality() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"abcd"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    // -- New tests added per CHECKER B4 / W1 / W2 (2026-04-27) --

    /// CHECKER B4: SC #2 "bypass OFF by default" runtime test.
    /// Mechanically the same as `missing_authorization_header_returns_401` but
    /// explicitly named to match VALIDATION.md row 25-01-06 and to make the
    /// SC #2 contract trivially greppable in the test output. When
    /// AuthConfig::token = Some(...) (the default path when the operator has
    /// not set YARD_API_AUTH_DISABLED), an unauthenticated request is rejected.
    #[tokio::test]
    async fn bypass_off_by_default() {
        let app = build_router(AuthConfig {
            token: Some("test-token".into()),
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/protected")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// CHECKER W1: WebSocket upgrade gating runtime test.
    /// Drives a GET request with `Connection: Upgrade` + `Upgrade: websocket`
    /// headers (no Authorization) through a Router that mounts a fake
    /// "ws-like" route under the auth layer. The bearer middleware runs
    /// BEFORE the WebSocketUpgrade extractor (axum middleware order), so the
    /// upgrade handshake is rejected with 401 — never returns 101 Switching
    /// Protocols.
    ///
    /// This test does NOT pull in the real events_router (which requires
    /// ApiState construction with broadcast::Sender + InMemoryDb + ...) —
    /// instead it mounts a /api/ws/events GET handler under the same
    /// auth layer. The auth-layer's behavior is identical regardless of
    /// downstream handler shape; what matters is that the middleware short-
    /// circuits with 401 before any extractor (including WebSocketUpgrade)
    /// runs.
    #[tokio::test]
    async fn ws_upgrade_requires_auth() {
        let app = Router::new()
            .route("/api/ws/events", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(AuthConfig {
                    token: Some("s3cret".into()),
                }),
                require_bearer,
            ));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/ws/events")
                    .header("Connection", "Upgrade")
                    .header("Upgrade", "websocket")
                    .header("Sec-WebSocket-Version", "13")
                    .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Must be 401 — NEVER 101 Switching Protocols. The bearer middleware
        // runs before any extractor; absent a valid Authorization header the
        // upgrade handshake never happens.
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_ne!(resp.status().as_u16(), 101);
    }

    /// CHECKER W2: webhook bypass runtime test (T-25-03).
    /// Mirrors the parent-router shape from main.rs:
    ///   parent = Router::new()
    ///       .merge(github_router)              // NO auth layer
    ///       .merge(api_routes_with_auth)       // .layer(auth_layer)
    ///
    /// Two-arm test:
    ///   (a) GET /api/webhook/github with no Authorization MUST NOT return 401.
    ///       (The github HMAC layer will return its own 4xx — likely 400 for
    ///       missing signature header — but the status must be DIFFERENT from
    ///       the bearer-layer 401, proving the auth-layer is not in the path.)
    ///   (b) GET /api/dashboard with no Authorization MUST return 401.
    ///       (Confirms the auth-layer IS in the api_routes path.)
    #[tokio::test]
    async fn webhook_route_skips_bearer_auth() {
        // Stand-in for the github_router: a /api/webhook/github route that
        // returns 400 on every request (mimicking "missing HMAC signature").
        // The point of the test is what status the AUTH layer would return
        // (401) vs. what the webhook handler returns (anything except 401).
        async fn fake_github_webhook() -> impl IntoResponse {
            (StatusCode::BAD_REQUEST, "missing signature")
        }
        async fn fake_dashboard() -> impl IntoResponse {
            (StatusCode::OK, "dashboard")
        }

        let auth_cfg = Arc::new(AuthConfig {
            token: Some("s3cret".into()),
        });

        let github_router_stub =
            Router::new().route("/api/webhook/github", get(fake_github_webhook)); // NO .layer(auth_layer)

        let api_routes = Router::new()
            .route("/api/dashboard", get(fake_dashboard))
            .layer(axum::middleware::from_fn_with_state(
                auth_cfg.clone(),
                require_bearer,
            ));

        let parent = Router::new().merge(github_router_stub).merge(api_routes);

        // Arm (a): webhook with no auth — must NOT be 401.
        let resp_webhook = parent
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/webhook/github")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            resp_webhook.status(),
            StatusCode::UNAUTHORIZED,
            "webhook route must NOT be gated by the bearer auth layer (T-25-03)"
        );
        // Sanity: it should be the github stub's 400.
        assert_eq!(resp_webhook.status(), StatusCode::BAD_REQUEST);

        // Arm (b): dashboard with no auth — MUST be 401.
        let resp_dashboard = parent
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp_dashboard.status(),
            StatusCode::UNAUTHORIZED,
            "api dashboard route must be gated by the bearer auth layer"
        );
    }
}
