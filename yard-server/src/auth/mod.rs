//! Trait-based authentication middleware for /api/* endpoints (Phase 45).
//!
//! Replaces the prior `AuthConfig` + `require_bearer` system with a pluggable
//! `AuthProvider` trait. Two implementations:
//!   - `NoopAuth` — passes all requests without configuration (VPN-only, D-02).
//!   - `OAuth2Provider` (future, Plan 03) — session-cookie + OAuth2 SSO.
//!
//! The `require_auth` middleware checks in order:
//!   1. `is_noop()` — if true, pass through unconditionally.
//!   2. Session cookie (`yard_session=<session_id>`) — validated via
//!      `AuthProvider::validate_session`.
//!   3. `Authorization: Bearer <token>` header — fallback for CLI/automation
//!      (Pitfall 5 from RESEARCH.md). Delegated to the provider.
//!   4. If neither credential present: 401 JSON for `/api/*`, 302 redirect
//!      to `/login` for HTML routes (D-04).
//!
//! ## Cookie + Bearer coexistence
//!
//! The cookie path serves browsers (same-origin, HttpOnly, Secure). The bearer
//! path serves CLI / automation callers. When both are present, the cookie
//! takes precedence (browser sessions are the primary auth path in Phase 45+).

pub mod session;

pub mod oauth2;

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    extract::{ConnectInfo, Request, State},
    http::HeaderMap,
    http::header::{AUTHORIZATION, COOKIE},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::api::error::ApiError;

/// Name of the HttpOnly session cookie. Single source of truth shared between
/// the auth middleware here and the session endpoints in
/// `crate::api::auth_session`.
pub const COOKIE_NAME: &str = "yard_session";

// ---- AuthProvider Trait ----

/// Pluggable authentication backend (D-01).
///
/// Implementations:
///   - `NoopAuth` — zero-config VPN-only mode (D-02).
///   - `OAuth2Provider` (Plan 03) — session-cookie + OAuth2 SSO.
#[allow(dead_code)]
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Check if a session ID is valid. Returns the user email if valid,
    /// `None` if the session is expired or unknown.
    async fn validate_session(&self, session_id: &str) -> Option<String>;

    /// Whether this provider requires authentication at all. When true,
    /// `require_auth` passes all requests unconditionally.
    fn is_noop(&self) -> bool;

    /// Optional: check a bearer token for CLI/automation compatibility.
    /// Default implementation returns None (bearer not supported).
    /// OAuth2Provider overrides this to check YARD_API_TOKEN.
    async fn check_bearer(&self, _token: &str) -> Option<String> {
        None
    }
}

/// No-op authentication provider for VPN-only deployments (D-02).
/// All requests pass through without configuration. `validate_session`
/// always returns `Some("anonymous@noop")`.
#[allow(dead_code)]
pub struct NoopAuth;

#[async_trait]
impl AuthProvider for NoopAuth {
    async fn validate_session(&self, _session_id: &str) -> Option<String> {
        Some("anonymous@noop".to_string())
    }

    fn is_noop(&self) -> bool {
        true
    }
}

// ---- Middleware ----

/// Axum middleware that enforces authentication via the `AuthProvider` trait.
///
/// Check order:
/// 1. `is_noop()` — pass through immediately.
/// 2. Session cookie — validate via `AuthProvider::validate_session`.
/// 3. Bearer header — validate via `AuthProvider::check_bearer`.
/// 4. No credential — differentiate HTML (302 to /login) from API (401 JSON)
///    per D-04.
#[allow(dead_code)]
pub async fn require_auth(
    State(auth): State<Arc<dyn AuthProvider>>,
    ConnectInfo(_addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // 1. NoopAuth: pass through unconditionally.
    if auth.is_noop() {
        return Ok(next.run(req).await);
    }

    // 2. Check session cookie.
    if let Some(session_id) = extract_cookie_token(req.headers()) {
        let session_str = String::from_utf8_lossy(&session_id).to_string();
        if let Some(_email) = auth.validate_session(&session_str).await {
            return Ok(next.run(req).await);
        }
        // Session invalid/expired — differentiate HTML vs API (D-04).
        return if is_api_request(req.uri().path()) {
            Err(ApiError::Unauthorized("session expired".into()))
        } else {
            Ok(redirect_to_login_response())
        };
    }

    // 3. Check Authorization: Bearer header (CLI/automation fallback).
    if let Some(bearer_token) = extract_bearer(req.headers()) {
        if let Some(_email) = auth.check_bearer(&bearer_token).await {
            return Ok(next.run(req).await);
        }
        return Err(ApiError::Unauthorized("invalid credential".into()));
    }

    // 4. No credential present — differentiate HTML vs API (D-04).
    if is_api_request(req.uri().path()) {
        Err(ApiError::Unauthorized(
            "missing Authorization header and yard_session cookie".into(),
        ))
    } else {
        Ok(redirect_to_login_response())
    }
}

/// True if the request path starts with `/api/` — JSON error responses
/// are appropriate. Otherwise (HTML routes), redirect to `/login`.
#[allow(dead_code)]
fn is_api_request(path: &str) -> bool {
    path.starts_with("/api/")
}

/// Build a 302 redirect response to `/login`.
#[allow(dead_code)]
fn redirect_to_login_response() -> Response {
    let mut resp = StatusCode::FOUND.into_response();
    resp.headers_mut().insert(
        header::LOCATION,
        "/login".parse().expect("static /login path is valid"),
    );
    resp
}

// ---- Legacy AuthConfig (kept for api/auth_session.rs compatibility) ----

/// DEPRECATED: Legacy bearer-token configuration. Retained temporarily so
/// `api/auth_session.rs` compiles until Plan 03 rewrites it. Plan 03 will
/// remove this struct entirely.
pub struct AuthConfig {
    pub token: Option<String>,
    pub bypass_loopback: bool,
}

// ---- Helpers ----

/// Extract the value of the `yard_session` cookie from the request headers.
/// Returns `Some(value_bytes)` if present, `None` otherwise.
///
/// Cookie parsing is intentionally minimal — split on `;`, trim whitespace,
/// match the literal prefix `yard_session=`.
fn extract_cookie_token(headers: &HeaderMap) -> Option<Vec<u8>> {
    let raw = headers.get(COOKIE)?.to_str().ok()?;
    let prefix = format!("{COOKIE_NAME}=");
    for piece in raw.split(';') {
        let piece = piece.trim();
        if let Some(value) = piece.strip_prefix(prefix.as_str()) {
            // WR-03: an empty cookie value is "no credential present",
            // not "presented an invalid credential".
            if value.is_empty() {
                return None;
            }
            return Some(value.as_bytes().to_vec());
        }
    }
    None
}

/// Extract a Bearer token from the Authorization header.
#[allow(dead_code)]
fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

/// True if the SocketAddr's IP is loopback (covers 127.0.0.0/8 and ::1).
#[allow(dead_code)]
fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// Constant-time byte equality. Avoids early-return on first mismatch
/// to prevent timing-side-channel inference of the secret content.
///
/// Length mismatch DOES short-circuit (the deployment chooses a fixed
/// token length and the attacker has to guess 2^N bits per byte once
/// the length is known — content equality is the dominant attack
/// surface).
pub(crate) fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---- Legacy require_bearer (kept for api/auth_session.rs and main.rs compatibility) ----

/// DEPRECATED: Legacy bearer-token middleware. Retained so main.rs compiles
/// until Plan 03 wires the new `require_auth` middleware. Plan 03 will remove
/// this function entirely.
pub async fn require_bearer(
    State(cfg): State<Arc<AuthConfig>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if cfg.bypass_loopback && is_loopback(&addr) {
        return Ok(next.run(req).await);
    }

    let Some(expected) = cfg.token.as_deref() else {
        return Err(ApiError::Unauthorized(
            "missing Authorization header and yard_session cookie".into(),
        ));
    };

    check_credential(req.headers(), expected.as_bytes())?;

    Ok(next.run(req).await)
}

/// Legacy credential check — header-OR-cookie against a single expected token.
fn check_credential(headers: &HeaderMap, expected: &[u8]) -> Result<(), ApiError> {
    let header_token: Option<Vec<u8>> = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.as_bytes().to_vec());

    let cookie_token: Option<Vec<u8>> = extract_cookie_token(headers);

    let presented = match (header_token, cookie_token) {
        (Some(h), _) => h,
        (None, Some(c)) => c,
        (None, None) => {
            return Err(ApiError::Unauthorized(
                "missing Authorization header and yard_session cookie".into(),
            ));
        }
    };

    if !ct_eq(&presented, expected) {
        return Err(ApiError::Unauthorized("invalid credential".into()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use std::collections::HashMap;
    use tower::ServiceExt;

    // ---- Mock AuthProvider for tests ----

    struct MockAuthProvider {
        valid_sessions: HashMap<String, String>,
        valid_bearers: HashMap<String, String>,
        noop: bool,
    }

    impl MockAuthProvider {
        fn authenticated(sessions: Vec<(&str, &str)>) -> Self {
            Self {
                valid_sessions: sessions
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                valid_bearers: HashMap::new(),
                noop: false,
            }
        }

        fn authenticated_with_bearer(
            sessions: Vec<(&str, &str)>,
            bearers: Vec<(&str, &str)>,
        ) -> Self {
            Self {
                valid_sessions: sessions
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                valid_bearers: bearers
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                noop: false,
            }
        }

        fn rejecting() -> Self {
            Self {
                valid_sessions: HashMap::new(),
                valid_bearers: HashMap::new(),
                noop: false,
            }
        }
    }

    #[async_trait]
    impl AuthProvider for MockAuthProvider {
        async fn validate_session(&self, session_id: &str) -> Option<String> {
            self.valid_sessions.get(session_id).cloned()
        }

        fn is_noop(&self) -> bool {
            self.noop
        }

        async fn check_bearer(&self, token: &str) -> Option<String> {
            self.valid_bearers.get(token).cloned()
        }
    }

    // ---- Test Helpers ----

    fn loopback_addr() -> SocketAddr {
        "127.0.0.1:5555".parse().unwrap()
    }

    fn remote_addr() -> SocketAddr {
        "10.0.0.5:54321".parse().unwrap()
    }

    async fn ok_handler() -> impl IntoResponse {
        (StatusCode::OK, "ok")
    }

    fn build_auth_router(provider: Arc<dyn AuthProvider>) -> Router {
        Router::new()
            .route("/api/protected", get(ok_handler))
            .route("/html/page", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                provider,
                require_auth,
            ))
    }

    fn req_with_addr(uri: &str, addr: SocketAddr) -> Request<Body> {
        let mut req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        req.extensions_mut().insert(ConnectInfo::<SocketAddr>(addr));
        req
    }

    fn req_with_header_and_addr(
        uri: &str,
        header_name: &str,
        header_value: &str,
        addr: SocketAddr,
    ) -> Request<Body> {
        let mut req = Request::builder()
            .uri(uri)
            .header(header_name, header_value)
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(ConnectInfo::<SocketAddr>(addr));
        req
    }

    // ---- Legacy router for existing tests ----

    fn build_legacy_router(cfg: AuthConfig) -> Router {
        Router::new()
            .route("/api/protected", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(cfg),
                require_bearer,
            ))
    }

    // ============================================================
    // NEW AuthProvider + require_auth tests
    // ============================================================

    #[tokio::test]
    async fn noop_auth_validate_session_returns_anonymous() {
        let noop = NoopAuth;
        let email = noop.validate_session("anything").await;
        assert_eq!(email, Some("anonymous@noop".to_string()));
    }

    #[tokio::test]
    async fn noop_auth_is_noop_returns_true() {
        let noop = NoopAuth;
        assert!(noop.is_noop());
    }

    #[tokio::test]
    async fn require_auth_with_noop_passes_all_requests() {
        let provider: Arc<dyn AuthProvider> = Arc::new(NoopAuth);
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn require_auth_no_credential_api_returns_401() {
        let provider: Arc<dyn AuthProvider> =
            Arc::new(MockAuthProvider::rejecting());
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_no_credential_html_returns_302() {
        let provider: Arc<dyn AuthProvider> =
            Arc::new(MockAuthProvider::rejecting());
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_addr("/html/page", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FOUND);
        let location = resp
            .headers()
            .get(header::LOCATION)
            .expect("302 must have Location header")
            .to_str()
            .unwrap();
        assert_eq!(location, "/login");
    }

    #[tokio::test]
    async fn require_auth_valid_session_cookie_passes() {
        let provider: Arc<dyn AuthProvider> = Arc::new(
            MockAuthProvider::authenticated(vec![("valid-session-id", "user@example.com")]),
        );
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "yard_session=valid-session-id",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn require_auth_invalid_session_api_returns_401() {
        let provider: Arc<dyn AuthProvider> =
            Arc::new(MockAuthProvider::rejecting());
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "yard_session=bad-session",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_invalid_session_html_returns_302() {
        let provider: Arc<dyn AuthProvider> =
            Arc::new(MockAuthProvider::rejecting());
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/html/page",
                "Cookie",
                "yard_session=bad-session",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FOUND);
    }

    #[tokio::test]
    async fn require_auth_valid_bearer_passes() {
        let provider: Arc<dyn AuthProvider> = Arc::new(
            MockAuthProvider::authenticated_with_bearer(
                vec![],
                vec![("api-token-123", "cli-user@example.com")],
            ),
        );
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Authorization",
                "Bearer api-token-123",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn require_auth_invalid_bearer_returns_401() {
        let provider: Arc<dyn AuthProvider> = Arc::new(
            MockAuthProvider::authenticated_with_bearer(vec![], vec![]),
        );
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Authorization",
                "Bearer wrong-token",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_empty_cookie_treated_as_missing() {
        let provider: Arc<dyn AuthProvider> =
            Arc::new(MockAuthProvider::rejecting());
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "yard_session=",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_cookie_among_others_passes() {
        let provider: Arc<dyn AuthProvider> = Arc::new(
            MockAuthProvider::authenticated(vec![("my-session", "user@example.com")]),
        );
        let app = build_auth_router(provider);
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "theme=dark; yard_session=my-session; locale=en",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // ============================================================
    // Legacy require_bearer tests (preserved for backward compat)
    // ============================================================

    #[tokio::test]
    async fn missing_authorization_header_returns_401() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_bearer_returns_401() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Authorization",
                "Bearer wrong",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn correct_bearer_returns_200() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Authorization",
                "Bearer s3cret",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn bypass_skips_check() {
        let app = build_legacy_router(AuthConfig {
            token: None,
            bypass_loopback: true,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
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

    #[tokio::test]
    async fn bypass_off_by_default() {
        let app = build_legacy_router(AuthConfig {
            token: Some("test-token".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bypass_with_loopback_source_skips_check() {
        let app = build_legacy_router(AuthConfig {
            token: None,
            bypass_loopback: true,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn bypass_with_remote_source_falls_through_to_bearer_check_missing_header_returns_401() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: true,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", remote_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bypass_with_remote_source_and_correct_bearer_returns_200() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: true,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Authorization",
                "Bearer s3cret",
                remote_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cookie_only_returns_200() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "yard_session=s3cret",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invalid_cookie_returns_401() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "yard_session=wrong",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn header_takes_precedence_over_cookie_valid_header_wins() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let mut req = Request::builder()
            .uri("/api/protected")
            .header("Authorization", "Bearer s3cret")
            .header("Cookie", "yard_session=wrong")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(ConnectInfo::<SocketAddr>(loopback_addr()));
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cookie_alongside_other_cookies_returns_200() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "theme=dark; yard_session=s3cret; locale=en",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn empty_cookie_value_treated_as_missing_credential() {
        let app = build_legacy_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_header_and_addr(
                "/api/protected",
                "Cookie",
                "yard_session=",
                loopback_addr(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ws_upgrade_requires_auth() {
        let app = Router::new()
            .route("/api/ws/events", get(ok_handler))
            .layer(axum::middleware::from_fn_with_state(
                Arc::new(AuthConfig {
                    token: Some("s3cret".into()),
                    bypass_loopback: false,
                }),
                require_bearer,
            ));
        let mut req = Request::builder()
            .method("GET")
            .uri("/api/ws/events")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(ConnectInfo::<SocketAddr>(loopback_addr()));
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_ne!(resp.status().as_u16(), 101);
    }

    #[tokio::test]
    async fn webhook_route_skips_bearer_auth() {
        async fn fake_github_webhook() -> impl IntoResponse {
            (StatusCode::BAD_REQUEST, "missing signature")
        }
        async fn fake_dashboard() -> impl IntoResponse {
            (StatusCode::OK, "dashboard")
        }

        let auth_cfg = Arc::new(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: false,
        });

        let github_router_stub =
            Router::new().route("/api/webhook/github", get(fake_github_webhook));

        let api_routes = Router::new()
            .route("/api/dashboard", get(fake_dashboard))
            .layer(axum::middleware::from_fn_with_state(
                auth_cfg.clone(),
                require_bearer,
            ));

        let parent = Router::new().merge(github_router_stub).merge(api_routes);

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
        assert_eq!(resp_webhook.status(), StatusCode::BAD_REQUEST);

        let resp_dashboard = parent
            .oneshot(req_with_addr("/api/dashboard", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(
            resp_dashboard.status(),
            StatusCode::UNAUTHORIZED,
            "api dashboard route must be gated by the bearer auth layer"
        );
    }
}
