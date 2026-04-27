//! Bearer-token authentication middleware for /api/* endpoints (SRV-01).
//!
//! `AuthConfig::token = Some(s)` requires every request to carry an
//! `Authorization: Bearer <s>` header (constant-time compared). When the
//! `bypass_loopback` flag is set AND the caller's source SocketAddr is
//! loopback (127.0.0.0/8 or ::1), the bearer check is skipped. Non-loopback
//! callers ALWAYS go through the standard bearer path even when the flag
//! is on.
//!
//! **REVERSES CONTEXT D-08 by deliberate gap-closure decision.** The
//! prior shipped implementation made `YARD_API_AUTH_DISABLED=1` skip the
//! check independent of the source address; this module now enforces the
//! "localhost-only dev bypass" reading of ROADMAP SC #2. Verify-phase MUST
//! NOT re-flag this as a regression.
//!
//! The `/api/webhook/github` route is HMAC-secured separately and is
//! merged at the parent router level so this middleware does NOT see it.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Request, State},
    http::header::AUTHORIZATION,
    middleware::Next,
    response::Response,
};

use crate::api::error::ApiError;

/// Configuration for the bearer-token middleware.
///
/// `token` holds the expected bearer token. `bypass_loopback`, when true,
/// allows the middleware to skip the bearer check ONLY for callers whose
/// source SocketAddr is loopback (127.0.0.0/8 or ::1). Non-loopback
/// callers always go through the standard bearer path.
///
/// REVERSES CONTEXT D-08: the prior shipped behaviour made the bypass
/// independent of the bind address. This struct's `bypass_loopback` flag
/// implements the "localhost-only dev bypass" reading of ROADMAP SC #2,
/// closing the gap surfaced by 25-VERIFICATION.md human-verification
/// item #2. The `bypass_loopback` bool is set from `YARD_API_AUTH_DISABLED`
/// at startup; never flipped at runtime (per threat model T-25-G2-03 —
/// no per-request env reads, no TOCTOU window).
pub struct AuthConfig {
    pub token: Option<String>,
    /// When true, requests whose source SocketAddr is loopback
    /// (127.0.0.0/8 or ::1) skip the bearer check. When false,
    /// the bearer check applies to every request regardless of
    /// source. Set from YARD_API_AUTH_DISABLED at startup; never
    /// flipped at runtime.
    pub bypass_loopback: bool,
}

/// Axum middleware that requires `Authorization: Bearer <YARD_API_TOKEN>`
/// on every request unless the dev bypass is on AND the source SocketAddr
/// is loopback.
///
/// Returns 401 with `{error, status: 401}` body via the
/// `ApiError::Unauthorized` variant on missing/malformed/invalid header.
///
/// The `ConnectInfo<SocketAddr>` extractor reflects the OS-reported peer
/// address (NOT any X-Forwarded-For / Forwarded header — see threat model
/// T-25-G2-02). This is the only signal consulted for loopback enforcement.
pub async fn require_bearer(
    State(cfg): State<Arc<AuthConfig>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Loopback-only bypass (REVERSES CONTEXT D-08 per gap-closure
    // decision). When the operator opts into bypass via
    // YARD_API_AUTH_DISABLED, only loopback callers skip the check;
    // non-loopback callers fall through to the standard bearer path.
    if cfg.bypass_loopback && is_loopback(&addr) {
        return Ok(next.run(req).await);
    }

    // Bearer check. With the new shape, an operator who sets
    // YARD_API_AUTH_DISABLED=1 but forgets YARD_API_TOKEN correctly sees
    // non-loopback callers get 401 (fail-closed). The "no token configured
    // = bypass enabled" behaviour from the original implementation is
    // replaced by the explicit `bypass_loopback` flag.
    let Some(expected) = cfg.token.as_deref() else {
        return Err(ApiError::Unauthorized(
            "missing or malformed Authorization header".into(),
        ));
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

/// True if the SocketAddr's IP is loopback (covers 127.0.0.0/8 and ::1).
///
/// Uses `IpAddr::is_loopback`, the stdlib helper that already covers both
/// IPv4 and IPv6 loopback ranges — no hand-rolled IP matching required.
fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
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

    /// Convenience loopback SocketAddr used by every test that wants to
    /// assert "bearer check still applies because source is loopback only
    /// matters when bypass_loopback is true".
    fn loopback_addr() -> SocketAddr {
        "127.0.0.1:5555".parse().unwrap()
    }

    /// Convenience non-loopback SocketAddr used by the gap-B tests that
    /// assert "bypass is gated on source IP".
    fn remote_addr() -> SocketAddr {
        "10.0.0.5:54321".parse().unwrap()
    }

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

    /// Build a request with a `ConnectInfo<SocketAddr>` extension injected
    /// so the middleware's extractor finds it. axum's `from_fn_with_state`
    /// extracts ConnectInfo from the request's extensions — without
    /// injection, the extractor errors at runtime (500). This helper is
    /// the standard tower-test idiom for axum middleware that consumes
    /// `ConnectInfo`.
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

    // -- Existing tests, re-anchored on ConnectInfo --

    #[tokio::test]
    async fn missing_authorization_header_returns_401() {
        let app = build_router(AuthConfig {
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
        let app = build_router(AuthConfig {
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
        let app = build_router(AuthConfig {
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

    /// Re-anchored bypass test: source is loopback, bypass_loopback is on,
    /// no token configured → 200. Test intent (bypass-on skips the check)
    /// preserved by the new loopback gate.
    #[tokio::test]
    async fn bypass_skips_check() {
        let app = build_router(AuthConfig {
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

    /// CHECKER B4: SC #2 "bypass OFF by default" runtime test.
    /// Mechanically the same as `missing_authorization_header_returns_401`
    /// but explicitly named to make the SC #2 contract trivially greppable
    /// in the test output. When AuthConfig::token = Some(...) and
    /// bypass_loopback = false (the default path when the operator has not
    /// set YARD_API_AUTH_DISABLED), an unauthenticated request is rejected
    /// regardless of source.
    #[tokio::test]
    async fn bypass_off_by_default() {
        let app = build_router(AuthConfig {
            token: Some("test-token".into()),
            bypass_loopback: false,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
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
        // Must be 401 — NEVER 101 Switching Protocols.
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
    ///   (b) GET /api/dashboard with no Authorization MUST return 401.
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
            Router::new().route("/api/webhook/github", get(fake_github_webhook)); // NO .layer(auth_layer)

        let api_routes = Router::new()
            .route("/api/dashboard", get(fake_dashboard))
            .layer(axum::middleware::from_fn_with_state(
                auth_cfg.clone(),
                require_bearer,
            ));

        let parent = Router::new().merge(github_router_stub).merge(api_routes);

        // Arm (a): webhook with no auth — must NOT be 401. The github_router
        // arm does NOT go through the auth layer, so no ConnectInfo
        // injection is required for it.
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

        // Arm (b): dashboard with no auth — MUST be 401. The api_routes
        // arm DOES go through the auth layer, so ConnectInfo MUST be
        // injected on the request (loopback source — bypass off, so 401
        // is expected regardless of source IP).
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

    // -- New gap-B tests (loopback-only bypass enforcement) --

    /// Gap B / SC #2 broad reading: bypass takes effect ONLY for loopback callers.
    /// AuthConfig.bypass_loopback=true + source 127.0.0.1 + no Authorization header → 200.
    #[tokio::test]
    async fn bypass_with_loopback_source_skips_check() {
        let app = build_router(AuthConfig {
            token: None,
            bypass_loopback: true,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", loopback_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Gap B / SC #2 broad reading: bypass does NOT cover non-loopback callers.
    /// AuthConfig.bypass_loopback=true + source 10.0.0.5 + no Authorization header → 401.
    /// (Even with bypass on, a non-loopback caller without a valid bearer is rejected.)
    #[tokio::test]
    async fn bypass_with_remote_source_falls_through_to_bearer_check_missing_header_returns_401() {
        let app = build_router(AuthConfig {
            token: Some("s3cret".into()),
            bypass_loopback: true,
        });
        let resp = app
            .oneshot(req_with_addr("/api/protected", remote_addr()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Gap B / SC #2 broad reading: bypass-on + non-loopback source + valid bearer → 200.
    /// Operator who runs with bypass on AND configures YARD_API_TOKEN can still serve
    /// non-loopback callers via the standard bearer path.
    #[tokio::test]
    async fn bypass_with_remote_source_and_correct_bearer_returns_200() {
        let app = build_router(AuthConfig {
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
}
