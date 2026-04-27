//! Browser-session login endpoints (Gap A from 25-VERIFICATION.md §1).
//!
//! These endpoints sit OUTSIDE the bearer-auth layer (mounted at the parent
//! router level alongside `github_router`). They cannot require auth
//! themselves — that's the chicken-and-egg this module exists to break.
//!
//! - `POST /api/auth/session` — body `{ "token": "<YARD_API_TOKEN>" }`.
//!   On match: returns 200 + `Set-Cookie: yard_session=<token>; HttpOnly; SameSite=Strict; Path=/; Secure`.
//!   On mismatch: returns 401, no Set-Cookie header.
//! - `POST /api/auth/logout` — clears the cookie unconditionally; returns 204.
//!
//! The cookie value IS `YARD_API_TOKEN`. There is no separate session id
//! and no session storage. Forging the cookie requires guessing the token,
//! same security level as forging the `Authorization: Bearer` header.
//!
//! ## `Secure` cookie attribute is always set
//!
//! The `Set-Cookie` header always includes `Secure`. yard-server does not
//! attempt to detect whether the inbound request was HTTPS — that heuristic
//! is broken behind a TLS-terminating proxy (axum sees `http://` because
//! the proxy talks plain HTTP to the app, even though the operator's
//! browser used HTTPS). Failing closed (always-Secure) is the right
//! direction: it guarantees the cookie is only sent on HTTPS connections.
//!
//! Practical consequence: the cookie path requires HTTPS for the browser
//! to send it back. Loopback HTTP-only callers (e.g. `http://127.0.0.1:3001`
//! during local-dev) cannot use the cookie path — they MUST use the
//! `Authorization: Bearer` header instead. This is documented in
//! `docs/server.md`.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, StatusCode, header::SET_COOKIE},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Deserialize;

use crate::api::error::ApiError;
use crate::auth::{AuthConfig, COOKIE_NAME, ct_eq};

/// `Set-Cookie` value used by `post_session` to set the yard_session cookie.
///
/// Attributes:
/// - `HttpOnly` — JS cannot read via `document.cookie` (XSS payload cannot
///   exfiltrate the token).
/// - `SameSite=Strict` — browser will not include the cookie on cross-site
///   requests (CSRF defence).
/// - `Path=/` — scoped to the yard-server origin.
/// - `Secure` — cookie is only sent over HTTPS connections (see module
///   doc-comment for why this is always set, not conditional).
///
/// No `Max-Age` / `Expires` — session cookie, cleared on browser close.
fn session_cookie_value(token: &str) -> String {
    format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/; Secure")
}

#[derive(Deserialize)]
pub struct SessionRequest {
    pub token: String,
}

/// Mount POST /api/auth/session and POST /api/auth/logout at the parent
/// router OUTSIDE the bearer-auth layer.
pub fn auth_session_router(auth_config: Arc<AuthConfig>) -> Router {
    Router::new()
        .route("/api/auth/session", post(post_session))
        .route("/api/auth/logout", post(post_logout))
        .with_state(auth_config)
}

pub async fn post_session(
    State(cfg): State<Arc<AuthConfig>>,
    Json(req): Json<SessionRequest>,
) -> Result<Response, ApiError> {
    let Some(expected) = cfg.token.as_deref() else {
        // WR-04: do NOT tell an unauthenticated caller that the server is
        // misconfigured (no token at all → no auth) — that's a useful
        // signal for internet scanning to map "yard-servers with auth
        // turned off". Return the same generic "invalid token" the
        // mismatch branch returns. Operator-facing visibility into the
        // misconfiguration goes through tracing::error! on the server
        // side, not the response body.
        //
        // In production this branch is effectively dead code:
        // main.rs::start_api_server reads YARD_API_TOKEN via
        // required_env, so cfg.token is always Some at boot. The branch
        // is reachable only in tests / future configurations that allow
        // None — fail closed and don't leak the configuration state.
        tracing::error!(
            "/api/auth/session called but YARD_API_TOKEN is not configured; \
             returning generic Unauthorized to caller"
        );
        return Err(ApiError::Unauthorized("invalid token".into()));
    };

    if !ct_eq(req.token.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::Unauthorized("invalid token".into()));
    }

    let cookie_value = session_cookie_value(expected);
    let header_value = HeaderValue::from_str(&cookie_value)
        .map_err(|e| ApiError::Internal(format!("failed to build Set-Cookie header: {e}")))?;

    let mut resp = (StatusCode::OK, "ok").into_response();
    resp.headers_mut().insert(SET_COOKIE, header_value);
    Ok(resp)
}

pub async fn post_logout() -> Result<Response, ApiError> {
    // Cleared cookie — Max-Age=0 tells the browser to drop the cookie
    // immediately. Use the same identifying attributes (HttpOnly,
    // SameSite=Strict, Path=/, Secure) so the cookie's identity matches
    // and the clear takes effect.
    //
    // `Secure` MUST be present on the clear because the set-cookie value
    // (`session_cookie_value`) is always-Secure (see module doc-comment
    // "Secure cookie attribute is always set"). RFC 6265bis "Leave Secure
    // Cookies Alone" + concrete browser behaviour (Chrome 80+, Firefox 79+)
    // forbid an insecure context from overwriting a Secure cookie — so a
    // logout request received over HTTP that returns a Set-Cookie WITHOUT
    // `Secure` would be rejected by the browser, leaving the (Secure)
    // session cookie in place and silently breaking sign-out. Mirror the
    // `Secure` attribute the original set-cookie used.
    //
    // The dev / loopback HTTP path is unaffected: when the cookie was set
    // over HTTP (which only happens in tests), the browser does not
    // require the clear to also originate from a secure context, and
    // when the cookie was set over HTTPS (production), the browser only
    // honors a Secure clear from a Secure context. Either way `Secure`
    // on the clear is correct.
    let header_value = HeaderValue::from_static(
        "yard_session=; HttpOnly; SameSite=Strict; Path=/; Secure; Max-Age=0",
    );
    let mut resp = (StatusCode::NO_CONTENT, "").into_response();
    resp.headers_mut().insert(SET_COOKIE, header_value);
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn make_router(token: Option<String>) -> Router {
        auth_session_router(Arc::new(AuthConfig {
            token,
            bypass_loopback: false,
        }))
    }

    #[tokio::test]
    async fn session_endpoint_returns_set_cookie_on_correct_token() {
        let app = make_router(Some("s3cret".into()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/session")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"token":"s3cret"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let cookie = resp
            .headers()
            .get(SET_COOKIE)
            .expect("Set-Cookie must be present on success")
            .to_str()
            .unwrap();
        assert!(cookie.starts_with("yard_session=s3cret;"), "got: {cookie}");
        assert!(cookie.contains("HttpOnly"), "missing HttpOnly: {cookie}");
        assert!(
            cookie.contains("SameSite=Strict"),
            "missing SameSite: {cookie}"
        );
        assert!(cookie.contains("Path=/"), "missing Path: {cookie}");
        assert!(
            cookie.contains("Secure"),
            "missing Secure (must be always-set per tech-debt note): {cookie}"
        );
    }

    #[tokio::test]
    async fn session_endpoint_returns_401_on_wrong_token() {
        let app = make_router(Some("s3cret".into()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/session")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"token":"wrong"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(
            resp.headers().get(SET_COOKIE).is_none(),
            "Set-Cookie must NOT be present on failure"
        );
    }

    #[tokio::test]
    async fn session_endpoint_returns_400_on_invalid_body() {
        let app = make_router(Some("s3cret".into()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/session")
                    .header("Content-Type", "application/json")
                    .body(Body::from("not json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        // axum's Json extractor rejects malformed bodies with 400 by default.
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn logout_endpoint_clears_cookie() {
        let app = make_router(Some("s3cret".into()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/logout")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let cookie = resp
            .headers()
            .get(SET_COOKIE)
            .expect("Set-Cookie must be present on logout")
            .to_str()
            .unwrap();
        assert!(cookie.starts_with("yard_session=;"), "got: {cookie}");
        assert!(cookie.contains("Max-Age=0"), "missing Max-Age=0: {cookie}");
        // BL-03 fix: clear must mirror the set-cookie's Secure attribute,
        // otherwise modern browsers refuse to overwrite the Secure
        // session cookie set by `post_session` (RFC 6265bis "Leave
        // Secure Cookies Alone"). Without Secure here, sign-out from
        // HTTPS silently fails.
        assert!(cookie.contains("Secure"), "missing Secure on clear: {cookie}");
    }

    #[tokio::test]
    async fn logout_endpoint_works_without_existing_cookie() {
        // No Cookie header on the inbound request — logout still 204s.
        let app = make_router(Some("s3cret".into()));
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/logout")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn session_endpoint_returns_401_when_token_unconfigured() {
        // Operator started with bypass_loopback=true and no token (loopback-only mode).
        // /api/auth/session has no token to match against → 401.
        let app = make_router(None);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/session")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"token":"anything"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
