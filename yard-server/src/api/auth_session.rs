//! OAuth2 session endpoints for browser-based authentication (Phase 45 Plan 03).
//!
//! These endpoints sit OUTSIDE the `require_auth` layer (mounted at the parent
//! router level alongside `github_router`). They cannot require auth
//! themselves — that's the chicken-and-egg this module exists to break.
//!
//! Routes:
//! - `GET  /api/auth/providers`      — list configured OAuth2 providers (for login page).
//! - `GET  /api/auth/oauth/start`    — redirect to provider authorize URL (PKCE + CSRF).
//! - `GET  /api/auth/oauth/callback` — exchange authorization code for tokens, create session.
//! - `POST /api/auth/oauth/refresh`  — exchange refresh token for new tokens, extend session (AUTH-05).
//! - `POST /api/auth/session`        — legacy bearer-token login (backward compat).
//! - `POST /api/auth/logout`         — delete session + clear cookie.
//!
//! ## `Secure` cookie attribute is always set
//!
//! See Phase 25 module doc-comment for rationale — always-Secure is fail-closed
//! and correct behind TLS-terminating proxies. The cookie path requires HTTPS
//! for the browser to send it back. Loopback HTTP-only callers use the
//! `Authorization: Bearer` header instead.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderValue, StatusCode, header::SET_COOKIE},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::api::error::ApiError;
use crate::auth::{COOKIE_NAME, ct_eq};
use crate::auth::oauth2::ProviderRegistry;
use crate::auth::session::{OAuthState, Session};
use crate::db::Database;
use crate::secrets::SecretStore;

/// Session duration in seconds: 8 hours (D-09).
const SESSION_MAX_AGE_SECS: i64 = 28800;

/// Maximum total session lifetime: 24 hours. Refresh cannot extend a session
/// beyond this horizon from its original `created_at` timestamp. Forces
/// re-authentication daily even when refresh tokens are available (CR-03).
const MAX_SESSION_LIFETIME_SECS: i64 = 86400;

/// Minimum remaining seconds before expiry at which a refresh is allowed.
/// Prevents amplification attacks where an attacker floods the refresh
/// endpoint while the session has plenty of remaining validity (WR-01).
const REFRESH_WINDOW_SECS: i64 = 1800; // 30 minutes

/// State shared by all auth session routes.
pub struct AuthSessionState {
    pub db: Arc<dyn Database>,
    pub secret_store: Arc<dyn SecretStore>,
    /// `None` when NoopAuth (no OAuth2 providers configured).
    pub provider_registry: Option<Arc<ProviderRegistry>>,
    /// Legacy YARD_API_TOKEN for bearer-token session creation (backward compat).
    pub api_token: Option<String>,
}

/// `Set-Cookie` value with 8-hour Max-Age (D-09).
///
/// Attributes:
/// - `HttpOnly` — JS cannot read via `document.cookie`.
/// - `SameSite=Strict` — CSRF defence.
/// - `Path=/` — scoped to the yard-server origin.
/// - `Secure` — only sent over HTTPS.
/// - `Max-Age=28800` — 8-hour session (D-09).
fn session_cookie_value(session_id: &str) -> String {
    format!(
        "{COOKIE_NAME}={session_id}; HttpOnly; SameSite=Strict; Path=/; Secure; Max-Age={SESSION_MAX_AGE_SECS}"
    )
}

/// Clear cookie — Max-Age=0 tells the browser to drop immediately.
fn clear_cookie_value() -> &'static str {
    "yard_session=; HttpOnly; SameSite=Strict; Path=/; Secure; Max-Age=0"
}

// ---- Request/Response types ----

#[derive(Deserialize)]
pub struct SessionRequest {
    pub token: String,
}

#[derive(Deserialize)]
pub struct OAuthStartQuery {
    pub provider: String,
}

#[derive(Deserialize)]
pub struct OAuthCallbackQuery {
    pub code: String,
    pub state: String,
}

#[derive(Serialize)]
struct ProvidersResponse {
    providers: Vec<ProviderInfo>,
}

#[derive(Serialize)]
struct ProviderInfo {
    id: String,
    name: String,
}

#[derive(Serialize)]
struct RefreshResponse {
    email: String,
    expires_at: String,
}

// ---- Router ----

/// Mount auth session routes at the parent router OUTSIDE the require_auth layer.
pub fn auth_session_router(state: Arc<AuthSessionState>) -> Router {
    Router::new()
        .route("/api/auth/providers", get(get_providers))
        .route("/api/auth/oauth/start", get(oauth_start))
        .route("/api/auth/oauth/callback", get(oauth_callback))
        .route("/api/auth/oauth/refresh", post(oauth_refresh))
        .route("/api/auth/session", post(post_session))
        .route("/api/auth/logout", post(post_logout))
        .with_state(state)
}

// ---- Handlers ----

/// GET /api/auth/providers — returns list of configured OAuth2 providers.
/// When NoopAuth (no provider_registry), returns empty list.
async fn get_providers(
    State(state): State<Arc<AuthSessionState>>,
) -> Result<Json<ProvidersResponse>, ApiError> {
    let providers = match &state.provider_registry {
        Some(registry) => registry
            .provider_names()
            .into_iter()
            .map(|(id, name)| ProviderInfo {
                id: id.to_string(),
                name: name.to_string(),
            })
            .collect(),
        None => Vec::new(),
    };
    Ok(Json(ProvidersResponse { providers }))
}

/// GET /api/auth/oauth/start?provider=entra — redirect to OAuth2 authorize URL.
/// Stores OAuthState (csrf_state, pkce_verifier, provider) in DynamoDB.
async fn oauth_start(
    State(state): State<Arc<AuthSessionState>>,
    Query(query): Query<OAuthStartQuery>,
) -> Result<Response, ApiError> {
    let registry = state
        .provider_registry
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("OAuth2 is not configured".into()))?;

    let (auth_url, csrf_state, pkce_verifier) = registry
        .generate_auth_url(&query.provider)
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    // Persist CSRF state + PKCE verifier for the callback (Pitfall 2).
    let oauth_state = OAuthState {
        csrf_state: csrf_state.clone(),
        pkce_verifier,
        provider: query.provider,
        created_at: Utc::now(),
    };
    state
        .db
        .store_oauth_state(&oauth_state)
        .await
        .map_err(|e| ApiError::Internal(format!("failed to store OAuth state: {e}")))?;

    // 302 redirect to the provider's authorize URL.
    let mut resp = StatusCode::FOUND.into_response();
    let location = HeaderValue::from_str(&auth_url)
        .map_err(|e| ApiError::Internal(format!("invalid auth URL: {e}")))?;
    resp.headers_mut()
        .insert(axum::http::header::LOCATION, location);
    Ok(resp)
}

/// GET /api/auth/oauth/callback?code=...&state=... — exchange code for tokens.
/// Creates a server-side Session in DynamoDB, sets the session cookie, redirects to "/".
async fn oauth_callback(
    State(state): State<Arc<AuthSessionState>>,
    Query(query): Query<OAuthCallbackQuery>,
) -> Result<Response, ApiError> {
    let registry = state
        .provider_registry
        .as_ref()
        .ok_or_else(|| ApiError::BadRequest("OAuth2 is not configured".into()))?;

    // Look up the OAuthState from DynamoDB using the CSRF state parameter.
    // WR-01: This is a primary-key get_item, not a string comparison — the
    // comparison happens server-side inside DynamoDB, so there is no
    // timing oracle from the client's perspective. The CSRF state token is
    // 128 bits of cryptographic randomness (CsrfToken::new_random()), making
    // brute-force infeasible regardless of comparison method.
    let oauth_state = state
        .db
        .get_oauth_state(&query.state)
        .await
        .map_err(|e| ApiError::Internal(format!("failed to look up OAuth state: {e}")))?
        .ok_or_else(|| {
            ApiError::BadRequest(
                "Invalid or expired state -- please try signing in again".into(),
            )
        })?;

    // Delete the OAuthState immediately (one-time use, T-45-12).
    if let Err(e) = state.db.delete_oauth_state(&query.state).await {
        tracing::warn!(error = %e, "failed to delete OAuth state (non-fatal)");
    }

    // Resolve the client secret from SecretStore (D-06, T-45-14).
    let provider_config = registry
        .get_provider(&oauth_state.provider)
        .ok_or_else(|| {
            ApiError::Internal(format!(
                "provider '{}' not found in registry",
                oauth_state.provider
            ))
        })?;

    let client_secret = state
        .secret_store
        .resolve(&provider_config.client_secret_arn)
        .await
        .map_err(|e| {
            tracing::error!(
                provider = %oauth_state.provider,
                error = %e,
                "failed to resolve OAuth2 client secret"
            );
            ApiError::Internal("authentication configuration error".into())
        })?;

    // Exchange the authorization code for tokens.
    let token_result = match registry
        .exchange_code(
            &oauth_state.provider,
            query.code,
            oauth_state.pkce_verifier,
            client_secret,
        )
        .await
    {
        Ok(result) => result,
        Err(e) => {
            tracing::error!(
                provider = %oauth_state.provider,
                error = %e,
                "OAuth2 code exchange failed"
            );
            // T-45-13: redirect to /login with error query param, not raw JSON.
            let mut resp = StatusCode::FOUND.into_response();
            resp.headers_mut().insert(
                axum::http::header::LOCATION,
                HeaderValue::from_static("/login?error=auth_failed"),
            );
            return Ok(resp);
        }
    };

    // Extract email from the ID token JWT claims (T-45-15).
    // No cryptographic verification — the token came directly from the provider
    // over TLS (standard practice for confidential clients per RFC 6749).
    let email = extract_email_from_id_token(token_result.extra_fields())
        .unwrap_or_else(|| "unknown@provider".to_string());

    // Extract refresh_token from the token response (AUTH-05, T-45-24).
    use oauth2::TokenResponse;
    let refresh_token = token_result
        .refresh_token()
        .map(|t| t.secret().clone());

    // Create session in DynamoDB.
    let session_id = uuid::Uuid::new_v4().to_string();
    let now = Utc::now();
    let expires_at = now + Duration::seconds(SESSION_MAX_AGE_SECS);

    let session = Session {
        session_id: session_id.clone(),
        email,
        provider: oauth_state.provider,
        refresh_token,
        created_at: now,
        expires_at,
    };

    state
        .db
        .create_session(&session)
        .await
        .map_err(|e| ApiError::Internal(format!("failed to create session: {e}")))?;

    // Set session cookie and redirect to dashboard.
    let cookie = session_cookie_value(&session_id);
    let cookie_header = HeaderValue::from_str(&cookie)
        .map_err(|e| ApiError::Internal(format!("failed to build Set-Cookie header: {e}")))?;

    let mut resp = StatusCode::FOUND.into_response();
    resp.headers_mut().insert(SET_COOKIE, cookie_header);
    resp.headers_mut().insert(
        axum::http::header::LOCATION,
        HeaderValue::from_static("/"),
    );
    Ok(resp)
}

/// POST /api/auth/oauth/refresh — exchange refresh token for new tokens (AUTH-05).
///
/// Called by the UI when the session is approaching expiry. Requires a valid
/// session cookie. Extends the session TTL by 8 hours.
async fn oauth_refresh(
    State(state): State<Arc<AuthSessionState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    let registry = state
        .provider_registry
        .as_ref()
        .ok_or_else(|| ApiError::Unauthorized("OAuth2 is not configured".into()))?;

    // Extract session_id from cookie.
    let session_id = extract_session_id_from_cookie(&headers)
        .ok_or_else(|| ApiError::Unauthorized("missing session cookie".into()))?;

    // Load the session from DynamoDB.
    let session = state
        .db
        .get_session(&session_id)
        .await
        .map_err(|e| ApiError::Internal(format!("failed to load session: {e}")))?
        .ok_or_else(|| ApiError::Unauthorized("session not found or expired".into()))?;

    // CR-03: enforce maximum session lifetime — sessions cannot be refreshed
    // beyond 24 hours from their original creation time.
    let max_lifetime = session.created_at + Duration::seconds(MAX_SESSION_LIFETIME_SECS);
    if Utc::now() > max_lifetime {
        // Delete the expired session so it cannot be retried.
        let _ = state.db.delete_session(&session_id).await;
        return Err(ApiError::Unauthorized(
            "session exceeded maximum lifetime -- please sign in again".into(),
        ));
    }

    // WR-01: only allow refresh within the last REFRESH_WINDOW_SECS (30 min)
    // of session validity. Prevents amplification attacks where an attacker
    // floods the refresh endpoint while the session has plenty of remaining time.
    let now = Utc::now();
    let remaining = (session.expires_at - now).num_seconds();
    if remaining > REFRESH_WINDOW_SECS {
        return Err(ApiError::BadRequest(
            "refresh not allowed yet -- session still has sufficient validity".into(),
        ));
    }

    // Check that the session has a refresh token.
    let refresh_token = session
        .refresh_token
        .ok_or_else(|| ApiError::Unauthorized("no refresh token available".into()))?;

    // Resolve the client secret for this provider.
    let provider_config = registry
        .get_provider(&session.provider)
        .ok_or_else(|| {
            ApiError::Internal(format!(
                "provider '{}' not found in registry",
                session.provider
            ))
        })?;

    let client_secret = state
        .secret_store
        .resolve(&provider_config.client_secret_arn)
        .await
        .map_err(|e| {
            tracing::error!(
                provider = %session.provider,
                error = %e,
                "failed to resolve OAuth2 client secret for refresh"
            );
            ApiError::Internal("authentication configuration error".into())
        })?;

    // Exchange the refresh token for new tokens.
    let token_result = registry
        .exchange_refresh(&session.provider, refresh_token, client_secret)
        .await
        .map_err(|e| {
            tracing::error!(
                provider = %session.provider,
                error = %e,
                "OAuth2 token refresh failed"
            );
            ApiError::Unauthorized("refresh token expired or revoked".into())
        })?;

    // Extract new refresh token (some providers rotate refresh tokens).
    use oauth2::TokenResponse;
    let new_refresh_token = token_result
        .refresh_token()
        .map(|t| t.secret().clone());

    // Extend session TTL by 8 hours.
    let new_expires_at = Utc::now() + Duration::seconds(SESSION_MAX_AGE_SECS);

    state
        .db
        .update_session_tokens(
            &session_id,
            new_refresh_token.as_deref(),
            new_expires_at,
        )
        .await
        .map_err(|e| ApiError::Internal(format!("failed to update session: {e}")))?;

    // Reset the cookie Max-Age to 28800 (8h clock reset).
    let cookie = session_cookie_value(&session_id);
    let cookie_header = HeaderValue::from_str(&cookie)
        .map_err(|e| ApiError::Internal(format!("failed to build Set-Cookie header: {e}")))?;

    let refresh_resp = RefreshResponse {
        email: session.email,
        expires_at: new_expires_at.to_rfc3339(),
    };

    let mut resp = Json(refresh_resp).into_response();
    resp.headers_mut().insert(SET_COOKIE, cookie_header);
    Ok(resp)
}

/// POST /api/auth/session — legacy bearer-token login (backward compat).
/// Creates a DynamoDB session so the cookie-based session system works for
/// bearer users too.
pub async fn post_session(
    State(state): State<Arc<AuthSessionState>>,
    Json(req): Json<SessionRequest>,
) -> Result<Response, ApiError> {
    let Some(expected) = state.api_token.as_deref() else {
        tracing::error!(
            "/api/auth/session called but YARD_API_TOKEN is not configured; \
             returning generic Unauthorized to caller"
        );
        return Err(ApiError::Unauthorized("invalid token".into()));
    };

    if !ct_eq(req.token.as_bytes(), expected.as_bytes()) {
        return Err(ApiError::Unauthorized("invalid token".into()));
    }

    // Create a DynamoDB session for the bearer user.
    let session_id = uuid::Uuid::new_v4().to_string();
    let now = Utc::now();
    let expires_at = now + Duration::seconds(SESSION_MAX_AGE_SECS);

    let session = Session {
        session_id: session_id.clone(),
        email: "bearer@local".to_string(),
        provider: "bearer".to_string(),
        refresh_token: None,
        created_at: now,
        expires_at,
    };

    state
        .db
        .create_session(&session)
        .await
        .map_err(|e| ApiError::Internal(format!("failed to create session: {e}")))?;

    let cookie = session_cookie_value(&session_id);
    let header_value = HeaderValue::from_str(&cookie)
        .map_err(|e| ApiError::Internal(format!("failed to build Set-Cookie header: {e}")))?;

    let mut resp = (StatusCode::OK, "ok").into_response();
    resp.headers_mut().insert(SET_COOKIE, header_value);
    Ok(resp)
}

/// POST /api/auth/logout — delete session from DynamoDB and clear cookie.
pub async fn post_logout(
    State(state): State<Arc<AuthSessionState>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    // Extract session_id from cookie and delete the session from DynamoDB.
    if let Some(session_id) = extract_session_id_from_cookie(&headers)
        && let Err(e) = state.db.delete_session(&session_id).await
    {
        tracing::warn!(error = %e, "failed to delete session from DynamoDB (non-fatal)");
    }

    let header_value = HeaderValue::from_static(clear_cookie_value());
    let mut resp = (StatusCode::NO_CONTENT, "").into_response();
    resp.headers_mut().insert(SET_COOKIE, header_value);
    Ok(resp)
}

// ---- Helpers ----

/// Extract the `yard_session` cookie value from request headers.
fn extract_session_id_from_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    let raw = headers
        .get(axum::http::header::COOKIE)?
        .to_str()
        .ok()?;
    let prefix = format!("{COOKIE_NAME}=");
    for piece in raw.split(';') {
        let piece = piece.trim();
        if let Some(value) = piece.strip_prefix(prefix.as_str()) {
            if value.is_empty() {
                return None;
            }
            // WR-03: reject obviously invalid session IDs early. Server-generated
            // IDs are UUID v4 (36 chars, hex + hyphens). This avoids passing
            // arbitrary strings (null bytes, megabytes) to DynamoDB.
            if !is_valid_session_id(value) {
                return None;
            }
            return Some(value.to_string());
        }
    }
    None
}

/// Check that a session ID looks like a UUID v4 (36 chars: 8-4-4-4-12, hex digits + hyphens).
fn is_valid_session_id(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    // Expected hyphen positions in a UUID: 8, 13, 18, 23
    for (i, ch) in s.chars().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if ch != '-' {
                    return false;
                }
            }
            _ => {
                if !ch.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

/// Extract the email claim from an OAuth2 ID token.
///
/// Parses the JWT payload (split on '.', base64url-decode the second part,
/// serde_json::from_slice). No cryptographic verification — the token came
/// directly from the provider's token endpoint over TLS (T-45-15).
///
/// Falls back to "sub" claim if "email" is not present.
fn extract_email_from_id_token(
    extra: &crate::auth::oauth2::IdTokenExtraFields,
) -> Option<String> {
    let id_token_str = extra.id_token.as_deref()?;

    // JWT structure: header.payload.signature
    let parts: Vec<&str> = id_token_str.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    // base64url decode the payload (second part).
    use base64::Engine;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let payload_bytes = engine.decode(parts[1]).ok()?;

    let claims: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;

    // Prefer "email", fall back to "sub".
    claims
        .get("email")
        .and_then(|v| v.as_str())
        .or_else(|| claims.get("sub").and_then(|v| v.as_str()))
        .map(|s| s.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::db::test_support::InMemoryDb;
    use crate::secrets::test_support::InMemorySecretStore;

    fn make_state(
        api_token: Option<String>,
        providers: Option<Arc<ProviderRegistry>>,
    ) -> Arc<AuthSessionState> {
        let db = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));
        Arc::new(AuthSessionState {
            db: db as Arc<dyn Database>,
            secret_store,
            provider_registry: providers,
            api_token,
        })
    }

    fn make_state_with_db(
        api_token: Option<String>,
        providers: Option<Arc<ProviderRegistry>>,
        db: Arc<dyn Database>,
        secret_store: Arc<dyn SecretStore>,
    ) -> Arc<AuthSessionState> {
        Arc::new(AuthSessionState {
            db,
            secret_store,
            provider_registry: providers,
            api_token,
        })
    }

    fn make_router(state: Arc<AuthSessionState>) -> Router {
        auth_session_router(state)
    }

    fn make_provider_registry() -> Arc<ProviderRegistry> {
        use crate::auth::oauth2::ProviderRegistry;
        use oauth2::{AuthUrl, Client, ClientId, RedirectUrl, TokenUrl};

        let redirect_url = RedirectUrl::new(
            "http://127.0.0.1:3001/api/auth/oauth/callback".to_string(),
        )
        .unwrap();

        let entra_client = Client::new(ClientId::new("entra-cid".into()))
            .set_auth_uri(
                AuthUrl::new(
                    "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/authorize"
                        .to_string(),
                )
                .unwrap(),
            )
            .set_token_uri(
                TokenUrl::new(
                    "https://login.microsoftonline.com/test-tenant/oauth2/v2.0/token"
                        .to_string(),
                )
                .unwrap(),
            )
            .set_redirect_uri(redirect_url.clone());

        let google_client = Client::new(ClientId::new("google-cid".into()))
            .set_auth_uri(
                AuthUrl::new("https://accounts.google.com/o/oauth2/v2/auth".to_string())
                    .unwrap(),
            )
            .set_token_uri(
                TokenUrl::new("https://oauth2.googleapis.com/token".to_string()).unwrap(),
            )
            .set_redirect_uri(redirect_url);

        Arc::new(ProviderRegistry {
            providers: vec![
                crate::auth::oauth2::OAuth2ProviderConfig {
                    name: "entra".to_string(),
                    display_name: "Microsoft".to_string(),
                    client: entra_client,
                    client_secret_arn: "arn:aws:secretsmanager:us-east-1:111:secret:entra"
                        .to_string(),
                },
                crate::auth::oauth2::OAuth2ProviderConfig {
                    name: "google".to_string(),
                    display_name: "Google".to_string(),
                    client: google_client,
                    client_secret_arn: "arn:aws:secretsmanager:us-east-1:111:secret:google"
                        .to_string(),
                },
            ],
            redirect_uri: "http://127.0.0.1:3001/api/auth/oauth/callback".to_string(),
        })
    }

    // ---- GET /api/auth/providers tests ----

    #[tokio::test]
    async fn providers_with_no_registry_returns_empty_list() {
        let state = make_state(None, None);
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/providers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["providers"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn providers_with_registry_returns_correct_list() {
        let registry = make_provider_registry();
        let state = make_state(None, Some(registry));
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/providers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let providers = json["providers"].as_array().unwrap();
        assert_eq!(providers.len(), 2);
        assert_eq!(providers[0]["id"], "entra");
        assert_eq!(providers[0]["name"], "Microsoft");
        assert_eq!(providers[1]["id"], "google");
        assert_eq!(providers[1]["name"], "Google");
    }

    // ---- GET /api/auth/oauth/start tests ----

    #[tokio::test]
    async fn oauth_start_unknown_provider_returns_400() {
        let registry = make_provider_registry();
        let state = make_state(None, Some(registry));
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/oauth/start?provider=nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn oauth_start_no_registry_returns_400() {
        let state = make_state(None, None);
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/oauth/start?provider=entra")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn oauth_start_valid_provider_returns_302() {
        let registry = make_provider_registry();
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));
        let state = make_state_with_db(
            None,
            Some(registry),
            db.clone(),
            secret_store,
        );
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/auth/oauth/start?provider=entra")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FOUND);
        let location = resp
            .headers()
            .get(axum::http::header::LOCATION)
            .expect("302 must have Location header")
            .to_str()
            .unwrap();
        assert!(
            location.contains("login.microsoftonline.com"),
            "redirect should go to Entra: {location}"
        );
    }

    // ---- POST /api/auth/logout tests ----

    #[tokio::test]
    async fn logout_deletes_session_and_clears_cookie() {
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));

        // Create a session first.
        let session = Session {
            session_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string(),
            email: "user@example.com".to_string(),
            provider: "entra".to_string(),
            refresh_token: None,
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::hours(8),
        };
        db.create_session(&session).await.unwrap();

        let state = make_state_with_db(
            Some("token".into()),
            None,
            db.clone(),
            secret_store,
        );
        let app = make_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/logout")
                    .header("Cookie", "yard_session=a1b2c3d4-e5f6-7890-abcd-ef1234567890")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Verify cookie is cleared.
        let cookie = resp
            .headers()
            .get(SET_COOKIE)
            .expect("Set-Cookie must be present on logout")
            .to_str()
            .unwrap();
        assert!(cookie.starts_with("yard_session=;"), "got: {cookie}");
        assert!(cookie.contains("Max-Age=0"), "missing Max-Age=0: {cookie}");
        assert!(cookie.contains("Secure"), "missing Secure on clear: {cookie}");

        // Verify session is deleted from database.
        let session_after = db.get_session("a1b2c3d4-e5f6-7890-abcd-ef1234567890").await.unwrap();
        assert!(session_after.is_none(), "session should be deleted from db");
    }

    // ---- POST /api/auth/oauth/refresh tests ----

    #[tokio::test]
    async fn oauth_refresh_no_session_returns_401() {
        let registry = make_provider_registry();
        let state = make_state(None, Some(registry));
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/oauth/refresh")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn oauth_refresh_session_without_refresh_token_returns_401() {
        let registry = make_provider_registry();
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));

        // Create a session without a refresh token.
        let session = Session {
            session_id: "b2c3d4e5-f6a7-8901-bcde-f12345678901".to_string(),
            email: "user@example.com".to_string(),
            provider: "entra".to_string(),
            refresh_token: None,
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::hours(8),
        };
        db.create_session(&session).await.unwrap();

        let state = make_state_with_db(
            None,
            Some(registry),
            db,
            secret_store,
        );
        let app = make_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/auth/oauth/refresh")
                    .header("Cookie", "yard_session=b2c3d4e5-f6a7-8901-bcde-f12345678901")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ---- POST /api/auth/session tests (backward compat) ----

    #[tokio::test]
    async fn session_endpoint_returns_set_cookie_on_correct_token() {
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let secret_store: Arc<dyn SecretStore> =
            Arc::new(InMemorySecretStore::new(HashMap::new()));
        let state = make_state_with_db(
            Some("s3cret".into()),
            None,
            db,
            secret_store,
        );
        let app = make_router(state);
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
        assert!(cookie.starts_with("yard_session="), "got: {cookie}");
        assert!(cookie.contains("HttpOnly"), "missing HttpOnly: {cookie}");
        assert!(
            cookie.contains("SameSite=Strict"),
            "missing SameSite: {cookie}"
        );
        assert!(cookie.contains("Path=/"), "missing Path: {cookie}");
        assert!(cookie.contains("Secure"), "missing Secure: {cookie}");
        assert!(
            cookie.contains("Max-Age=28800"),
            "missing Max-Age=28800 (8h): {cookie}"
        );
    }

    #[tokio::test]
    async fn session_endpoint_returns_401_on_wrong_token() {
        let state = make_state(Some("s3cret".into()), None);
        let app = make_router(state);
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
    }

    #[tokio::test]
    async fn session_endpoint_returns_401_when_token_unconfigured() {
        let state = make_state(None, None);
        let app = make_router(state);
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

    #[tokio::test]
    async fn logout_endpoint_works_without_existing_cookie() {
        let state = make_state(Some("s3cret".into()), None);
        let app = make_router(state);
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
}
