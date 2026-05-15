//! Session model and helpers (Phase 45).
//!
//! Defines the `Session` and `OAuthState` structs used by the `Database` trait
//! for session CRUD and OAuth2 state management.
//!
//! - `Session` represents a server-side session stored in DynamoDB (D-08).
//!   Includes `refresh_token` for OAuth2 token refresh (AUTH-05, D-09).
//! - `OAuthState` stores the CSRF state -> PKCE verifier mapping needed
//!   between the OAuth2 redirect and callback (Pitfall 2 from RESEARCH.md).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A server-side session stored in DynamoDB (D-08).
///
/// Entity key: `PK=SESSION#{session_id}, SK=USER`.
/// TTL attribute: `ttl` = epoch seconds from `expires_at`.
///
/// Session duration is 8 hours (D-09). Refresh token extends within window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub email: String,
    /// Provider name: "entra", "google", or "noop".
    pub provider: String,
    /// OAuth2 refresh token for token refresh (AUTH-05, D-09).
    /// `None` for NoopAuth sessions.
    pub refresh_token: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// OAuth2 CSRF state -> PKCE verifier mapping stored in DynamoDB
/// (Pitfall 2 from RESEARCH.md).
///
/// Entity key: `PK=OAUTH_STATE#{csrf_state}, SK=PKCE`.
/// TTL: 10 minutes from creation (short-lived, deleted after exchange).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthState {
    pub csrf_state: String,
    pub pkce_verifier: String,
    pub provider: String,
    pub created_at: DateTime<Utc>,
}
