//! OAuth2 provider configuration for Entra ID and Google Workspace (AUTH-03, AUTH-04).
//!
//! Implements PKCE authorization code flow using the `oauth2` crate v5.0.
//! Client secrets are NOT stored in the client -- they are resolved from
//! `SecretStore` (AWS Secrets Manager) at token exchange time per D-06.
//!
//! Provider construction from env vars uses graceful degradation (D-07):
//! a misconfigured provider is logged and skipped; the server starts with
//! only the successfully-constructed providers.
//!
//! Uses a custom `YardTokenResponse` (via `IdTokenExtraFields`) instead of
//! `BasicTokenResponse` so the OpenID Connect `id_token` is captured during
//! token exchange and available for email extraction in the callback handler.

use oauth2::basic::{
    BasicErrorResponse, BasicRevocationErrorResponse, BasicTokenIntrospectionResponse,
    BasicTokenType,
};
use oauth2::{
    AuthUrl, AuthorizationCode, Client, ClientId, ClientSecret, CsrfToken, EndpointNotSet,
    EndpointSet, ExtraTokenFields, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl,
    RefreshToken, Scope, StandardRevocableToken, StandardTokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};

/// Extra fields from the token response that capture `id_token` for
/// OpenID Connect email extraction (Plan 03). Without this, the oauth2
/// crate's `EmptyExtraTokenFields` silently drops the `id_token` during
/// deserialization.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct IdTokenExtraFields {
    /// The OpenID Connect ID token (JWT). Present when the `openid` scope
    /// was requested and the provider supports OIDC.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub id_token: Option<String>,
}
impl ExtraTokenFields for IdTokenExtraFields {}

/// Token response type that captures the `id_token` field alongside
/// the standard OAuth2 fields (access_token, refresh_token, etc.).
pub type YardTokenResponse = StandardTokenResponse<IdTokenExtraFields, BasicTokenType>;

/// OAuth2 client type that produces `YardTokenResponse` (with `id_token` support)
/// instead of the default `BasicTokenResponse` (which drops unknown fields).
type YardOAuthClient<
    HasAuthUrl = EndpointNotSet,
    HasDeviceAuthUrl = EndpointNotSet,
    HasIntrospectionUrl = EndpointNotSet,
    HasRevocationUrl = EndpointNotSet,
    HasTokenUrl = EndpointNotSet,
> = Client<
    BasicErrorResponse,
    YardTokenResponse,
    BasicTokenIntrospectionResponse,
    StandardRevocableToken,
    BasicRevocationErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
>;

/// A `YardOAuthClient` with auth URL and token URL set (type-state pattern).
/// Device auth, introspection, and revocation URLs are not needed for the
/// authorization code + PKCE flow.
type ConfiguredClient =
    YardOAuthClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

/// Configuration for a single OAuth2 provider.
///
/// Each struct holds the client (with auth/token URLs but NO client secret
/// baked in -- per D-06) and the `client_secret_arn` for deferred SecretStore
/// resolution at token exchange time.
pub struct OAuth2ProviderConfig {
    /// Short machine-readable identifier, e.g. "entra" or "google".
    pub name: String,
    /// Human-readable name for the login page, e.g. "Microsoft" or "Google".
    pub display_name: String,
    /// The oauth2 `Client` pre-configured with auth/token URLs and redirect URI.
    /// Uses `YardTokenResponse` to capture `id_token`. Client secret is NOT
    /// set here (D-06).
    pub client: ConfiguredClient,
    /// The Secrets Manager ARN (or other SecretStore key) holding the client
    /// secret. Resolved at token exchange time, never at construction.
    pub client_secret_arn: String,
}

/// Registry of configured OAuth2 providers. Built at startup from env vars
/// by `build_providers_from_env`.
pub struct ProviderRegistry {
    /// Successfully-constructed providers. May be empty (D-07: graceful
    /// degradation when all providers are misconfigured or unconfigured).
    pub providers: Vec<OAuth2ProviderConfig>,
    /// The redirect URI shared by all providers. Logged at startup.
    /// Used by tests and potentially by UI for display.
    #[allow(dead_code)]
    pub redirect_uri: String,
}

impl ProviderRegistry {
    /// Returns a list of (provider_id, display_name) tuples for the login page.
    pub fn provider_names(&self) -> Vec<(&str, &str)> {
        self.providers
            .iter()
            .map(|p| (p.name.as_str(), p.display_name.as_str()))
            .collect()
    }

    /// Look up a provider by its machine-readable ID.
    pub fn get_provider(&self, provider_id: &str) -> Option<&OAuth2ProviderConfig> {
        self.providers.iter().find(|p| p.name == provider_id)
    }

    /// Generate an authorization URL for the given provider, including PKCE
    /// challenge and CSRF state token.
    ///
    /// Returns `(auth_url, csrf_state, pkce_verifier)` as strings. The
    /// caller is responsible for persisting the CSRF state and PKCE verifier
    /// (e.g. in DynamoDB keyed by the state token) so the callback handler
    /// can validate the state and complete the exchange.
    pub fn generate_auth_url(
        &self,
        provider_id: &str,
    ) -> anyhow::Result<(String, String, String)> {
        let provider = self
            .get_provider(provider_id)
            .ok_or_else(|| anyhow::anyhow!("unknown OAuth2 provider: {provider_id}"))?;

        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

        let (auth_url, csrf_state) = provider
            .client
            .authorize_url(CsrfToken::new_random)
            .add_scope(Scope::new("openid".to_string()))
            .add_scope(Scope::new("email".to_string()))
            .set_pkce_challenge(pkce_challenge)
            .url();

        Ok((
            auth_url.to_string(),
            csrf_state.secret().clone(),
            pkce_verifier.secret().clone(),
        ))
    }

    /// Exchange an authorization code for tokens. The `client_secret` has
    /// already been resolved from SecretStore by the caller.
    ///
    /// Returns the token response containing access_token, optionally
    /// refresh_token, and the id_token (if the provider returned one).
    pub async fn exchange_code(
        &self,
        provider_id: &str,
        authorization_code: String,
        pkce_verifier: String,
        client_secret: String,
    ) -> anyhow::Result<YardTokenResponse> {
        let provider = self
            .get_provider(provider_id)
            .ok_or_else(|| anyhow::anyhow!("unknown OAuth2 provider: {provider_id}"))?;

        let http_client = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;

        let token_result = provider
            .client
            .clone()
            .set_client_secret(ClientSecret::new(client_secret))
            .exchange_code(AuthorizationCode::new(authorization_code))
            .set_pkce_verifier(PkceCodeVerifier::new(pkce_verifier))
            .request_async(&http_client)
            .await
            .map_err(|e| anyhow::anyhow!("token exchange failed: {e}"))?;

        Ok(token_result)
    }

    /// Refresh an access token using a refresh token. The `client_secret`
    /// has already been resolved from SecretStore by the caller.
    ///
    /// Used by Plan 03's token refresh endpoint per AUTH-05.
    pub async fn exchange_refresh(
        &self,
        provider_id: &str,
        refresh_token: String,
        client_secret: String,
    ) -> anyhow::Result<YardTokenResponse> {
        let provider = self
            .get_provider(provider_id)
            .ok_or_else(|| anyhow::anyhow!("unknown OAuth2 provider: {provider_id}"))?;

        let http_client = reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;

        let token_result = provider
            .client
            .clone()
            .set_client_secret(ClientSecret::new(client_secret))
            .exchange_refresh_token(&RefreshToken::new(refresh_token))
            .request_async(&http_client)
            .await
            .map_err(|e| anyhow::anyhow!("token refresh failed: {e}"))?;

        Ok(token_result)
    }
}

// ---------------------------------------------------------------------------
// Provider construction (pure functions + env-var wrappers)
// ---------------------------------------------------------------------------

/// Default redirect URI used when YARD_OAUTH_REDIRECT_URI is not set.
/// Matches the dev server port (3001) per project conventions.
const DEFAULT_REDIRECT_URI: &str = "http://127.0.0.1:3001/api/auth/oauth/callback";

/// Entra ID endpoint templates. Pitfall 4 from RESEARCH.md: tenant_id is
/// required -- no default to /common/ endpoint.
const ENTRA_AUTH_URL_TEMPLATE: &str =
    "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize";
const ENTRA_TOKEN_URL_TEMPLATE: &str =
    "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token";

/// Google OAuth2 endpoints (well-known, stable).
const GOOGLE_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Create a new `YardOAuthClient` with the given client ID.
/// This is the equivalent of `BasicClient::new(...)` but uses our custom
/// token response type.
fn new_yard_client(client_id: ClientId) -> YardOAuthClient {
    Client::new(client_id)
}

/// Pure builder: construct an Entra ID provider from explicit parameters.
/// No env var reads -- testable without `unsafe`.
fn build_entra_config(
    client_id: &str,
    tenant_id: &str,
    client_secret_arn: &str,
    redirect_url: &RedirectUrl,
) -> anyhow::Result<OAuth2ProviderConfig> {
    let auth_url_str = ENTRA_AUTH_URL_TEMPLATE.replace("{tenant_id}", tenant_id);
    let token_url_str = ENTRA_TOKEN_URL_TEMPLATE.replace("{tenant_id}", tenant_id);

    let auth_url = AuthUrl::new(auth_url_str)
        .map_err(|e| anyhow::anyhow!("invalid Entra auth URL: {e}"))?;
    let token_url = TokenUrl::new(token_url_str)
        .map_err(|e| anyhow::anyhow!("invalid Entra token URL: {e}"))?;

    let client = new_yard_client(ClientId::new(client_id.to_string()))
        .set_auth_uri(auth_url)
        .set_token_uri(token_url)
        .set_redirect_uri(redirect_url.clone());

    Ok(OAuth2ProviderConfig {
        name: "entra".to_string(),
        display_name: "Microsoft".to_string(),
        client,
        client_secret_arn: client_secret_arn.to_string(),
    })
}

/// Pure builder: construct a Google Workspace provider from explicit parameters.
/// No env var reads -- testable without `unsafe`.
fn build_google_config(
    client_id: &str,
    client_secret_arn: &str,
    redirect_url: &RedirectUrl,
) -> anyhow::Result<OAuth2ProviderConfig> {
    let auth_url = AuthUrl::new(GOOGLE_AUTH_URL.to_string())
        .map_err(|e| anyhow::anyhow!("invalid Google auth URL: {e}"))?;
    let token_url = TokenUrl::new(GOOGLE_TOKEN_URL.to_string())
        .map_err(|e| anyhow::anyhow!("invalid Google token URL: {e}"))?;

    let client = new_yard_client(ClientId::new(client_id.to_string()))
        .set_auth_uri(auth_url)
        .set_token_uri(token_url)
        .set_redirect_uri(redirect_url.clone());

    Ok(OAuth2ProviderConfig {
        name: "google".to_string(),
        display_name: "Google".to_string(),
        client,
        client_secret_arn: client_secret_arn.to_string(),
    })
}

/// Build a `ProviderRegistry` from environment variables.
///
/// Reads `YARD_OAUTH_ENTRA_*` and `YARD_OAUTH_GOOGLE_*` env vars. Per D-07,
/// each provider is constructed independently; if one fails (missing or
/// invalid env vars), a warning is logged and it is skipped. The returned
/// registry may contain 0, 1, or 2 providers.
///
/// The redirect URI is read from `YARD_OAUTH_REDIRECT_URI`, defaulting to
/// `http://127.0.0.1:3001/api/auth/oauth/callback`. Logged at startup for
/// operator verification (T-45-10).
pub fn build_providers_from_env() -> ProviderRegistry {
    let redirect_uri_str = std::env::var("YARD_OAUTH_REDIRECT_URI")
        .unwrap_or_else(|_| DEFAULT_REDIRECT_URI.to_string());

    tracing::info!(redirect_uri = %redirect_uri_str, "OAuth2 redirect URI configured");

    let redirect_url = match RedirectUrl::new(redirect_uri_str.clone()) {
        Ok(url) => url,
        Err(e) => {
            tracing::warn!(
                error = %e,
                uri = %redirect_uri_str,
                "invalid YARD_OAUTH_REDIRECT_URI; no OAuth2 providers will be configured"
            );
            return ProviderRegistry {
                providers: Vec::new(),
                redirect_uri: redirect_uri_str,
            };
        }
    };

    let mut providers = Vec::new();

    // D-07: each provider wrapped in match for graceful degradation.
    // Entra: requires CLIENT_ID, TENANT_ID, and CLIENT_SECRET (Secrets Manager ARN).
    if let Ok(client_id) = std::env::var("YARD_OAUTH_ENTRA_CLIENT_ID") {
        let tenant_id = std::env::var("YARD_OAUTH_ENTRA_TENANT_ID");
        let secret_arn = match std::env::var("YARD_OAUTH_ENTRA_CLIENT_SECRET") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                tracing::warn!(
                    provider = "entra",
                    "YARD_OAUTH_ENTRA_CLIENT_SECRET not set or empty; skipping provider"
                );
                // Skip — an empty ARN would fail at token exchange time, not at startup.
                String::new()
            }
        };

        if !secret_arn.is_empty() {
            match tenant_id {
                Ok(tid) => {
                    match build_entra_config(&client_id, &tid, &secret_arn, &redirect_url) {
                        Ok(provider) => {
                            tracing::info!(provider = "entra", "OAuth2 provider configured");
                            providers.push(provider);
                        }
                        Err(e) => {
                            tracing::warn!(
                                provider = "entra",
                                error = %e,
                                "OAuth2 provider configuration failed; skipping"
                            );
                        }
                    }
                }
                Err(_) => {
                    tracing::warn!(
                        provider = "entra",
                        "YARD_OAUTH_ENTRA_TENANT_ID not set (no default to /common/); skipping"
                    );
                }
            }
        }
    }

    // Google: requires CLIENT_ID and CLIENT_SECRET (Secrets Manager ARN).
    if let Ok(client_id) = std::env::var("YARD_OAUTH_GOOGLE_CLIENT_ID") {
        let secret_arn = match std::env::var("YARD_OAUTH_GOOGLE_CLIENT_SECRET") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                tracing::warn!(
                    provider = "google",
                    "YARD_OAUTH_GOOGLE_CLIENT_SECRET not set or empty; skipping provider"
                );
                String::new()
            }
        };

        if !secret_arn.is_empty() {
            match build_google_config(&client_id, &secret_arn, &redirect_url) {
                Ok(provider) => {
                    tracing::info!(provider = "google", "OAuth2 provider configured");
                    providers.push(provider);
                }
                Err(e) => {
                    tracing::warn!(
                        provider = "google",
                        error = %e,
                        "OAuth2 provider configuration failed; skipping"
                    );
                }
            }
        }
    }

    ProviderRegistry {
        providers,
        redirect_uri: redirect_uri_str,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_redirect_url() -> RedirectUrl {
        RedirectUrl::new(DEFAULT_REDIRECT_URI.to_string()).unwrap()
    }

    // --- Entra provider construction ---

    #[test]
    fn entra_config_with_valid_params_produces_correct_provider() {
        let provider = build_entra_config(
            "test-client-id",
            "test-tenant-123",
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:entra-secret",
            &test_redirect_url(),
        )
        .unwrap();

        assert_eq!(provider.name, "entra");
        assert_eq!(provider.display_name, "Microsoft");
        assert_eq!(
            provider.client_secret_arn,
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:entra-secret"
        );
    }

    #[test]
    fn entra_auth_url_contains_tenant_id() {
        let redirect_url = test_redirect_url();
        let provider =
            build_entra_config("cid", "my-tenant-abc", "arn:fake", &redirect_url).unwrap();

        let registry = ProviderRegistry {
            providers: vec![provider],
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        };

        let (auth_url, csrf_state, pkce_verifier) =
            registry.generate_auth_url("entra").unwrap();

        assert!(
            auth_url.contains("my-tenant-abc"),
            "auth URL should contain tenant_id: {auth_url}"
        );
        assert!(
            auth_url.starts_with("https://login.microsoftonline.com/"),
            "auth URL should start with Entra base: {auth_url}"
        );
        assert!(
            auth_url.contains("scope="),
            "auth URL should contain scope parameter: {auth_url}"
        );
        assert!(
            auth_url.contains("code_challenge="),
            "auth URL should contain PKCE code_challenge: {auth_url}"
        );
        assert!(!csrf_state.is_empty(), "CSRF state should not be empty");
        assert!(
            !pkce_verifier.is_empty(),
            "PKCE verifier should not be empty"
        );
    }

    // --- Google provider construction ---

    #[test]
    fn google_config_with_valid_params_produces_correct_provider() {
        let provider = build_google_config(
            "google-client-id",
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:google-secret",
            &test_redirect_url(),
        )
        .unwrap();

        assert_eq!(provider.name, "google");
        assert_eq!(provider.display_name, "Google");
    }

    #[test]
    fn google_auth_url_starts_with_correct_base() {
        let redirect_url = test_redirect_url();
        let provider =
            build_google_config("google-cid", "arn:fake", &redirect_url).unwrap();

        let registry = ProviderRegistry {
            providers: vec![provider],
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        };

        let (auth_url, _csrf_state, _pkce_verifier) =
            registry.generate_auth_url("google").unwrap();

        assert!(
            auth_url.starts_with("https://accounts.google.com/"),
            "auth URL should start with Google base: {auth_url}"
        );
    }

    // --- ProviderRegistry methods ---

    #[test]
    fn provider_names_returns_expected_tuples() {
        let redirect_url = test_redirect_url();
        let entra =
            build_entra_config("eid", "tid", "arn:e", &redirect_url).unwrap();
        let google =
            build_google_config("gid", "arn:g", &redirect_url).unwrap();

        let registry = ProviderRegistry {
            providers: vec![entra, google],
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        };

        let names = registry.provider_names();
        assert_eq!(names.len(), 2, "expected 2 providers: {names:?}");
        assert_eq!(names[0], ("entra", "Microsoft"));
        assert_eq!(names[1], ("google", "Google"));
    }

    #[test]
    fn get_provider_returns_none_for_unknown() {
        let registry = ProviderRegistry {
            providers: Vec::new(),
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        };
        assert!(registry.get_provider("nonexistent").is_none());
    }

    #[test]
    fn generate_auth_url_unknown_provider_returns_err() {
        let registry = ProviderRegistry {
            providers: Vec::new(),
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        };
        let result = registry.generate_auth_url("nonexistent");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("unknown"),
            "error should mention 'unknown': {msg}"
        );
    }

    #[test]
    fn empty_registry_has_no_providers() {
        let registry = ProviderRegistry {
            providers: Vec::new(),
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
        };
        assert!(registry.providers.is_empty());
        assert!(registry.provider_names().is_empty());
    }
}
