//! Git clone logic with GitHub App installation token authentication.
//!
//! Provides:
//! - `generate_installation_token`: obtains a fresh installation access token
//!   from the GitHub API using App credentials (D-01).
//! - `clone_repo`: shallow-clones a GitHub repo using the token (D-03).
//! - `validate_repo_url`: URL validation helper rejecting non-https and
//!   non-github.com URLs (T-40-08).
//!
//! Security notes:
//! - Auth URL (containing the token) is NEVER logged (T-40-07).
//! - Only the sanitized `repo_url` appears in log messages.
//! - `git clone` uses `--quiet` to suppress credential-leaking stderr.

use anyhow::{Context, Result};

/// Validate that a repo URL is an HTTPS GitHub URL and extract owner/repo.
///
/// Returns `(owner, repo)` on success. Rejects:
/// - Non-https schemes (T-40-08: URL injection prevention)
/// - Non-github.com hosts
/// - URLs missing owner/repo path segments
pub fn validate_repo_url(url: &str) -> Result<(String, String)> {
    if !url.starts_with("https://") {
        anyhow::bail!("repo URL must use https scheme, got: {url}");
    }

    // Extract the host+path portion after "https://"
    let after_scheme = &url["https://".len()..];

    // The host must be github.com (with optional trailing content after /)
    let (host, path) = match after_scheme.split_once('/') {
        Some((h, p)) => (h, p),
        None => anyhow::bail!("repo URL must contain a path after the host: {url}"),
    };

    if host != "github.com" {
        anyhow::bail!("repo URL must be on github.com, got host: {host}");
    }

    // Extract owner/repo from path, stripping optional .git suffix
    let path = path.trim_end_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);

    let parts: Vec<&str> = path.splitn(3, '/').collect();
    if parts.len() < 2 || parts[0].is_empty() || parts[1].is_empty() {
        anyhow::bail!("repo URL must contain owner/repo path: {url}");
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Generate a GitHub App installation access token.
///
/// Makes a direct HTTP request to the GitHub API to create an installation
/// access token. Uses a self-signed JWT (RS256) for App authentication.
///
/// # Arguments
/// * `app_id` - GitHub App ID
/// * `private_key_pem` - PEM-encoded RSA private key for the App
/// * `installation_id` - GitHub App installation ID
///
/// # Security
/// - The token value is NEVER logged (T-40-07).
/// - Debug log emitted on success confirms generation without leaking the token.
///
/// # Implementation Note
/// Uses octocrab's App authentication flow internally. The `installation_and_token`
/// method handles JWT signing and token retrieval.
#[allow(dead_code)]
pub async fn generate_installation_token(
    app_id: u64,
    private_key_pem: &str,
    installation_id: u64,
) -> Result<String> {
    // Build the JWT manually using base64 + hmac/sha2... but RS256 requires RSA.
    // Instead, use reqwest to POST to GitHub's installation token endpoint
    // after building the JWT using octocrab's internal auth flow.
    //
    // Since we cannot directly import jsonwebtoken or secrecy (transitive deps,
    // blocked by Rust edition 2024), we generate the JWT manually and call
    // GitHub's REST API with reqwest.

    let jwt = build_github_app_jwt(app_id, private_key_pem)?;

    let client = reqwest::Client::new();
    let url = format!(
        "https://api.github.com/app/installations/{installation_id}/access_tokens"
    );

    let resp = client
        .post(&url)
        .header("Authorization", format!("Bearer {jwt}"))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "yard-server")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .send()
        .await
        .context("failed to request installation access token")?;

    if !resp.status().is_success() {
        let status = resp.status();
        // Do NOT log the response body — it may contain sensitive details.
        anyhow::bail!(
            "GitHub installation token request failed with status {status}"
        );
    }

    #[derive(serde::Deserialize)]
    struct TokenResponse {
        token: String,
    }

    let token_resp: TokenResponse = resp
        .json()
        .await
        .context("failed to parse installation token response")?;

    tracing::debug!("GitHub App installation token generated successfully");

    Ok(token_resp.token)
}

/// Build a GitHub App JWT (RS256) for authentication.
///
/// The JWT is valid for 10 minutes (GitHub's maximum) and is used to
/// authenticate as the GitHub App when requesting installation tokens.
fn build_github_app_jwt(app_id: u64, private_key_pem: &str) -> Result<String> {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    // Parse the RSA private key from PEM
    let der = pem_to_der(private_key_pem)?;

    // JWT header: {"alg":"RS256","typ":"JWT"}
    let header = r#"{"alg":"RS256","typ":"JWT"}"#;
    let header_b64 = URL_SAFE_NO_PAD.encode(header.as_bytes());

    // JWT payload: iat (issued at), exp (expiration), iss (issuer = app_id)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock before Unix epoch")?
        .as_secs();

    // GitHub allows up to 10 minutes; use 9 minutes for clock skew safety
    let payload = format!(
        r#"{{"iat":{},"exp":{},"iss":"{}"}}"#,
        now.saturating_sub(60), // 60s in the past for clock skew
        now + 540,              // 9 minutes from now
        app_id
    );
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.as_bytes());

    // Signing input: base64(header).base64(payload)
    let signing_input = format!("{header_b64}.{payload_b64}");

    // Sign with RS256 (RSASSA-PKCS1-v1_5 with SHA-256)
    let signature = rsa_sha256_sign(&der, signing_input.as_bytes())?;
    let sig_b64 = URL_SAFE_NO_PAD.encode(&signature);

    Ok(format!("{signing_input}.{sig_b64}"))
}

/// Extract DER-encoded PKCS#8 or PKCS#1 private key from PEM text.
fn pem_to_der(pem: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    // Strip PEM headers/footers and decode base64
    let mut base64_content = String::new();
    for line in pem.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("-----") {
            continue;
        }
        if trimmed.is_empty() {
            continue;
        }
        base64_content.push_str(trimmed);
    }

    STANDARD
        .decode(&base64_content)
        .context("failed to decode PEM base64 content")
}

/// Sign data with RSA-SHA256 (RSASSA-PKCS1-v1_5).
///
/// Uses rustls's ring-based crypto provider for RSA signing, which is
/// already a direct dependency of yard-server.
fn rsa_sha256_sign(der: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    // rustls re-exports ring's signing primitives through its crypto provider.
    // We use the ring crate's signing API via rustls's dependency.
    //
    // However, rustls does not publicly re-export ring's raw signing API.
    // Instead, we use the WebPKI-compatible signing approach through
    // the rustls crypto provider.
    //
    // Since we cannot use ring directly (transitive dep), we use a different
    // approach: construct the PKCS#1 v1.5 signature manually using sha2 + rsa
    // padding.
    //
    // Actually, the simplest approach that works without adding deps:
    // Use the `rustls::crypto::ring::default_provider()` which gives us
    // access to ring's signing capabilities indirectly.

    // Try PKCS#8 first, then PKCS#1
    use rustls::pki_types::PrivateKeyDer;
    use rustls::sign::SigningKey;

    // Try parsing as PKCS#8
    let key_der = if der.len() > 2 && der[0] == 0x30 {
        // Try PKCS#8 first (more common for GitHub App keys)
        match rustls::crypto::ring::sign::RsaSigningKey::new(
            &PrivateKeyDer::Pkcs8(der.to_vec().into()),
        ) {
            Ok(key) => {
                let signer = key
                    .choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
                    .ok_or_else(|| anyhow::anyhow!("RSA key does not support PKCS1-SHA256"))?;
                return Ok(signer.sign(data)
                    .context("RSA-SHA256 signing failed")?
                    .to_vec());
            }
            Err(_) => {
                // Try PKCS#1
                PrivateKeyDer::Pkcs1(der.to_vec().into())
            }
        }
    } else {
        PrivateKeyDer::Pkcs1(der.to_vec().into())
    };

    let signing_key = rustls::crypto::ring::sign::RsaSigningKey::new(&key_der)
        .map_err(|e| anyhow::anyhow!("failed to load RSA private key: {e}"))?;

    let signer = signing_key
        .choose_scheme(&[rustls::SignatureScheme::RSA_PKCS1_SHA256])
        .ok_or_else(|| anyhow::anyhow!("RSA key does not support PKCS1-SHA256 scheme"))?;

    let signature = signer
        .sign(data)
        .context("RSA-SHA256 signing failed")?;

    Ok(signature.to_vec())
}

/// Clone a GitHub repository using an installation access token.
///
/// Performs a shallow clone (`--depth 1`, `--quiet`) into the given
/// destination directory. The auth URL is constructed internally and
/// NEVER logged (T-40-07).
///
/// # Arguments
/// * `repo_url` - HTTPS GitHub URL (e.g. `https://github.com/owner/repo`)
/// * `dest` - Local filesystem path to clone into
/// * `token` - GitHub installation access token
///
/// # Security
/// - Auth URL containing the token is never logged.
/// - git stderr is captured but not logged verbatim (may contain the auth URL).
/// - Only the sanitized `repo_url` appears in log/error messages.
#[allow(dead_code)]
pub async fn clone_repo(repo_url: &str, dest: &std::path::Path, token: &str) -> Result<()> {
    let (owner, repo) = validate_repo_url(repo_url)?;

    // Construct authenticated URL (NEVER log this)
    let auth_url = format!("https://x-access-token:{token}@github.com/{owner}/{repo}.git");

    let dest_str = dest
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("clone destination path is not valid UTF-8"))?;

    let output = tokio::process::Command::new("git")
        .args(["clone", "--depth", "1", "--quiet", &auth_url, dest_str])
        .output()
        .await
        .context("failed to spawn git clone process")?;

    if !output.status.success() {
        // T-40-07: Do NOT log stderr verbatim — it may contain the auth URL.
        // Provide a sanitized error message with only the public repo_url.
        anyhow::bail!(
            "git clone of {repo_url} failed with exit code {}",
            output.status.code().unwrap_or(-1)
        );
    }

    tracing::info!(repo_url = %repo_url, "repository cloned successfully");

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_repo_url_accepts_valid_https_github() {
        let (owner, repo) =
            validate_repo_url("https://github.com/sean-mca/yard").unwrap();
        assert_eq!(owner, "sean-mca");
        assert_eq!(repo, "yard");
    }

    #[test]
    fn test_validate_repo_url_accepts_dotgit_suffix() {
        let (owner, repo) =
            validate_repo_url("https://github.com/sean-mca/yard.git").unwrap();
        assert_eq!(owner, "sean-mca");
        assert_eq!(repo, "yard");
    }

    #[test]
    fn test_validate_repo_url_rejects_non_https() {
        let result = validate_repo_url("http://github.com/owner/repo");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("https"),
            "error should mention https requirement: {err}"
        );
    }

    #[test]
    fn test_validate_repo_url_rejects_ssh() {
        let result = validate_repo_url("git@github.com:owner/repo.git");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_repo_url_rejects_non_github() {
        let result = validate_repo_url("https://gitlab.com/owner/repo");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("github.com"),
            "error should mention github.com requirement: {err}"
        );
    }

    #[test]
    fn test_validate_repo_url_rejects_missing_repo() {
        let result = validate_repo_url("https://github.com/owner");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_repo_url_rejects_empty_owner() {
        let result = validate_repo_url("https://github.com//repo");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_repo_url_handles_trailing_slash() {
        let (owner, repo) =
            validate_repo_url("https://github.com/sean-mca/yard/").unwrap();
        assert_eq!(owner, "sean-mca");
        assert_eq!(repo, "yard");
    }
}
