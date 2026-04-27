//! Shared HTTP-fetch helper for the bundled Dioxus UI (Gap A from
//! 25-VERIFICATION.md §1).
//!
//! Centralises the 401-redirect-to-/login side-effect and the non-success
//! status / JSON-parse error handling that every fetch site previously
//! duplicated. Single source of truth for the UI's read of the server's
//! cookie-auth contract:
//!
//!  - On 401: navigate to `Route::Login {}` (WASM target only) and return
//!    a uniform `"authentication required — redirecting to /login"` error
//!    string. Native builds skip the navigator call (no router on native;
//!    server-side prerender path) but still return Err so the caller's
//!    render path treats it as a transient error.
//!  - On any other non-success status: return
//!    `"Server error ({status}): {body}"` matching the existing UI error
//!    format so the dashboard-error rendering path is unchanged.
//!  - On parse failure: return `"Failed to parse response: {e}"`.
//!
//! No `Authorization` header injection: same-origin browser cookies (the
//! `yard_session` cookie set by `POST /api/auth/session`) are included
//! automatically by the browser's fetch implementation. The CLI / automation
//! `Authorization: Bearer` path is unchanged on the server side; the UI
//! does not use it.
//!
//! Each call site became a one-liner (`get_json::<T>(url).await`,
//! `get_text(url).await`, `post_json(url, &body).await`,
//! `post_no_body(url).await`) — net negative LOC vs. the seven-fold
//! duplication that existed before this module landed.

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Centralised 401-redirect side-effect. Pushes `Route::Login {}` on
/// WASM target; no-op on native (server-side prerender — no router).
fn redirect_to_login() {
    #[cfg(target_arch = "wasm32")]
    {
        use crate::Route;
        use dioxus::prelude::navigator;
        navigator().push(Route::Login {});
    }
}

/// Inspect a response's status BEFORE consuming the body. On 401 trigger
/// the redirect side-effect and return Err. On any other non-success status
/// consume the body to build an `"Server error ({status}): {body}"` string
/// matching the existing UI error format. On success return Ok(response)
/// unchanged so the caller can decode the body as JSON / text / etc.
async fn check_status_or_consume(resp: reqwest::Response) -> Result<reqwest::Response, String> {
    if resp.status().as_u16() == 401 {
        redirect_to_login();
        return Err("authentication required — redirecting to /login".to_string());
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Server error ({status}): {body}"));
    }
    Ok(resp)
}

/// GET `url` and parse the response body as JSON into `T`. Centralises the
/// 401-redirect, non-success status, and parse-error handling shared by the
/// dashboard / drift / jobs / settings query call sites.
pub async fn get_json<T: DeserializeOwned>(url: &str) -> Result<T, String> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| format!("Request failed: {e}"))?;
    let resp = check_status_or_consume(resp).await?;
    resp.json::<T>()
        .await
        .map_err(|e| format!("Failed to parse response: {e}"))
}

/// GET `url` and return the response body as plain text. Used by
/// `ui/jobs.rs::fetch_job_file` to fetch raw YAML.
pub async fn get_text(url: &str) -> Result<String, String> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| format!("Request failed: {e}"))?;
    let resp = check_status_or_consume(resp).await?;
    resp.text()
        .await
        .map_err(|e| format!("Failed to read response: {e}"))
}

/// POST `body` (serialised as JSON) to `url`. Returns `Ok(())` on success;
/// applies the same 401-redirect and non-success-status handling as the
/// GET helpers. Used by `ui/settings.rs::save_setting`.
pub async fn post_json<B: Serialize>(url: &str, body: &B) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .json(body)
        .send()
        .await
        .map_err(|e| format!("Request failed: {e}"))?;
    check_status_or_consume(resp).await?;
    Ok(())
}

/// POST to `url` with no request body (e.g. `/api/auth/logout`). Returns
/// `Ok(())` on success. Does NOT trigger a 401 redirect: the only caller
/// (logout) is unauthenticated by design and a 401 here is a server-side
/// misconfiguration the user should see, not a redirect target.
pub async fn post_no_body(url: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .send()
        .await
        .map_err(|e| format!("Request failed: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Server error ({status}): {body}"));
    }
    Ok(())
}

/// Variant of `get_json` that returns `Ok(default)` on any non-success
/// response that is NOT 401. Used by `DriftSummaryQuery::run` which
/// historically swallowed non-success as `Ok(0)` for graceful header
/// rendering; this helper preserves that semantics while still triggering
/// the 401-redirect side-effect.
pub async fn get_json_or_default<T: DeserializeOwned + Default>(url: &str) -> Result<T, String> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| format!("Request failed: {e}"))?;
    if resp.status().as_u16() == 401 {
        redirect_to_login();
        return Err("authentication required — redirecting to /login".to_string());
    }
    if !resp.status().is_success() {
        return Ok(T::default());
    }
    resp.json::<T>()
        .await
        .map_err(|e| format!("Failed to parse response: {e}"))
}
