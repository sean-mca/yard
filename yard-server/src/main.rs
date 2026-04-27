use dioxus::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
mod db;
#[cfg(not(target_arch = "wasm32"))]
mod github;
#[cfg(not(target_arch = "wasm32"))]
mod alerting;
#[cfg(not(target_arch = "wasm32"))]
mod auth;
#[cfg(not(target_arch = "wasm32"))]
mod secrets;
mod types;
mod ui;

const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

#[derive(Debug, Clone, PartialEq, Routable)]
enum Route {
    #[layout(Shell)]
    #[route("/")]
    Dashboard {},
    #[route("/jobs")]
    Jobs {},
    #[route("/drift")]
    Drift {},
    #[route("/settings")]
    Settings {},
    // Plan 25-05 Gap A: /login is reachable WITHOUT auth (it's the page that
    // enables auth). The Shell component below short-circuits on
    // `Route::Login {}` and renders just the bare Login page (no sidebar /
    // top-bar chrome) so the login page is visually unambiguous.
    #[route("/login")]
    Login {},
}

fn main() {
    #[cfg(not(target_arch = "wasm32"))]
    start_api_server();

    dioxus::launch(app);
}

/// Read a required environment variable, failing with a clear message if missing or empty.
#[cfg(not(target_arch = "wasm32"))]
fn required_env(name: &str) -> anyhow::Result<String> {
    match std::env::var(name).ok() {
        None => anyhow::bail!("{name} must be set"),
        Some(v) if v.is_empty() => anyhow::bail!("{name} must not be empty"),
        Some(v) => Ok(v),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn start_api_server() {
    use api::auth_session::auth_session_router;
    use api::dashboard::{ApiState, dashboard_router};
    use api::drift::drift_router;
    use api::jobs::jobs_router;
    use api::settings::settings_router;
    use db::DbConfig;
    use github::{client::{GitHubApi, GitHubClient}, router::AppState, router::github_router};
    use std::sync::Arc;
    use tower_governor::GovernorLayer;
    use tower_governor::governor::GovernorConfigBuilder;
    use axum::http::{Method, header::{AUTHORIZATION, CONTENT_TYPE}};
    use tower_http::cors::{AllowOrigin, CorsLayer};
    use tower_http::trace::TraceLayer;

    // Initialize structured logging (controlled via RUST_LOG env var)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Install rustls crypto provider before any TLS clients are created
    let _ = rustls::crypto::ring::default_provider().install_default();

    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        rt.block_on(async move {
            let github_token = required_env("YARD_GITHUB_TOKEN")?;
            let webhook_secret = required_env("YARD_WEBHOOK_SECRET")?;
            let repo_owner = required_env("YARD_REPO_OWNER")?;
            let repo_name = required_env("YARD_REPO_NAME")?;

            // SRV-01 / D-07 (REVISED by Phase 25 gap-closure plan 03 — REVERSES D-08):
            // explicit, off-by-default dev bypass that takes effect ONLY for loopback
            // callers. Trim whitespace + ASCII-lowercase before matching so common
            // operator inputs ("True", "YES", " 1\n" from a heredoc, etc.) disable
            // auth predictably.
            let bypass_loopback = std::env::var("YARD_API_AUTH_DISABLED")
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(false);

            if bypass_loopback {
                // WR-07: single canonical warn surface. Previously this
                // branch also emitted an eprintln! banner with the same
                // information, which broke JSON-formatted log shippers
                // (Loki, CloudWatch, Datadog) that line-parse stderr —
                // the tracing JSON line and the free-form eprintln!
                // line shared the same stream. tracing::warn! is the
                // canonical structured event; the operator's
                // tracing-subscriber config decides the surface format.
                tracing::warn!(
                    "YARD_API_AUTH_DISABLED=1 — /api/* endpoints skip the bearer check \
                     for LOOPBACK callers (127.0.0.1, ::1) only; non-loopback callers \
                     still require Authorization: Bearer. DO NOT use in production."
                );
            }

            // YARD_API_TOKEN is now ALWAYS required (even when bypass is on) so that
            // non-loopback callers can authenticate via the standard bearer path
            // when the bypass is enabled. Boot fails fast if it's unset.
            let api_token_raw = required_env("YARD_API_TOKEN")?;
            // WR-02: validate the token charset at BOOT, not at first
            // login. The token is interpolated into a `Set-Cookie` header
            // value (yard-server/src/api/auth_session.rs::session_cookie_value),
            // and `HeaderValue::from_str` rejects non-visible-ASCII at
            // runtime. Worse, RFC 6265 cookie-value syntax forbids `;`,
            // `,`, whitespace, double-quote, and control chars; a token
            // containing `;` would silently parse on the way in (the
            // cookie parser splits on `;`) but produce a truncated /
            // attribute-injectable Set-Cookie on the way out.
            //
            // Reject anything outside printable ASCII excluding the
            // cookie-syntax forbidden set. Error message must NOT echo
            // the token.
            if api_token_raw.bytes().any(|b| {
                !(0x21..=0x7E).contains(&b) || matches!(b, b';' | b',' | b'"' | b'\\')
            }) {
                anyhow::bail!(
                    "YARD_API_TOKEN must contain only printable ASCII (0x21..=0x7E) \
                     excluding `;`, `,`, `\"`, and `\\` — current value contains a \
                     forbidden character. See docs/server.md for the allowed charset."
                );
            }
            let api_token: Option<String> = Some(api_token_raw);

            let auth_config = std::sync::Arc::new(crate::auth::AuthConfig {
                token: api_token,
                bypass_loopback,
            });

            // SRV-02 / D-18: shared aws_config provider chain. Built once and
            // reused for the SecretsManager client so credentials and region
            // resolve consistently with the existing DynamoDB client.
            let sdk_config = aws_config::load_from_env().await;

            let secret_store: std::sync::Arc<dyn crate::secrets::SecretStore> =
                std::sync::Arc::new(crate::secrets::AwsSecretStore::new(&sdk_config));

            // Initialize DynamoDB persistence
            let db_config = DbConfig::from_env();
            let dynamo_db = db::DynamoDatabase::connect(
                &db_config.table_name,
                &db_config.region,
                db_config.endpoint_url.as_deref(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to DynamoDB: {e}"))?;
            dynamo_db.migrate()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to run DynamoDB migrations: {e}"))?;
            let db: std::sync::Arc<dyn db::Database> = std::sync::Arc::new(dynamo_db);

            // SRV-02 / D-22: refuse to boot if the legacy plaintext
            // slack_webhook_url row exists. Operator must migrate manually —
            // see docs/server.md.
            if db
                .get_setting("slack_webhook_url")
                .await
                .map_err(|e| anyhow::anyhow!("Failed to check legacy slack_webhook_url: {e}"))?
                .is_some()
            {
                anyhow::bail!(
                    "Legacy plaintext slack_webhook_url detected in DynamoDB Settings.\n\
                     yard-server v1.5+ requires Slack webhook URLs to live in AWS Secrets Manager.\n\
                     Migrate manually:\n  \
                     1) Copy the URL out of DynamoDB:\n     \
                        aws dynamodb get-item --table-name <yard_table> \\\n       \
                          --key '{{\"PK\":{{\"S\":\"SETTING#slack_webhook_url\"}},\"SK\":{{\"S\":\"SETTING#slack_webhook_url\"}}}}'\n  \
                     2) Create a Secrets Manager secret holding that URL:\n     \
                        aws secretsmanager create-secret --name yard/slack-webhook \\\n       \
                          --secret-string <THE_URL>\n  \
                     3) Delete the legacy row from DynamoDB:\n     \
                        aws dynamodb delete-item --table-name <yard_table> \\\n       \
                          --key '{{\"PK\":{{\"S\":\"SETTING#slack_webhook_url\"}},\"SK\":{{\"S\":\"SETTING#slack_webhook_url\"}}}}'\n  \
                     4) POST slack_webhook_secret_arn to /api/settings (or set via the UI).\n\
                     See docs/server.md for full instructions."
                );
            }

            let github_client: std::sync::Arc<dyn GitHubApi> = std::sync::Arc::new(
                GitHubClient::new(&github_token)
                    .map_err(|e| anyhow::anyhow!("Failed to create GitHub client: {e}"))?,
            );

            let (event_tx, _seed_rx) = api::events::new_event_channel();

            let api_state = Arc::new(ApiState {
                github_token,
                repo_owner,
                repo_name,
                db: db.clone(),
                event_tx,
                secret_store: secret_store.clone(),
            });

            let webhook_state = Arc::new(AppState {
                github_client,
                webhook_secret,
                db,
                api_state: api_state.clone(),
            });

            // BL-01: scope CORS to the cookie-auth model's same-origin
            // assumption. Previously this layer was open (allow_origin(Any)
            // + allow_methods(Any) + allow_headers(Any)) which (a) let any
            // origin drive-by probe /api/auth/session at 30 RPS via a
            // victim's browser, and (b) prevented future CSRF-token-style
            // mitigations because the server lost the same-origin signal.
            //
            // Operator opts in via YARD_CORS_ORIGIN (a single origin, e.g.
            // `https://dashboard.example.com`). When set, only that origin
            // gets a successful CORS preflight. When unset, fall back to
            // AllowOrigin::any() to preserve the current open-by-default
            // dev-friendly behaviour — but with tighter methods / headers
            // so the preflight grants only what the UI actually uses.
            //
            // Methods: GET (read endpoints, dashboard / drift / settings,
            // EventSource, WebSocket upgrade), POST (auth/session,
            // auth/logout, settings writes, GitHub webhook).
            //
            // Headers: Content-Type (JSON bodies on POSTs), Authorization
            // (CLI / automation Bearer header path; the cookie path does
            // not need a CORS-allowed header since cookies are not
            // request headers from the application's perspective).
            //
            // SameSite=Strict on yard_session still prevents the cookie
            // from riding cross-origin requests, and AllowOrigin::any()
            // is incompatible with credentials per the CORS spec, so the
            // cookie-auth attack surface is unchanged. This fix narrows
            // the preflight surface and gives operators a knob to scope
            // origins explicitly.
            let allow_origin = std::env::var("YARD_CORS_ORIGIN")
                .ok()
                .and_then(|s| s.parse::<axum::http::HeaderValue>().ok())
                .map(AllowOrigin::exact)
                .unwrap_or_else(AllowOrigin::any);
            let cors = CorsLayer::new()
                .allow_origin(allow_origin)
                .allow_methods([Method::GET, Method::POST])
                .allow_headers([CONTENT_TYPE, AUTHORIZATION]);

            let rate_limit_config = GovernorConfigBuilder::default()
                .per_second(30)
                .burst_size(60)
                .finish()
                .expect("Failed to build rate limiter config");
            let rate_limit = GovernorLayer::new(Arc::new(rate_limit_config));

            // SRV-01 / D-05: api routes (everything except the GitHub webhook,
            // which is HMAC-secured separately) sit behind the bearer layer.
            // Build them as a sub-router, layer auth, then merge into the
            // parent. The webhook router is merged at the parent level so its
            // `POST /api/webhook/github` path bypasses the bearer check.
            let api_routes = axum::Router::new()
                .merge(dashboard_router(api_state.clone()))
                .merge(jobs_router(api_state.clone()))
                .merge(drift_router(api_state.clone()))
                .merge(settings_router(api_state.clone()))
                .merge(api::events::events_router(api_state.clone()))
                .layer(axum::middleware::from_fn_with_state(
                    auth_config.clone(),
                    crate::auth::require_bearer,
                ));

            let router = axum::Router::new()
                .merge(github_router(webhook_state))
                // Plan 25-04 Gap A: /api/auth/session and /api/auth/logout sit
                // OUTSIDE the bearer-auth layer (chicken-and-egg — login can't
                // require login). Both endpoints are still rate-limited by the
                // .layer(rate_limit) below since rate_limit is on the parent
                // router.
                .merge(auth_session_router(auth_config.clone()))
                .merge(api_routes)
                .layer(rate_limit)
                .layer(cors)
                .layer(TraceLayer::new_for_http());

            // Spawn background polling tasks
            tokio::spawn(drift_poll_loop(api_state.clone()));
            tokio::spawn(dashboard_poll_loop(api_state));

            let port = std::env::var("YARD_PORT").unwrap_or_else(|_| "3001".to_string());
            let addr: std::net::SocketAddr = format!("0.0.0.0:{port}")
                .parse()
                .map_err(|e| anyhow::anyhow!("Invalid listen address (YARD_PORT={port}): {e}"))?;
            eprintln!("API server listening on {addr}");
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to bind to {addr}: {e}"))?;
            // ConnectInfo<SocketAddr> is required by the auth middleware for the
            // loopback-bypass enforcement (gap-closure plan 03). into_make_service_with_connect_info
            // is the axum primitive that surfaces the peer SocketAddr to extractors.
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Server error: {e}"))?;

            Ok::<(), anyhow::Error>(())
        })
        .expect("API server failed");
    });
}

#[cfg(not(target_arch = "wasm32"))]
async fn drift_poll_loop(state: std::sync::Arc<api::dashboard::ApiState>) {
    use tracing::{info, warn};

    const DEFAULT_INTERVAL_MINS: u64 = 3;

    // Wait for server to be ready before first check
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    loop {
        // Read interval from settings (stored as minutes string)
        let interval_mins = match state.db.get_setting("drift_interval").await {
            Ok(Some(val)) => val.parse::<u64>().unwrap_or(DEFAULT_INTERVAL_MINS),
            _ => DEFAULT_INTERVAL_MINS,
        };

        info!(
            interval_mins = interval_mins,
            "Running scheduled drift check"
        );

        match api::drift::run_drift_check(&state).await {
            Ok(data) => {
                info!(
                    drifted = data.drifted,
                    in_sync = data.in_sync,
                    "Scheduled drift check complete"
                );
                let _ = state.event_tx.send(api::events::Event::DriftRefreshed);

                // ---- Phase 8: drift threshold alerting ----
                // Disabled-by-default short-circuit (D-07): check cheap settings
                // before reading cooldown state.
                //
                // Read all five alert-related settings in a single snapshot so
                // an operator flipping `slack_enabled` to false mid-tick can't
                // race the per-key reads and trigger an alert after Disable.
                let settings: std::collections::HashMap<String, String> = state
                    .db
                    .list_settings()
                    .await
                    .ok()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|s| (s.key, s.value))
                    .collect();

                let slack_enabled = settings
                    .get("slack_enabled")
                    .map(|v| v == "true")
                    .unwrap_or(false);
                let arn = settings
                    .get("slack_webhook_secret_arn")
                    .cloned()
                    .unwrap_or_default();
                let threshold_opt = settings
                    .get("alert_drift_threshold")
                    .and_then(|s| s.parse::<u32>().ok());

                if slack_enabled
                    && !arn.is_empty()
                    && let Some(threshold) = threshold_opt
                {
                    let cooldown_mins = settings
                        .get("alert_cooldown_minutes")
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(10);
                    // Saturating multiply prevents overflow for attacker-set u64::MAX (T-08-03-01).
                    let cooldown =
                        std::time::Duration::from_secs(cooldown_mins.saturating_mul(60));

                    let last_sent = settings
                        .get("alert_last_sent_at")
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.with_timezone(&chrono::Utc));

                    let cfg = alerting::threshold::AlertConfig {
                        threshold,
                        cooldown,
                        last_sent,
                    };
                    let now = chrono::Utc::now();
                    match alerting::threshold::evaluate(&data, &cfg, now) {
                        alerting::threshold::AlertDecision::Send => {
                            // SRV-02 / D-25: resolve the secret only when we're
                            // about to send. On resolve failure log + skip; do
                            // NOT log the resolved URL anywhere (D-17 / T-25-07).
                            //
                            // WR-06: cap the resolve at 5 seconds. The AWS SDK's
                            // default standard retry mode allows up to ~3 attempts
                            // with retries — a transient SecretsManager outage
                            // can stall a single resolve for ~30s. While stalled,
                            // the entire drift_poll_loop is blocked: no new drift
                            // checks fire, no other Slack alerts are sent. A
                            // 5-second hard ceiling bounds the pipeline-stall
                            // surface and is enough headroom for a healthy AWS
                            // region. Uses the already-pulled-in tokio::time —
                            // no new dep.
                            let webhook_url = match tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                state.secret_store.resolve(&arn),
                            )
                            .await
                            {
                                Ok(Ok(url)) => url,
                                Ok(Err(e)) => {
                                    warn!(
                                        arn = %arn,
                                        error = %e,
                                        "Failed to resolve Slack webhook secret; skipping alert"
                                    );
                                    continue;
                                }
                                Err(_) => {
                                    warn!(
                                        arn = %arn,
                                        timeout_secs = 5,
                                        "Slack webhook secret resolve timed out; skipping alert"
                                    );
                                    continue;
                                }
                            };
                            match alerting::slack::post_slack_alert(
                                &webhook_url,
                                &data,
                                threshold,
                            )
                            .await
                            {
                                Ok(()) => {
                                    let ts = now.to_rfc3339();
                                    match state
                                        .db
                                        .set_setting("alert_last_sent_at", &ts)
                                        .await
                                    {
                                        Ok(()) => {
                                            info!(
                                                drifted = data.drifted,
                                                threshold = threshold,
                                                "Drift alert sent"
                                            );
                                            let _ = state.event_tx.send(
                                                api::events::Event::AlertSent {
                                                    drifted_count: data.drifted,
                                                },
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                error = %e,
                                                "Failed to persist alert_last_sent_at after successful Slack POST"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    // SRV-02 / D-17 / T-25-07: do NOT use the
                                    // reqwest::Error Display impl directly —
                                    // it embeds the request URL (the resolved
                                    // Slack webhook secret) for connect /
                                    // timeout / error_for_status variants.
                                    // Log structured fields that never include
                                    // the URL.
                                    let kind = if e.is_timeout() {
                                        "timeout"
                                    } else if e.is_connect() {
                                        "connect"
                                    } else if e.is_request() {
                                        "request"
                                    } else if e.is_body() {
                                        "body"
                                    } else if e.is_decode() {
                                        "decode"
                                    } else {
                                        "other"
                                    };
                                    warn!(
                                        kind = kind,
                                        status = e.status().map(|s| s.as_u16()),
                                        "Slack alert POST failed"
                                    );
                                }
                            }
                        }
                        alerting::threshold::AlertDecision::Cooldown => {
                            info!("Drift alert skipped (cooldown)");
                        }
                        alerting::threshold::AlertDecision::BelowThreshold => {}
                    }
                }
                // ---- End Phase 8 alert block ----
            }
            Err(e) => {
                warn!("Scheduled drift check failed: {e}");
                let _ = state.event_tx.send(api::events::Event::DriftFailed {
                    reason: api::events::sanitize_reason(&e),
                });
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(interval_mins * 60)).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn dashboard_poll_loop(state: std::sync::Arc<api::dashboard::ApiState>) {
    use tracing::{info, warn};

    const DEFAULT_INTERVAL_MINS: u64 = 5;

    // Wait for server to be ready before first refresh
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    loop {
        info!("Refreshing dashboard cache");

        match api::dashboard::refresh_dashboard_cache(&state).await {
            Ok(cache) => {
                info!(
                    total_prs = cache.prs.len(),
                    open_prs = cache.open_prs,
                    "Dashboard cache refreshed"
                );
                let _ = state.event_tx.send(api::events::Event::DashboardRefreshed);
            }
            Err(e) => {
                warn!("Dashboard cache refresh failed: {e}");
                let _ = state.event_tx.send(api::events::Event::DashboardFailed {
                    reason: api::events::sanitize_reason(&e),
                });
            }
        }

        let interval_mins = match state.db.get_setting("dashboard_interval").await {
            Ok(Some(val)) => val.parse::<u64>().unwrap_or(DEFAULT_INTERVAL_MINS),
            _ => DEFAULT_INTERVAL_MINS,
        };

        tokio::time::sleep(std::time::Duration::from_secs(interval_mins * 60)).await;
    }
}

fn app() -> Element {
    let theme = use_signal(|| ui::settings::Theme::Light);
    use_context_provider(|| theme);

    rsx! {
        document::Stylesheet { href: TAILWIND_CSS }
        Router::<Route> {}
    }
}

#[component]
fn Shell() -> Element {
    let sidebar_open = use_signal(|| true);
    let route: Route = use_route();
    let theme: Signal<ui::settings::Theme> = use_context();

    // Plan 25-05 Gap A: /login renders bare (no sidebar/topbar). Short-
    // circuit BEFORE we set up ConnectionCtx / spawn the WS task so the
    // login page makes no API calls and has no auth dependencies.
    if matches!(route, Route::Login {}) {
        return rsx! { Outlet::<Route> {} };
    }

    let is_dark = matches!(theme(), ui::settings::Theme::Dark);

    let title = match &route {
        Route::Dashboard {} => "Dashboard",
        Route::Jobs {} => "Jobs",
        Route::Drift {} => "Drift",
        Route::Settings {} => "Settings",
        // Unreachable: short-circuited above. Kept exhaustive so adding a
        // future Route::* arm doesn't silently break the title bar.
        Route::Login {} => "",
    };

    // Phase 7: provide ConnectionCtx + spawn the WS task (wasm32 only).
    // On native, ConnectionIndicator falls back to Connecting (Plan 04).
    #[cfg(target_arch = "wasm32")]
    {
        let ctx = ui::connection::ConnectionCtx {
            state: use_signal(|| ui::connection::ConnectionState::Connecting),
            dashboard_tick: use_signal(|| 0u64),
            drift_tick: use_signal(|| 0u64),
        };
        use_context_provider(|| ctx);

        // Spawn the WS task exactly once per mount. `use_hook` runs its closure
        // only on first render; the captured Signal handles are stable across
        // re-renders, so no duplicate connections are created.
        use_hook(|| {
            let dashboard_tick = ctx.dashboard_tick;
            let drift_tick = ctx.drift_tick;
            ui::connection::spawn_ws_task(ctx.state, move |event| {
                use ui::connection::Event::*;
                // `write_unchecked(&self)` permits updates from an `Fn` closure;
                // Signals use interior mutability, borrow-checked at runtime.
                match event {
                    DashboardRefreshed
                    | DashboardFailed { .. }
                    | WebhookReceived => {
                        let mut w = dashboard_tick.write_unchecked();
                        *w = w.wrapping_add(1);
                    }
                    DriftRefreshed | DriftFailed { .. } | AlertSent { .. } => {
                        let mut w = drift_tick.write_unchecked();
                        *w = w.wrapping_add(1);
                    }
                }
            });
        });
    }

    rsx! {
        div {
            class: format!(
                "min-h-screen flex bg-white text-zinc-950 dark:bg-zinc-950 dark:text-zinc-50 {}",
                if is_dark { "dark" } else { "" }
            ),
            ui::sidebar::Sidebar { open: sidebar_open, route }
            main { class: "flex-1 min-h-screen",
                div { class: "h-14 flex items-center justify-between px-6 border-b border-zinc-200 dark:border-zinc-800",
                    h1 { class: "text-sm font-semibold", "{title}" }
                    ui::connection_indicator::ConnectionIndicator {}
                }
                Outlet::<Route> {}
            }
        }
    }
}

#[component]
fn Dashboard() -> Element {
    rsx! { ui::dashboard::Dashboard {} }
}

#[component]
fn Jobs() -> Element {
    rsx! { ui::jobs::Jobs {} }
}

#[component]
fn Drift() -> Element {
    rsx! { ui::drift::Drift {} }
}

#[component]
fn Settings() -> Element {
    let theme: Signal<ui::settings::Theme> = use_context();
    rsx! { ui::settings::Settings { theme } }
}

#[component]
fn Login() -> Element {
    rsx! { ui::login::Login {} }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_env_missing_var_returns_error() {
        let result = required_env("YARD_TEST_NONEXISTENT_VAR_12345");
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("must be set"),
            "expected 'must be set' in: {msg}"
        );
    }
}
