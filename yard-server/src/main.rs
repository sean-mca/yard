use dioxus::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
mod db;
#[cfg(not(target_arch = "wasm32"))]
mod github;
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
    use api::dashboard::{ApiState, dashboard_router};
    use api::drift::drift_router;
    use api::jobs::jobs_router;
    use api::settings::settings_router;
    use db::DbConfig;
    use github::{client::{GitHubApi, GitHubClient}, router::AppState, router::github_router};
    use std::sync::Arc;
    use tower_governor::GovernorLayer;
    use tower_governor::governor::GovernorConfigBuilder;
    use tower_http::cors::{Any, CorsLayer};
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

            let github_client: std::sync::Arc<dyn GitHubApi> = std::sync::Arc::new(
                GitHubClient::new(&github_token)
                    .map_err(|e| anyhow::anyhow!("Failed to create GitHub client: {e}"))?,
            );

            let api_state = Arc::new(ApiState {
                github_token,
                repo_owner,
                repo_name,
                db: db.clone(),
            });

            let webhook_state = Arc::new(AppState {
                github_client,
                webhook_secret,
                db,
                api_state: api_state.clone(),
            });

            let cors = CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any);

            let rate_limit_config = GovernorConfigBuilder::default()
                .per_second(30)
                .burst_size(60)
                .finish()
                .expect("Failed to build rate limiter config");
            let rate_limit = GovernorLayer::new(Arc::new(rate_limit_config));

            let router = axum::Router::new()
                .merge(github_router(webhook_state))
                .merge(dashboard_router(api_state.clone()))
                .merge(jobs_router(api_state.clone()))
                .merge(drift_router(api_state.clone()))
                .merge(settings_router(api_state.clone()))
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
            }
            Err(e) => {
                warn!("Scheduled drift check failed: {e}");
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
            }
            Err(e) => {
                warn!("Dashboard cache refresh failed: {e}");
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

    let is_dark = matches!(theme(), ui::settings::Theme::Dark);

    let title = match &route {
        Route::Dashboard {} => "Dashboard",
        Route::Jobs {} => "Jobs",
        Route::Drift {} => "Drift",
        Route::Settings {} => "Settings",
    };

    rsx! {
        div {
            class: format!(
                "min-h-screen flex bg-white text-zinc-950 dark:bg-zinc-950 dark:text-zinc-50 {}",
                if is_dark { "dark" } else { "" }
            ),
            ui::sidebar::Sidebar { open: sidebar_open, route }
            main { class: "flex-1 min-h-screen",
                div { class: "h-14 flex items-center px-6 border-b border-zinc-200 dark:border-zinc-800",
                    h1 { class: "text-sm font-semibold", "{title}" }
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
