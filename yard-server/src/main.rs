use dioxus::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
mod db;
#[cfg(not(target_arch = "wasm32"))]
mod github;
mod types;
mod ui;
#[cfg(not(target_arch = "wasm32"))]
mod yard_runner;

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

#[cfg(not(target_arch = "wasm32"))]
fn start_api_server() {
    use api::dashboard::{dashboard_router, ApiState};
    use api::drift::drift_router;
    use api::jobs::jobs_router;
    use api::settings::settings_router;
    use db::DbConfig;
    use github::{client::GitHubClient, router::github_router, router::AppState};
    use std::sync::Arc;
    use tower_http::cors::{Any, CorsLayer};

    // Install rustls crypto provider before any TLS clients are created
    let _ = rustls::crypto::ring::default_provider().install_default();

    std::thread::spawn(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let github_token = std::env::var("YARD_GITHUB_TOKEN").unwrap_or_default();
                let webhook_secret = std::env::var("YARD_WEBHOOK_SECRET").unwrap_or_default();
                let repo_owner = std::env::var("YARD_REPO_OWNER")
                    .expect("YARD_REPO_OWNER must be set");
                let repo_name = std::env::var("YARD_REPO_NAME")
                    .expect("YARD_REPO_NAME must be set");

                // Initialize DynamoDB persistence
                let db_config = DbConfig::from_env();
                let db = db::connect(&db_config)
                    .await
                    .expect("Failed to connect to DynamoDB");
                db.migrate()
                    .await
                    .expect("Failed to run DynamoDB migrations");

                let github_client =
                    GitHubClient::new(&github_token).expect("Failed to create GitHub client");

                let webhook_state = Arc::new(AppState {
                    github_client,
                    webhook_secret,
                    db: db.clone(),
                });

                let api_state = Arc::new(ApiState {
                    github_token,
                    repo_owner,
                    repo_name,
                    db,
                });

                let cors = CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any);

                let router = axum::Router::new()
                    .merge(github_router(webhook_state))
                    .merge(dashboard_router(api_state.clone()))
                    .merge(jobs_router(api_state.clone()))
                    .merge(drift_router(api_state.clone()))
                    .merge(settings_router(api_state.clone()))
                    .layer(cors);

                // Spawn background drift polling task
                tokio::spawn(drift_poll_loop(api_state));

                let addr: std::net::SocketAddr = "0.0.0.0:3001".parse().unwrap();
                eprintln!("API server listening on {addr}");
                let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
                axum::serve(listener, router).await.unwrap();
            });
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

        info!(interval_mins = interval_mins, "Running scheduled drift check");

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
