use dioxus::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
mod api;
#[cfg(not(target_arch = "wasm32"))]
mod github;
mod types;
mod ui;

const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

fn main() {
    #[cfg(not(target_arch = "wasm32"))]
    start_api_server();

    dioxus::launch(app);
}

#[cfg(not(target_arch = "wasm32"))]
fn start_api_server() {
    use api::dashboard::{dashboard_router, ApiState};
    use github::{client::GitHubClient, router::github_router, router::AppState};
    use std::sync::Arc;
    use tower_http::cors::{Any, CorsLayer};

    std::thread::spawn(|| {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let github_token = std::env::var("YARD_GITHUB_TOKEN").unwrap_or_default();
                let webhook_secret = std::env::var("YARD_WEBHOOK_SECRET").unwrap_or_default();
                let repo_owner = std::env::var("YARD_REPO_OWNER")
                    .unwrap_or_else(|_| "sean-mca".to_string());
                let repo_name = std::env::var("YARD_REPO_NAME")
                    .unwrap_or_else(|_| "yard-example".to_string());

                let github_client =
                    GitHubClient::new(&github_token).expect("Failed to create GitHub client");

                let webhook_state = Arc::new(AppState {
                    github_client,
                    webhook_secret,
                });

                let api_state = Arc::new(ApiState {
                    github_token,
                    repo_owner,
                    repo_name,
                });

                let cors = CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any);

                let router = axum::Router::new()
                    .merge(github_router(webhook_state))
                    .merge(dashboard_router(api_state))
                    .layer(cors);

                let addr: std::net::SocketAddr = "0.0.0.0:3001".parse().unwrap();
                eprintln!("API server listening on {addr}");
                let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
                axum::serve(listener, router).await.unwrap();
            });
    });
}

fn app() -> Element {
    let sidebar_open = use_signal(|| true);

    rsx! {
        document::Stylesheet { href: TAILWIND_CSS }
        div { class: "min-h-screen bg-white text-zinc-950 flex",
            ui::sidebar::Sidebar { open: sidebar_open }
            main { class: "flex-1 min-h-screen",
                div { class: "h-14 flex items-center px-6 border-b border-zinc-200",
                    h1 { class: "text-sm font-semibold", "Dashboard" }
                }
                ui::dashboard::Dashboard {}
            }
        }
    }
}
