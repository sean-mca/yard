use dioxus::prelude::*;

#[cfg(feature = "server")]
mod github;

const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

fn main() {
    #[cfg(feature = "server")]
    {
        use dioxus::server::{DioxusRouterExt, ServeConfig};
        use github::{client::GitHubClient, router::github_router, router::AppState};
        use std::sync::Arc;

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let github_token =
                    std::env::var("YARD_GITHUB_TOKEN").unwrap_or_default();
                let webhook_secret =
                    std::env::var("YARD_WEBHOOK_SECRET").unwrap_or_default();

                let github_client = GitHubClient::new(&github_token)
                    .expect("Failed to create GitHub client");

                let state = Arc::new(AppState {
                    github_client,
                    webhook_secret,
                });

                let addr = dioxus::cli_config::fullstack_address_or_localhost();
                let router = axum::Router::new()
                    .merge(github_router(state))
                    .serve_dioxus_application(ServeConfig::new(), app);

                let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
                axum::serve(listener, router).await.unwrap();
            });
    }

    #[cfg(not(feature = "server"))]
    dioxus::launch(app);
}

fn app() -> Element {
    let mut sidebar_open = use_signal(|| true);

    rsx! {
        document::Stylesheet { href: TAILWIND_CSS }
        div { class: "min-h-screen bg-white text-zinc-950 flex",
            // Sidebar
            aside {
                class: format!(
                    "h-screen sticky top-0 flex flex-col border-r border-zinc-200 bg-zinc-50/75 transition-all duration-200 {}",
                    if sidebar_open() { "w-64" } else { "w-14" }
                ),
                // Sidebar header
                div { class: "flex items-center h-14 px-3 border-b border-zinc-200",
                    if sidebar_open() {
                        span { class: "text-sm font-semibold tracking-tight pl-1", "yard" }
                    }
                    button {
                        class: "ml-auto p-1.5 rounded-md text-zinc-500 hover:text-zinc-950 hover:bg-zinc-200/50 transition-colors",
                        onclick: move |_| sidebar_open.toggle(),
                        if sidebar_open() {
                            // Collapse icon (chevron left)
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                width: "16", height: "16",
                                view_box: "0 0 24 24",
                                fill: "none",
                                stroke: "currentColor",
                                stroke_width: "2",
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                path { d: "m15 18-6-6 6-6" }
                            }
                        } else {
                            // Expand icon (chevron right)
                            svg {
                                xmlns: "http://www.w3.org/2000/svg",
                                width: "16", height: "16",
                                view_box: "0 0 24 24",
                                fill: "none",
                                stroke: "currentColor",
                                stroke_width: "2",
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                path { d: "m9 18 6-6-6-6" }
                            }
                        }
                    }
                }
                // Navigation
                nav { class: "flex-1 flex flex-col gap-1 p-2",
                    SidebarItem { icon: "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1", label: "Dashboard", expanded: sidebar_open(), active: true }
                    SidebarItem { icon: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2", label: "Jobs", expanded: sidebar_open(), active: false }
                    SidebarItem { icon: "M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15", label: "Drift", expanded: sidebar_open(), active: false }
                    SidebarItem { icon: "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z", label: "Settings", expanded: sidebar_open(), active: false }
                }
            }
            // Main content
            main { class: "flex-1 min-h-screen",
                // Top bar
                div { class: "h-14 flex items-center px-6 border-b border-zinc-200",
                    h1 { class: "text-sm font-semibold", "Dashboard" }
                }
                // Page content
                div { class: "p-6",
                    p { class: "text-sm text-zinc-500", "Select a view from the sidebar." }
                }
            }
        }
    }
}

#[component]
fn SidebarItem(icon: &'static str, label: &'static str, expanded: bool, active: bool) -> Element {
    rsx! {
        div { class: "relative group",
            button {
                class: format!(
                    "flex items-center gap-3 rounded-md text-sm cursor-pointer transition-colors {} {}",
                    if expanded { "px-2.5 py-1.5" } else { "justify-center p-2" },
                    if active {
                        "bg-zinc-200/75 text-zinc-950 font-medium"
                    } else {
                        "text-zinc-500 hover:text-zinc-950 hover:bg-zinc-100"
                    }
                ),
                svg {
                    xmlns: "http://www.w3.org/2000/svg",
                    width: "16", height: "16",
                    view_box: "0 0 24 24",
                    fill: "none",
                    stroke: "currentColor",
                    stroke_width: "2",
                    stroke_linecap: "round",
                    stroke_linejoin: "round",
                    class: "shrink-0",
                    path { d: "{icon}" }
                }
                if expanded {
                    span { "{label}" }
                }
            }
            if !expanded {
                div {
                    class: "absolute left-full top-1/2 -translate-y-1/2 ml-2 px-2 py-1 rounded-md bg-zinc-950 text-zinc-50 text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-50",
                    "{label}"
                }
            }
        }
    }
}
