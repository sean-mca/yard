//! Global search typeahead component.
//!
//! Renders a search input in the Shell top bar with a debounced API call
//! to `/api/search?q=...`. Results appear in an absolutely-positioned
//! dropdown grouped by entity type (Environments, Jobs, DAGs) with max
//! 5 results per group.
//!
//! Debounce uses `gloo_timers::future::sleep` on WASM; on native the
//! component is a static placeholder (search requires a browser).

use dioxus::prelude::*;

#[cfg(target_arch = "wasm32")]
use super::api_base;
#[cfg(target_arch = "wasm32")]
use super::fetch::get_json;
#[cfg(target_arch = "wasm32")]
use super::percent_encode;

/// Search result from the `/api/search?q=...` API endpoint.
/// Mirrors `crate::types::SearchResult` (added by Plan 44-01).
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
pub struct SearchResult {
    pub environments: Vec<SearchHit>,
    pub jobs: Vec<SearchHit>,
    pub dags: Vec<SearchHit>,
}

/// A single search hit across any entity type.
#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct SearchHit {
    pub name: String,
    pub environment: Option<String>,
    pub region: Option<String>,
    #[allow(dead_code)]
    pub entity_type: String,
}

/// Maximum results shown per group in the dropdown.
const MAX_PER_GROUP: usize = 5;

/// Debounce delay in milliseconds before triggering a search API call.
/// Used only in the wasm32 build (inside the debounced spawn block).
#[allow(dead_code)]
const DEBOUNCE_MS: u32 = 250;

#[component]
pub fn GlobalSearch() -> Element {
    let mut query = use_signal(String::new);
    let mut results = use_signal(|| None::<Result<SearchResult, String>>);
    let mut is_open = use_signal(|| false);
    let is_loading = use_signal(|| false);
    let mut debounce_version = use_signal(|| 0u32);

    rsx! {
        div { class: "relative",
            // Magnifying glass icon
            svg {
                xmlns: "http://www.w3.org/2000/svg",
                width: "14", height: "14",
                view_box: "0 0 24 24",
                fill: "none",
                stroke: "currentColor",
                stroke_width: "2",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                class: "absolute left-2.5 top-1/2 -translate-y-1/2 text-zinc-400",
                circle { cx: "11", cy: "11", r: "8" }
                line { x1: "21", y1: "21", x2: "16.65", y2: "16.65" }
            }
            input {
                r#type: "text",
                placeholder: "Search environments, jobs, DAGs...",
                class: "pl-8 pr-3 py-2 text-sm rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 dark:text-zinc-50 focus:outline-none focus:ring-2 focus:ring-zinc-300 dark:focus:ring-zinc-600 w-64",
                value: "{query}",
                oninput: move |e: Event<FormData>| {
                    let value = e.value();
                    query.set(value.clone());

                    if value.is_empty() {
                        is_open.set(false);
                        results.set(None);
                        return;
                    }

                    is_open.set(true);
                    debounce_version.set(debounce_version().wrapping_add(1));

                    // Debounced search: gloo_timers is WASM-only.
                    #[cfg(target_arch = "wasm32")]
                    let version = debounce_version();
                    #[cfg(target_arch = "wasm32")]
                    {
                        spawn(async move {
                            gloo_timers::future::sleep(
                                std::time::Duration::from_millis(DEBOUNCE_MS as u64),
                            )
                            .await;

                            // If a newer keystroke fired, this version is stale.
                            if debounce_version() != version {
                                return;
                            }

                            is_loading.set(true);
                            let encoded_query = percent_encode(&query.read());
                            let url = format!(
                                "{}/api/search?q={}",
                                api_base(),
                                encoded_query
                            );
                            let result = get_json::<SearchResult>(&url).await;
                            is_loading.set(false);
                            results.set(Some(result));
                        });
                    }
                },
                onkeydown: move |e: Event<KeyboardData>| {
                    if e.key() == Key::Escape {
                        is_open.set(false);
                    }
                },
                onfocusout: move |_| {
                    // Small delay to allow click events on results to register
                    // before closing the dropdown.
                    #[cfg(target_arch = "wasm32")]
                    {
                        spawn(async move {
                            gloo_timers::future::sleep(
                                std::time::Duration::from_millis(200),
                            )
                            .await;
                            is_open.set(false);
                        });
                    }
                },
            }

            // Typeahead dropdown
            if is_open() && !query.read().is_empty() {
                div {
                    class: "absolute top-full mt-1 w-96 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 shadow-lg z-50 max-h-80 overflow-y-auto",
                    {
                        if is_loading() {
                            rsx! {
                                div { class: "p-3 space-y-2",
                                    super::skeleton::SkeletonText { width: "w-3/4".to_string() }
                                    super::skeleton::SkeletonText { width: "w-1/2".to_string() }
                                    super::skeleton::SkeletonText { width: "w-2/3".to_string() }
                                }
                            }
                        } else {
                            match &*results.read() {
                                Some(Ok(sr)) => {
                                    let has_any = !sr.environments.is_empty()
                                        || !sr.jobs.is_empty()
                                        || !sr.dags.is_empty();
                                    if has_any {
                                        rsx! {
                                            if !sr.environments.is_empty() {
                                                ResultGroup {
                                                    label: "Environments",
                                                    hits: sr.environments.iter().take(MAX_PER_GROUP).cloned().collect(),
                                                    on_select: move |_| is_open.set(false),
                                                }
                                            }
                                            if !sr.jobs.is_empty() {
                                                ResultGroup {
                                                    label: "Jobs",
                                                    hits: sr.jobs.iter().take(MAX_PER_GROUP).cloned().collect(),
                                                    on_select: move |_| is_open.set(false),
                                                }
                                            }
                                            if !sr.dags.is_empty() {
                                                ResultGroup {
                                                    label: "DAGs",
                                                    hits: sr.dags.iter().take(MAX_PER_GROUP).cloned().collect(),
                                                    on_select: move |_| is_open.set(false),
                                                }
                                            }
                                        }
                                    } else {
                                        rsx! {
                                            p {
                                                class: "text-sm text-zinc-500 p-4 text-center",
                                                "No results for '{query}'"
                                            }
                                        }
                                    }
                                }
                                Some(Err(_)) => {
                                    rsx! {
                                        p {
                                            class: "text-sm text-zinc-500 p-4 text-center",
                                            "Search unavailable -- try again"
                                        }
                                    }
                                }
                                None => rsx! {}
                            }
                        }
                    }
                }
            }
        }
    }
}

/// A group of search results under a section header (Environments, Jobs, or DAGs).
#[component]
fn ResultGroup(label: &'static str, hits: Vec<SearchHit>, on_select: EventHandler) -> Element {
    rsx! {
        div {
            p {
                class: "text-xs font-semibold text-zinc-400 uppercase tracking-wide px-3 py-2",
                "{label}"
            }
            for hit in hits.iter() {
                {
                    let name = hit.name.clone();
                    let env = hit.environment.clone();
                    let region = hit.region.clone();
                    let secondary = build_secondary(&env, &region);
                    let href = build_href(label, &name, &env, &region);
                    rsx! {
                        Link {
                            to: "{href}",
                            class: "block px-3 py-2 hover:bg-zinc-50 dark:hover:bg-zinc-800 cursor-pointer",
                            onclick: move |_| on_select.call(()),
                            p { class: "text-sm font-semibold text-zinc-950 dark:text-zinc-50", "{name}" }
                            if !secondary.is_empty() {
                                p { class: "text-xs text-zinc-500", "{secondary}" }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Build secondary display text from optional environment and region.
fn build_secondary(env: &Option<String>, region: &Option<String>) -> String {
    match (env, region) {
        (Some(e), Some(r)) => format!("{e} / {r}"),
        (Some(e), None) => e.clone(),
        (None, Some(r)) => r.clone(),
        (None, None) => String::new(),
    }
}

/// Build the navigation href for a search result.
///
/// Environment results navigate to `/envs/{name}`.
/// Job and DAG results navigate to `/envs/{env}/{region}` when both are
/// available, falling back to `/envs/{env}` or `/jobs`.
fn build_href(
    group: &str,
    name: &str,
    env: &Option<String>,
    region: &Option<String>,
) -> String {
    match group {
        "Environments" => format!("/envs/{name}"),
        "Jobs" | "DAGs" => match (env, region) {
            (Some(e), Some(r)) => format!("/envs/{e}/{r}"),
            (Some(e), None) => format!("/envs/{e}"),
            _ => "/jobs".to_string(),
        },
        _ => "/".to_string(),
    }
}
