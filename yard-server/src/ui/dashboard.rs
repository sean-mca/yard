use dioxus::prelude::*;

use super::components::Pagination;
use super::metrics::{DriftStatus, MetricsBar};
use crate::types::*;

const API_BASE: &str = "http://127.0.0.1:3001";
const DRIFT_POLL_MS: u32 = 15_000;

#[cfg(target_arch = "wasm32")]
fn set_interval(ms: u32, mut f: impl FnMut() + 'static) {
    use wasm_bindgen::prelude::*;
    let closure = Closure::wrap(Box::new(move || f()) as Box<dyn FnMut()>);
    web_sys::window()
        .unwrap()
        .set_interval_with_callback_and_timeout_and_arguments_0(
            closure.as_ref().unchecked_ref(),
            ms as i32,
        )
        .unwrap();
    closure.forget();
}

#[cfg(not(target_arch = "wasm32"))]
fn set_interval(_ms: u32, _f: impl FnMut() + 'static) {}

#[derive(serde::Deserialize)]
struct DriftSummaryResponse {
    drifted: u32,
}

async fn fetch_drift_summary() -> Result<u32, String> {
    let resp = reqwest::get(format!("{API_BASE}/api/drift/summary"))
        .await
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        return Ok(0);
    }

    let data = resp
        .json::<DriftSummaryResponse>()
        .await
        .map_err(|e| format!("Parse failed: {e}"))?;

    Ok(data.drifted)
}

async fn fetch_dashboard_data(page: u32) -> Result<DashboardData, String> {
    let resp = reqwest::get(format!("{API_BASE}/api/dashboard?page={page}&per_page=15"))
        .await
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Server error ({status}): {body}"));
    }

    resp.json::<DashboardData>()
        .await
        .map_err(|e| format!("Failed to parse response: {e}"))
}

#[component]
pub fn Dashboard() -> Element {
    let mut page = use_signal(|| 1u32);
    let data = use_resource(move || fetch_dashboard_data(page()));
    let mut drift_tick = use_signal(|| 0u32);
    use_effect(move || {
        set_interval(DRIFT_POLL_MS, move || {
            if let Ok(mut val) = drift_tick.try_write() {
                *val += 1;
            }
        });
    });

    let drift_data = use_resource(move || {
        let _ = drift_tick();
        fetch_drift_summary()
    });

    let drift_status = match &*drift_data.read() {
        Some(Ok(0)) => DriftStatus::Ok,
        Some(Ok(n)) => DriftStatus::Drifted(*n),
        _ => DriftStatus::Ok,
    };

    match &*data.read() {
        Some(Ok(dashboard)) => rsx! {
            div { class: "p-6",
                MetricsBar {
                    open_prs: dashboard.open_prs,
                    plans_running: 0,
                    drift: drift_status,
                    jobs_tracked: dashboard.jobs_tracked,
                }
                PrTable { rows: dashboard.prs.clone() }
                Pagination {
                    page: dashboard.page,
                    has_more: dashboard.has_more,
                    on_prev: move |_| {
                        if page() > 1 {
                            page -= 1;
                        }
                    },
                    on_next: move |_| {
                        page += 1;
                    },
                }
            }
        },
        Some(Err(e)) => rsx! {
            div { class: "p-6",
                div { class: "rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700",
                    "Failed to load dashboard: {e}"
                }
            }
        },
        None => rsx! {
            div { class: "p-6",
                div { class: "flex items-center gap-2 text-sm text-zinc-500",
                    svg {
                        xmlns: "http://www.w3.org/2000/svg",
                        width: "16", height: "16",
                        view_box: "0 0 24 24",
                        fill: "none",
                        stroke: "currentColor",
                        stroke_width: "2",
                        class: "animate-spin",
                        path { d: "M21 12a9 9 0 11-6.219-8.56" }
                    }
                    "Loading..."
                }
            }
        },
    }
}

#[component]
fn PrTable(rows: Vec<PrRow>) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 overflow-hidden",
            if rows.is_empty() {
                div { class: "px-4 py-8 text-center text-sm text-zinc-500 dark:text-zinc-400",
                    "No pull requests found."
                }
            } else {
                table { class: "w-full text-sm",
                    thead {
                        tr { class: "border-b border-zinc-200 dark:border-zinc-800 bg-zinc-50/50 dark:bg-zinc-900/50",
                            th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "PR" }
                            th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Title" }
                            th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Author" }
                            th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Status" }
                            th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Plan" }
                            th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Updated" }
                        }
                    }
                    tbody {
                        for row in rows.iter() {
                            PrTableRow { row: row.clone() }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn PrTableRow(row: PrRow) -> Element {
    rsx! {
        tr { class: "border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50/50 dark:hover:bg-zinc-900/50 transition-colors",
            td { class: "px-4 py-3 font-mono",
                a {
                    href: "{row.url}",
                    target: "_blank",
                    class: "text-blue-600 hover:text-blue-800 hover:underline cursor-pointer",
                    "#{row.number}"
                }
            }
            td { class: "px-4 py-3 font-medium", "{row.title}" }
            td { class: "px-4 py-3 text-zinc-500 dark:text-zinc-400", "{row.author}" }
            td { class: "px-4 py-3", StateBadge { state: row.state.clone() } }
            td { class: "px-4 py-3", PlanBadge { result: row.plan_result.clone() } }
            td { class: "px-4 py-3 text-zinc-500 dark:text-zinc-400", "{row.updated}" }
        }
    }
}

#[component]
fn StateBadge(state: PrState) -> Element {
    let (label, classes) = match state {
        PrState::Open => ("Open", "bg-blue-50 text-blue-700 border-blue-200"),
        PrState::Merged => ("Merged", "bg-violet-50 text-violet-700 border-violet-200"),
        PrState::Closed => ("Closed", "bg-zinc-50 text-zinc-500 border-zinc-200"),
    };

    rsx! {
        span { class: format!("inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border {classes}"),
            "{label}"
        }
    }
}

#[component]
fn PlanBadge(result: PlanResult) -> Element {
    let (label, classes) = match result {
        PlanResult::Pass => ("Pass", "bg-emerald-50 text-emerald-700 border-emerald-200"),
        PlanResult::Fail => ("Fail", "bg-red-50 text-red-700 border-red-200"),
        PlanResult::Pending => ("Pending", "bg-amber-50 text-amber-700 border-amber-200"),
        PlanResult::None => ("—", "bg-zinc-50 text-zinc-400 border-zinc-200"),
    };

    rsx! {
        span { class: format!("inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border {classes}"),
            "{label}"
        }
    }
}
