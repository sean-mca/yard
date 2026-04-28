use dioxus::prelude::*;
use dioxus_query::prelude::*;
use std::time::Duration;

use super::components::Pagination;
use super::fetch::{get_json, get_json_or_default};
use super::metrics::{DriftStatus, MetricsBar};
use crate::types::*;

use super::api_base;

#[cfg(target_arch = "wasm32")]
use super::connection::{ConnectionCtx, ConnectionState};

// ---- Query types ----

#[derive(Clone, PartialEq, Hash, Eq)]
struct DashboardQuery;

impl QueryCapability for DashboardQuery {
    type Ok = DashboardData;
    type Err = String;
    type Keys = u32; // page number

    async fn run(&self, page: &Self::Keys) -> Result<Self::Ok, Self::Err> {
        // 401-redirect, status, and parse-error handling are centralised in
        // ui::fetch::get_json (Plan 25-05 Gap A). On 401 the helper pushes
        // Route::Login {} and returns Err.
        get_json::<DashboardData>(&format!(
            "{}/api/dashboard/cached?page={page}&per_page=15",
            api_base()
        ))
        .await
    }
}

#[derive(Clone, PartialEq, Hash, Eq)]
struct DriftSummaryQuery;

#[derive(serde::Deserialize, Default)]
struct DriftSummaryResponse {
    drifted: u32,
}

impl QueryCapability for DriftSummaryQuery {
    type Ok = u32;
    type Err = String;
    type Keys = ();

    async fn run(&self, _: &Self::Keys) -> Result<Self::Ok, Self::Err> {
        // ui::fetch::get_json_or_default preserves the historical "swallow
        // non-success as 0" semantics for the metrics-bar header while still
        // routing 401 through the redirect path (Plan 25-05 Gap A).
        let data = get_json_or_default::<DriftSummaryResponse>(&format!(
            "{}/api/drift/summary",
            api_base()
        ))
        .await?;
        Ok(data.drifted)
    }
}

// ---- Component ----

#[component]
pub fn Dashboard() -> Element {
    let mut page = use_signal(|| 1u32);

    // Phase 7: compute polling interval based on WS connection state.
    // When Live, pause polling (Duration::MAX); otherwise, 15s.
    // Per RESEARCH.md: Query::interval_time is NOT hashed, so swapping it
    // does not drop cached data.
    #[cfg(target_arch = "wasm32")]
    let ctx: ConnectionCtx = use_context();
    #[cfg(target_arch = "wasm32")]
    let interval = if matches!(*ctx.state.read(), ConnectionState::Live) {
        Duration::MAX
    } else {
        Duration::from_secs(15)
    };
    #[cfg(not(target_arch = "wasm32"))]
    let interval = Duration::from_secs(15);

    let data = use_query(
        Query::new(page(), DashboardQuery)
            .stale_time(Duration::from_secs(30))
            .interval_time(interval),
    );

    let drift_data = use_query(
        Query::new((), DriftSummaryQuery)
            .stale_time(Duration::from_secs(30))
            .interval_time(interval),
    );

    // Phase 7: invalidate queries on WS push events.
    // dashboard_tick drives dashboard data refresh; drift_tick drives drift summary.
    #[cfg(target_arch = "wasm32")]
    {
        let data_handle = data;
        use_effect(move || {
            let _ = ctx.dashboard_tick.read();
            data_handle.invalidate();
        });
        let drift_handle = drift_data;
        use_effect(move || {
            let _ = ctx.drift_tick.read();
            drift_handle.invalidate();
        });
    }

    let drift_state = drift_data.read();
    let drift_status = match &*drift_state.state() {
        QueryStateData::Settled { res: Ok(0), .. } => DriftStatus::Ok,
        QueryStateData::Settled { res: Ok(n), .. } => DriftStatus::Drifted(*n),
        _ => DriftStatus::Ok,
    };

    let data_state = data.read();
    match &*data_state.state() {
        QueryStateData::Settled { res: Ok(dashboard), .. } => rsx! {
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
        QueryStateData::Settled { res: Err(e), .. } => rsx! {
            div { class: "p-6",
                div { class: "rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700",
                    "Failed to load dashboard: {e}"
                }
            }
        },
        _ => rsx! {
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
