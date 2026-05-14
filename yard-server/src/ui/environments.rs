use dioxus::prelude::*;
use dioxus_query::prelude::*;
use std::time::Duration;

use super::components::{Breadcrumb, TabBar};
use super::drift::{DriftBadge, SummaryCard};
use super::fetch::get_json;
use super::sheet::Sheet;
use super::skeleton::{SkeletonCard, SkeletonTable};
use crate::types::*;

use super::api_base;

#[cfg(target_arch = "wasm32")]
use super::connection::{ConnectionCtx, ConnectionState};

// ---- Query types ----

#[derive(Clone, PartialEq, Hash, Eq)]
struct EnvListQuery;

impl QueryCapability for EnvListQuery {
    type Ok = EnvironmentListData;
    type Err = String;
    type Keys = ();

    async fn run(&self, _: &Self::Keys) -> Result<Self::Ok, Self::Err> {
        get_json::<EnvironmentListData>(&format!("{}/api/envs", api_base())).await
    }
}

#[derive(Clone, PartialEq, Hash, Eq)]
struct RegionListQuery;

impl QueryCapability for RegionListQuery {
    type Ok = Vec<RegionDetailData>;
    type Err = String;
    type Keys = String;

    async fn run(&self, keys: &Self::Keys) -> Result<Self::Ok, Self::Err> {
        get_json::<Vec<RegionDetailData>>(&format!(
            "{}/api/envs/{}/regions",
            api_base(),
            keys
        ))
        .await
    }
}

// ---- EnvironmentList ----

#[component]
pub fn EnvironmentList() -> Element {
    // WS-aware polling interval: pause when Live, 15s fallback.
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
        Query::new((), EnvListQuery)
            .stale_time(Duration::from_secs(30))
            .interval_time(interval),
    );

    // Invalidate on env health tick.
    #[cfg(target_arch = "wasm32")]
    {
        let data_handle = data;
        use_effect(move || {
            let _ = ctx.env_health_tick.read();
            data_handle.invalidate();
        });
    }

    let data_state = data.read();
    match &*data_state.state() {
        QueryStateData::Settled {
            res: Ok(env_data), ..
        } => {
            if env_data.environments.is_empty() {
                rsx! {
                    div { class: "p-6",
                        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-8 text-center",
                            h3 { class: "text-sm font-semibold text-zinc-950 dark:text-zinc-50 mb-2",
                                "No environments discovered"
                            }
                            p { class: "text-sm text-zinc-500 dark:text-zinc-400 max-w-md mx-auto",
                                "The server has not discovered any environments yet. Check that the repository path is configured correctly in your server settings."
                            }
                        }
                    }
                }
            } else {
                rsx! {
                    div { class: "p-6",
                        div { class: "grid grid-cols-3 gap-4 mb-6",
                            SummaryCard {
                                label: "Environments",
                                value: format!("{}", env_data.total_environments),
                            }
                            SummaryCard {
                                label: "Connected Accounts",
                                value: format!("{}/{}", env_data.connected_accounts, env_data.total_accounts),
                                accent: if env_data.connected_accounts == env_data.total_accounts { "emerald" } else { "amber" },
                            }
                        }
                        div { class: "grid grid-cols-3 gap-4",
                            for env in env_data.environments.iter() {
                                {
                                    let env = env.clone();
                                    rsx! {
                                        EnvCard { env }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        QueryStateData::Settled {
            res: Err(e), ..
        } => rsx! {
            div { class: "p-6",
                div { class: "rounded-lg border border-red-200 bg-red-50 px-4 py-8 text-center",
                    p { class: "text-sm text-red-700", "Failed to load environments: {e}" }
                }
            }
        },
        _ => rsx! {
            div { class: "p-6",
                div { class: "grid grid-cols-3 gap-4",
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                }
            }
        },
    }
}

#[component]
fn EnvCard(env: EnvironmentSummary) -> Element {
    let name = env.name.clone();
    let region_count = env.regions.len();
    let region_label = if region_count != 1 {
        format!("{region_count} regions")
    } else {
        "1 region".to_string()
    };
    rsx! {
        Link {
            to: crate::Route::EnvironmentDetail { env: name },
            class: "rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 p-4 cursor-pointer hover:bg-zinc-50/50 dark:hover:bg-zinc-900/50 transition-colors block",
            p { class: "text-sm font-semibold text-zinc-950 dark:text-zinc-50 mb-1", "{env.name}" }
            div { class: "flex items-center gap-2",
                p { class: "text-xs text-zinc-500 dark:text-zinc-400", "{region_label}" }
                if env.drift_count > 0 {
                    DriftBadge { drift_type: DriftType::Modified }
                }
            }
        }
    }
}

// ---- EnvironmentDetail ----

#[component]
pub fn EnvironmentDetail(env: String) -> Element {
    // WS-aware polling interval.
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
        Query::new(env.clone(), RegionListQuery)
            .stale_time(Duration::from_secs(30))
            .interval_time(interval),
    );

    #[cfg(target_arch = "wasm32")]
    {
        let data_handle = data;
        use_effect(move || {
            let _ = ctx.env_health_tick.read();
            data_handle.invalidate();
        });
    }

    let breadcrumb_env = env.clone();

    let data_state = data.read();
    match &*data_state.state() {
        QueryStateData::Settled {
            res: Ok(regions), ..
        } => {
            rsx! {
                div { class: "p-6",
                    Breadcrumb {
                        segments: vec![
                            ("Environments".to_string(), Some("/envs".to_string())),
                            (breadcrumb_env.clone(), None),
                        ],
                    }
                    if regions.is_empty() {
                        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-8 text-center",
                            p { class: "text-sm text-zinc-500 dark:text-zinc-400",
                                "No regions found for this environment."
                            }
                        }
                    } else {
                        div { class: "grid grid-cols-3 gap-4",
                            for region in regions.iter() {
                                {
                                    let region = region.clone();
                                    let env_name = env.clone();
                                    rsx! {
                                        RegionCard { env_name, region }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        QueryStateData::Settled {
            res: Err(e), ..
        } => rsx! {
            div { class: "p-6",
                Breadcrumb {
                    segments: vec![
                        ("Environments".to_string(), Some("/envs".to_string())),
                        (breadcrumb_env.clone(), None),
                    ],
                }
                div { class: "rounded-lg border border-red-200 bg-red-50 px-4 py-8 text-center",
                    p { class: "text-sm text-red-700", "Failed to load environments: {e}" }
                }
            }
        },
        _ => rsx! {
            div { class: "p-6",
                Breadcrumb {
                    segments: vec![
                        ("Environments".to_string(), Some("/envs".to_string())),
                        (breadcrumb_env.clone(), None),
                    ],
                }
                div { class: "grid grid-cols-3 gap-4",
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                    SkeletonCard { width: "w-full".to_string(), height: "h-[76px]".to_string() }
                }
            }
        },
    }
}

#[component]
fn RegionCard(env_name: String, region: RegionDetailData) -> Element {
    let drift_count = region.drift_items.len();
    let region_name = region.region_name.clone();
    let job_count = region.jobs.len();
    let dag_count = region.dags.len();
    let job_label = if job_count != 1 {
        format!("{job_count} jobs")
    } else {
        "1 job".to_string()
    };
    let dag_label = if dag_count != 1 {
        format!("{dag_count} DAGs")
    } else {
        "1 DAG".to_string()
    };
    rsx! {
        Link {
            to: crate::Route::RegionDetail { env: env_name, region: region_name },
            class: "rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 p-4 cursor-pointer hover:bg-zinc-50/50 dark:hover:bg-zinc-900/50 transition-colors block",
            p { class: "text-sm font-semibold text-zinc-950 dark:text-zinc-50 mb-1", "{region.region_name}" }
            div { class: "flex items-center gap-3",
                p { class: "text-xs text-zinc-500 dark:text-zinc-400", "{job_label}" }
                p { class: "text-xs text-zinc-500 dark:text-zinc-400", "{dag_label}" }
                if drift_count > 0 {
                    DriftBadge { drift_type: DriftType::Modified }
                }
            }
        }
    }
}

// ---- RegionDetail ----

#[component]
pub fn RegionDetail(env: String, region: String) -> Element {
    // WS-aware polling interval.
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
        Query::new(env.clone(), RegionListQuery)
            .stale_time(Duration::from_secs(30))
            .interval_time(interval),
    );

    #[cfg(target_arch = "wasm32")]
    {
        let data_handle = data;
        use_effect(move || {
            let _ = ctx.env_health_tick.read();
            data_handle.invalidate();
        });
    }

    let active_tab = use_signal(|| 0usize);
    let selected_job = use_signal(|| None::<crate::db::JobSummaryEntity>);

    let breadcrumb_env = env.clone();
    let breadcrumb_region = region.clone();

    let data_state = data.read();

    // Find the matching region from the list response.
    let region_data = match &*data_state.state() {
        QueryStateData::Settled {
            res: Ok(regions), ..
        } => regions.iter().find(|r| r.region_name == region).cloned(),
        _ => None,
    };

    let is_error = matches!(
        &*data_state.state(),
        QueryStateData::Settled { res: Err(_), .. }
    );
    let error_msg = if let QueryStateData::Settled { res: Err(e), .. } = &*data_state.state() {
        e.clone()
    } else {
        String::new()
    };
    let is_loading = !matches!(
        &*data_state.state(),
        QueryStateData::Settled { .. }
    );

    rsx! {
        div { class: "p-6",
            Breadcrumb {
                segments: vec![
                    ("Environments".to_string(), Some("/envs".to_string())),
                    (breadcrumb_env.clone(), Some(format!("/envs/{}", breadcrumb_env))),
                    (breadcrumb_region.clone(), None),
                ],
            }

            if is_error {
                div { class: "rounded-lg border border-red-200 bg-red-50 px-4 py-8 text-center",
                    p { class: "text-sm text-red-700", "Failed to load environments: {error_msg}" }
                }
            } else if is_loading {
                TabBar { tabs: vec!["Jobs".into(), "DAGs".into()], active: active_tab }
                SkeletonTable { rows: 8, cols: 5 }
            } else if let Some(rd) = &region_data {
                TabBar { tabs: vec!["Jobs".into(), "DAGs".into()], active: active_tab }

                if *active_tab.read() == 0 {
                    // Jobs tab
                    if rd.jobs.is_empty() {
                        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-8 text-center",
                            p { class: "text-sm text-zinc-500 dark:text-zinc-400", "No jobs in this region." }
                        }
                    } else {
                        JobTable {
                            items: rd.jobs.clone(),
                            drift_items: rd.drift_items.clone(),
                            selected: selected_job,
                        }
                    }
                } else {
                    // DAGs tab
                    if rd.dags.is_empty() {
                        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-8 text-center",
                            p { class: "text-sm text-zinc-500 dark:text-zinc-400", "No DAGs in this region." }
                        }
                    } else {
                        JobTable {
                            items: rd.dags.clone(),
                            drift_items: rd.drift_items.clone(),
                            selected: selected_job,
                        }
                    }
                }

                JobDetailSheet { item: selected_job }
            } else {
                div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-8 text-center",
                    p { class: "text-sm text-zinc-500 dark:text-zinc-400", "Region not found." }
                }
            }
        }
    }
}

// ---- Region detail sub-components ----

#[component]
fn JobTable(
    items: Vec<crate::db::JobSummaryEntity>,
    drift_items: Vec<DriftItem>,
    mut selected: Signal<Option<crate::db::JobSummaryEntity>>,
) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 overflow-hidden",
            table { class: "w-full text-sm",
                thead {
                    tr { class: "border-b border-zinc-200 dark:border-zinc-800 bg-zinc-50/50 dark:bg-zinc-900/50",
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Name" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Type" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Drift" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Fields Changed" }
                    }
                }
                tbody {
                    for item in items.iter() {
                        {
                            let item = item.clone();
                            let drift_item = drift_items.iter().find(|d| d.name == item.name).cloned();
                            rsx! {
                                tr {
                                    class: "border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50/50 dark:hover:bg-zinc-900/50 cursor-pointer transition-colors",
                                    onclick: {
                                        let item = item.clone();
                                        move |_| selected.set(Some(item.clone()))
                                    },
                                    td { class: "px-4 py-3 font-medium text-zinc-950 dark:text-zinc-50", "{item.name}" }
                                    td { class: "px-4 py-3 text-zinc-500 dark:text-zinc-400", "{item.job_type}" }
                                    td { class: "px-4 py-3",
                                        if let Some(di) = &drift_item {
                                            DriftBadge { drift_type: di.drift_type.clone() }
                                        } else {
                                            span { class: "text-zinc-400 dark:text-zinc-600", "\u{2014}" }
                                        }
                                    }
                                    td { class: "px-4 py-3 text-zinc-500 dark:text-zinc-400 text-xs",
                                        if let Some(di) = &drift_item {
                                            if di.fields_changed.is_empty() {
                                                "\u{2014}"
                                            } else {
                                                "{di.fields_changed.join(\", \")}"
                                            }
                                        } else {
                                            "\u{2014}"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn JobDetailSheet(mut item: Signal<Option<crate::db::JobSummaryEntity>>) -> Element {
    let is_open = item().is_some();
    let title = item()
        .as_ref()
        .map(|i| i.name.clone())
        .unwrap_or_default();

    rsx! {
        Sheet {
            open: is_open,
            title,
            width: "w-[640px]".to_string(),
            on_close: move |_| item.set(None),
            match &*item.read() {
                Some(job) => rsx! {
                    // Meta bar
                    div { class: "px-5 py-3 border-b border-zinc-100 dark:border-zinc-800 flex items-center gap-3",
                        span { class: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-blue-50 text-blue-700 border-blue-200",
                            "{job.env_name}"
                        }
                        span { class: "text-xs text-zinc-500 dark:text-zinc-400", "{job.region_name}" }
                        span { class: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border border-zinc-200 dark:border-zinc-700 bg-zinc-50 dark:bg-zinc-800 text-zinc-700 dark:text-zinc-300",
                            "{job.job_type}"
                        }
                    }
                    // Detail content
                    div { class: "px-5 py-4",
                        div { class: "space-y-3",
                            DetailRow { label: "Name", value: job.name.clone() }
                            DetailRow { label: "Environment", value: job.env_name.clone() }
                            DetailRow { label: "Region", value: job.region_name.clone() }
                            DetailRow { label: "Type", value: job.job_type.clone() }
                        }
                    }
                },
                None => rsx! {},
            }
        }
    }
}

#[component]
fn DetailRow(label: &'static str, value: String) -> Element {
    rsx! {
        div { class: "flex items-baseline gap-3",
            span { class: "text-xs font-medium text-zinc-500 dark:text-zinc-400 w-24 shrink-0", "{label}" }
            span { class: "text-sm text-zinc-950 dark:text-zinc-50", "{value}" }
        }
    }
}
