use dioxus::prelude::*;

use super::components::Pagination;
use super::sheet::Sheet;
use crate::types::*;

const PER_PAGE: usize = 15;

const API_BASE: &str = "http://127.0.0.1:3001";

async fn fetch_job_file(path: String) -> Result<String, String> {
    let url = format!("{API_BASE}/api/jobs/file?path={path}");
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Server error ({status}): {body}"));
    }

    resp.text()
        .await
        .map_err(|e| format!("Failed to read response: {e}"))
}

async fn fetch_jobs() -> Result<JobsData, String> {
    let resp = reqwest::get(format!("{API_BASE}/api/jobs"))
        .await
        .map_err(|e| format!("Request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Server error ({status}): {body}"));
    }

    resp.json::<JobsData>()
        .await
        .map_err(|e| format!("Failed to parse response: {e}"))
}

#[component]
pub fn Jobs() -> Element {
    let data = use_resource(fetch_jobs);
    let mut search = use_signal(String::new);

    rsx! {
        div { class: "p-6",
            SearchBar { search }
            match &*data.read() {
                Some(Ok(jobs_data)) => {
                    rsx! { FilteredJobs { jobs: jobs_data.jobs.clone(), search: search() } }
                },
                Some(Err(e)) => rsx! {
                    div { class: "rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700",
                        "Failed to load jobs: {e}"
                    }
                },
                None => rsx! {
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
                },
            }
        }
    }
}

#[component]
fn SearchBar(mut search: Signal<String>) -> Element {
    rsx! {
        div { class: "mb-4 flex items-center justify-end",
            div { class: "relative",
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
                    placeholder: "Search jobs...",
                    class: "pl-8 pr-3 py-1.5 text-sm rounded-md border border-zinc-200 bg-white focus:outline-none focus:ring-2 focus:ring-zinc-300 w-56",
                    value: "{search}",
                    oninput: move |e| search.set(e.value()),
                }
            }
        }
    }
}

#[component]
fn FilteredJobs(jobs: Vec<JobInfo>, search: String) -> Element {
    let mut page = use_signal(|| 1u32);
    let mut selected_job = use_signal(|| None::<JobInfo>);

    let query = search.to_lowercase();
    let filtered: Vec<&JobInfo> = jobs
        .iter()
        .filter(|j| {
            query.is_empty()
                || j.name.to_lowercase().contains(&query)
                || j.path.to_lowercase().contains(&query)
                || j.environment.to_lowercase().contains(&query)
                || j.region.to_lowercase().contains(&query)
        })
        .collect();
    let total = jobs.len();
    let shown = filtered.len();

    // Reset to page 1 when search changes
    let max_page = ((shown as f64) / (PER_PAGE as f64)).ceil().max(1.0) as u32;
    if page() > max_page {
        page.set(1);
    }

    let start = ((page() - 1) as usize) * PER_PAGE;
    let page_items: Vec<&&JobInfo> = filtered.iter().skip(start).take(PER_PAGE).collect();
    let has_more = start + PER_PAGE < shown;

    let label = if total == shown {
        let s = if total != 1 { "s" } else { "" };
        format!("{total} job{s} tracked")
    } else {
        format!("{shown} of {total} jobs")
    };

    rsx! {
        p { class: "text-sm text-zinc-500 mb-4", "{label}" }
        div { class: "rounded-lg border border-zinc-200 overflow-hidden",
            if page_items.is_empty() {
                div { class: "px-4 py-8 text-center text-sm text-zinc-500",
                    "No jobs found."
                }
            } else {
                table { class: "w-full text-sm",
                    thead {
                        tr { class: "border-b border-zinc-200 bg-zinc-50/50",
                            th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Name" }
                            th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Environment" }
                            th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Region" }
                            th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Path" }
                        }
                    }
                    tbody {
                        for job in page_items.iter() {
                            {
                                let job = (*job).clone();
                                rsx! {
                                    tr {
                                        class: "border-b border-zinc-100 hover:bg-zinc-50/50 cursor-pointer transition-colors",
                                        onclick: {
                                            let job = job.clone();
                                            move |_| selected_job.set(Some(job.clone()))
                                        },
                                        td { class: "px-4 py-3 font-medium", "{job.name}" }
                                        td { class: "px-4 py-3",
                                            span { class: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-blue-50 text-blue-700 border-blue-200",
                                                "{job.environment}"
                                            }
                                        }
                                        td { class: "px-4 py-3 text-zinc-500", "{job.region}" }
                                        td { class: "px-4 py-3 text-zinc-500 font-mono text-xs", "{job.path}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        if shown > PER_PAGE {
            Pagination {
                page: page(),
                has_more,
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
        JobSheet { job: selected_job }
    }
}

#[component]
fn JobSheet(mut job: Signal<Option<JobInfo>>) -> Element {
    let is_open = job().is_some();
    let title = job().as_ref().map(|j| j.name.clone()).unwrap_or_default();
    let path = job().as_ref().map(|j| j.path.clone()).unwrap_or_default();

    let file_content = use_resource(move || {
        let path = job().as_ref().map(|j| j.path.clone());
        async move {
            match path {
                Some(p) => Some(fetch_job_file(p).await),
                None => None,
            }
        }
    });

    rsx! {
        Sheet {
            open: is_open,
            title,
            on_close: move |_| job.set(None),
            if !path.is_empty() {
                div { class: "px-5 py-3 border-b border-zinc-100",
                    p { class: "text-xs font-mono text-zinc-400", "{path}" }
                }
            }
            match &*file_content.read() {
                Some(Some(Ok(content))) => rsx! {
                    pre { class: "px-5 py-4 text-xs font-mono text-zinc-700 whitespace-pre-wrap",
                        "{content}"
                    }
                },
                Some(Some(Err(e))) => rsx! {
                    div { class: "px-5 py-4 text-sm text-red-600", "Error: {e}" }
                },
                _ if is_open => rsx! {
                    div { class: "px-5 py-4 flex items-center gap-2 text-sm text-zinc-500",
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            width: "14", height: "14",
                            view_box: "0 0 24 24",
                            fill: "none",
                            stroke: "currentColor",
                            stroke_width: "2",
                            class: "animate-spin",
                            path { d: "M21 12a9 9 0 11-6.219-8.56" }
                        }
                        "Loading..."
                    }
                },
                _ => rsx! {},
            }
        }
    }
}

