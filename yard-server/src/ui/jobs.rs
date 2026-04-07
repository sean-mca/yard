use dioxus::prelude::*;

use crate::types::*;

const API_BASE: &str = "http://127.0.0.1:3001";

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

    match &*data.read() {
        Some(Ok(jobs_data)) => {
            let query = search().to_lowercase();
            let filtered: Vec<JobInfo> = jobs_data
                .jobs
                .iter()
                .filter(|j| {
                    query.is_empty()
                        || j.name.to_lowercase().contains(&query)
                        || j.path.to_lowercase().contains(&query)
                })
                .cloned()
                .collect();
            let total = jobs_data.jobs.len();
            let shown = filtered.len();

            rsx! {
                div { class: "p-6",
                    div { class: "mb-4 flex items-center justify-between",
                        {
                            let label = if total == shown {
                                let s = if total != 1 { "s" } else { "" };
                                format!("{total} job{s} tracked")
                            } else {
                                format!("{shown} of {total} jobs")
                            };
                            rsx! {
                                p { class: "text-sm text-zinc-500", "{label}" }
                            }
                        }
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
                    JobsTable { jobs: filtered }
                }
            }
        },
        Some(Err(e)) => rsx! {
            div { class: "p-6",
                div { class: "rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700",
                    "Failed to load jobs: {e}"
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
fn JobsTable(jobs: Vec<JobInfo>) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 overflow-hidden",
            if jobs.is_empty() {
                div { class: "px-4 py-8 text-center text-sm text-zinc-500",
                    "No jobs found in repository."
                }
            } else {
                table { class: "w-full text-sm",
                    thead {
                        tr { class: "border-b border-zinc-200 bg-zinc-50/50",
                            th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Name" }
                            th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Path" }
                        }
                    }
                    tbody {
                        for job in jobs.iter() {
                            tr { class: "border-b border-zinc-100 hover:bg-zinc-50/50 transition-colors",
                                td { class: "px-4 py-3 font-medium", "{job.name}" }
                                td { class: "px-4 py-3 text-zinc-500 font-mono text-xs", "{job.path}" }
                            }
                        }
                    }
                }
            }
        }
    }
}
