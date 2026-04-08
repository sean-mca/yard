use dioxus::prelude::*;
use dioxus_query::prelude::*;
use std::time::Duration;

use super::sheet::Sheet;
use crate::types::*;

use super::api_base;

// ---- Query type ----

#[derive(Clone, PartialEq, Hash, Eq)]
struct DriftQuery;

impl QueryCapability for DriftQuery {
    type Ok = DriftData;
    type Err = String;
    type Keys = ();

    async fn run(&self, _: &Self::Keys) -> Result<Self::Ok, Self::Err> {
        let resp = reqwest::get(format!("{}/api/drift/cached", api_base()))
            .await
            .map_err(|e| format!("Request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Server error ({status}): {body}"));
        }

        resp.json::<DriftData>()
            .await
            .map_err(|e| format!("Failed to parse response: {e}"))
    }
}

// ---- Component ----

#[component]
pub fn Drift() -> Element {
    let data = use_query(
        Query::new((), DriftQuery)
            .stale_time(Duration::from_secs(30))
            .interval_time(Duration::from_secs(15)),
    );
    let selected = use_signal(|| None::<DriftItem>);

    let data_state = data.read();
    match &*data_state.state() {
        QueryStateData::Settled { res: Ok(drift_data), .. } => {
            let total = drift_data.in_sync + drift_data.drifted;
            rsx! {
                div { class: "p-6",
                    div { class: "grid grid-cols-3 gap-4 mb-6",
                        SummaryCard { label: "Total Jobs", value: format!("{total}") }
                        SummaryCard { label: "In Sync", value: format!("{}", drift_data.in_sync), accent: "emerald" }
                        SummaryCard {
                            label: "Drifted",
                            value: format!("{}", drift_data.drifted),
                            accent: if drift_data.drifted > 0 { "amber" } else { "emerald" },
                        }
                    }

                    if drift_data.items.is_empty() {
                        div { class: "rounded-lg border border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950 px-4 py-8 text-center",
                            div { class: "flex items-center justify-center gap-2 text-emerald-700 dark:text-emerald-300",
                                svg {
                                    xmlns: "http://www.w3.org/2000/svg",
                                    width: "18", height: "18",
                                    view_box: "0 0 24 24",
                                    fill: "none",
                                    stroke: "currentColor",
                                    stroke_width: "2",
                                    stroke_linecap: "round",
                                    stroke_linejoin: "round",
                                    path { d: "M22 11.08V12a10 10 0 11-5.93-9.14" }
                                    path { d: "M22 4L12 14.01l-3-3" }
                                }
                                p { class: "text-sm font-medium", "All jobs in sync" }
                            }
                        }
                    } else {
                        DriftTable { items: drift_data.items.clone(), selected }
                    }

                    DriftSheet { item: selected }
                }
            }
        }
        QueryStateData::Settled { res: Err(e), .. } => rsx! {
            div { class: "p-6",
                div { class: "rounded-lg border border-red-200 bg-red-50 px-4 py-8 text-center",
                    p { class: "text-sm text-red-700", "Failed to load drift data: {e}" }
                }
            }
        },
        _ => rsx! {
            div { class: "p-6",
                div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 px-4 py-8 text-center",
                    p { class: "text-sm text-zinc-500", "Running drift check..." }
                }
            }
        },
    }
}

#[component]
fn SummaryCard(label: &'static str, value: String, #[props(default)] accent: String) -> Element {
    let border = match accent.as_str() {
        "emerald" => "border-emerald-200",
        "amber" => "border-amber-200",
        _ => "border-zinc-200",
    };
    let value_color = match accent.as_str() {
        "emerald" => "text-emerald-600",
        "amber" => "text-amber-600",
        _ => "text-zinc-950",
    };

    rsx! {
        div { class: format!("rounded-lg border bg-white dark:bg-zinc-900 p-4 {border} dark:border-zinc-800"),
            p { class: "text-xs font-medium text-zinc-500 dark:text-zinc-400 mb-1", "{label}" }
            p { class: format!("text-2xl font-semibold tracking-tight {value_color}"), "{value}" }
        }
    }
}

#[component]
fn DriftTable(items: Vec<DriftItem>, mut selected: Signal<Option<DriftItem>>) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 overflow-hidden",
            table { class: "w-full text-sm",
                thead {
                    tr { class: "border-b border-zinc-200 dark:border-zinc-800 bg-zinc-50/50 dark:bg-zinc-900/50",
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Name" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Environment" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Region" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Drift" }
                        th { class: "text-left font-medium text-zinc-500 dark:text-zinc-400 px-4 py-3", "Fields Changed" }
                    }
                }
                tbody {
                    for item in items.iter() {
                        {
                            let item = item.clone();
                            rsx! {
                                tr {
                                    class: "border-b border-zinc-100 dark:border-zinc-800 hover:bg-zinc-50/50 dark:hover:bg-zinc-900/50 cursor-pointer transition-colors",
                                    onclick: {
                                        let item = item.clone();
                                        move |_| selected.set(Some(item.clone()))
                                    },
                                    td { class: "px-4 py-3 font-medium", "{item.name}" }
                                    td { class: "px-4 py-3",
                                        span { class: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-blue-50 text-blue-700 border-blue-200",
                                            "{item.environment}"
                                        }
                                    }
                                    td { class: "px-4 py-3 text-zinc-500 dark:text-zinc-400", "{item.region}" }
                                    td { class: "px-4 py-3", DriftBadge { drift_type: item.drift_type.clone() } }
                                    td { class: "px-4 py-3 text-zinc-500 dark:text-zinc-400 text-xs",
                                        if item.fields_changed.is_empty() {
                                            "—"
                                        } else {
                                            "{item.fields_changed.join(\", \")}"
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
fn DriftBadge(drift_type: DriftType) -> Element {
    let (label, classes) = match drift_type {
        DriftType::Modified => ("Modified", "bg-amber-50 text-amber-700 border-amber-200"),
        DriftType::New => ("New", "bg-emerald-50 text-emerald-700 border-emerald-200"),
        DriftType::Deleted => ("Deleted", "bg-red-50 text-red-700 border-red-200"),
        DriftType::ResourceMissing => ("Resource Missing", "bg-purple-50 text-purple-700 border-purple-200"),
    };

    rsx! {
        span { class: format!("inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border {classes}"),
            "{label}"
        }
    }
}

#[component]
fn DriftSheet(mut item: Signal<Option<DriftItem>>) -> Element {
    let is_open = item().is_some();
    let title = item().as_ref().map(|i| i.name.clone()).unwrap_or_default();

    rsx! {
        Sheet {
            open: is_open,
            title,
            width: "w-[640px]".to_string(),
            on_close: move |_| item.set(None),
            match &*item.read() {
                Some(drift_item) => rsx! {
                    // Meta
                    div { class: "px-5 py-3 border-b border-zinc-100 dark:border-zinc-800 flex items-center gap-3",
                        span { class: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border bg-blue-50 text-blue-700 border-blue-200",
                            "{drift_item.environment}"
                        }
                        span { class: "text-xs text-zinc-500", "{drift_item.region}" }
                        DriftBadge { drift_type: drift_item.drift_type.clone() }
                    }

                    match &drift_item.drift_type {
                        DriftType::Modified => {
                            let old = drift_item.old_config.as_deref().unwrap_or("");
                            let new = drift_item.new_config.as_deref().unwrap_or("");
                            let changed_old = compute_changed_lines(old, new);
                            let changed_new = compute_changed_lines(new, old);
                            let old_html = render_panel_collapsed(old, &changed_old, "blue");
                            let new_html = render_panel_collapsed(new, &changed_new, "violet");
                            rsx! {
                                div { class: "px-5 py-4",
                                    if !drift_item.fields_changed.is_empty() {
                                        div { class: "mb-4",
                                            p { class: "text-xs font-medium text-zinc-500 mb-2", "Changed fields" }
                                            div { class: "flex gap-1.5 flex-wrap",
                                                for field in drift_item.fields_changed.iter() {
                                                    span { class: "px-2 py-0.5 rounded-full text-xs font-medium border border-amber-200 bg-amber-50 text-amber-700",
                                                        "{field}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    ConfigPanel { label: "Deployed", color: "blue", html: old_html }
                                    div { class: "mt-3" }
                                    ConfigPanel { label: "Config (repo)", color: "violet", html: new_html }
                                }
                            }
                        },
                        DriftType::New => {
                            let new = drift_item.new_config.as_deref().unwrap_or("");
                            let all: Vec<usize> = (0..new.lines().count()).collect();
                            let html = render_panel_collapsed(new, &all, "violet");
                            rsx! {
                                div { class: "px-5 py-4",
                                    div { class: "rounded-md border border-violet-200 bg-violet-50 px-3 py-2 text-sm text-violet-700 mb-4",
                                        "This job exists in config but has not been deployed yet."
                                    }
                                    ConfigPanel { label: "Config (repo)", color: "violet", html }
                                }
                            }
                        },
                        DriftType::Deleted => {
                            let old = drift_item.old_config.as_deref().unwrap_or("");
                            let all: Vec<usize> = (0..old.lines().count()).collect();
                            let html = render_panel_collapsed(old, &all, "blue");
                            rsx! {
                                div { class: "px-5 py-4",
                                    div { class: "rounded-md border border-blue-200 bg-blue-50 px-3 py-2 text-sm text-blue-700 mb-4",
                                        "This job was deployed but is no longer present in config."
                                    }
                                    ConfigPanel { label: "Deployed", color: "blue", html }
                                }
                            }
                        },
                        DriftType::ResourceMissing => {
                            rsx! {
                                div { class: "px-5 py-4",
                                    div { class: "rounded-md border border-purple-200 bg-purple-50 px-3 py-2 text-sm text-purple-700 mb-4",
                                        "One or more AWS resources for this job were deleted outside of yard."
                                    }
                                    if !drift_item.fields_changed.is_empty() {
                                        div { class: "mt-3",
                                            p { class: "text-xs font-medium text-zinc-500 mb-2", "Missing resources" }
                                            div { class: "flex gap-1.5 flex-wrap",
                                                for resource_type in drift_item.fields_changed.iter() {
                                                    span { class: "px-2 py-0.5 rounded-full text-xs font-medium border border-purple-200 bg-purple-50 text-purple-700",
                                                        "{resource_type}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                    }
                },
                None => rsx! {},
            }
        }
    }
}

#[component]
fn ConfigPanel(label: &'static str, color: &'static str, html: String) -> Element {
    let (border, header_bg, header_text, dot) = match color {
        "blue" => ("border-blue-200 dark:border-blue-800", "bg-blue-50 dark:bg-blue-950", "text-blue-700 dark:text-blue-300", "bg-blue-400"),
        "violet" => ("border-violet-200 dark:border-violet-800", "bg-violet-50 dark:bg-violet-950", "text-violet-700 dark:text-violet-300", "bg-violet-400"),
        _ => ("border-zinc-200 dark:border-zinc-700", "bg-zinc-50 dark:bg-zinc-800", "text-zinc-700 dark:text-zinc-300", "bg-zinc-400"),
    };

    rsx! {
        div { class: format!("rounded-md border {border} overflow-hidden"),
            div { class: format!("px-3 py-1.5 {header_bg} border-b {border} flex items-center gap-2"),
                span { class: format!("w-2 h-2 rounded-full {dot}") }
                span { class: format!("text-xs font-medium {header_text}"), "{label}" }
            }
            pre { class: "text-xs font-mono leading-5 overflow-x-auto",
                dangerous_inner_html: "{html}",
            }
        }
    }
}

// --- Diff engine ---

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Returns line indices in `source` that don't appear in the LCS with `other`.
fn compute_changed_lines(source: &str, other: &str) -> Vec<usize> {
    let src: Vec<&str> = source.lines().collect();
    let oth: Vec<&str> = other.lines().collect();
    let m = src.len();
    let n = oth.len();

    // LCS table
    let mut table = vec![vec![0u32; n + 1]; m + 1];
    for i in 1..=m {
        for j in 1..=n {
            if src[i - 1] == oth[j - 1] {
                table[i][j] = table[i - 1][j - 1] + 1;
            } else {
                table[i][j] = table[i - 1][j].max(table[i][j - 1]);
            }
        }
    }

    // Backtrack — collect source indices NOT in LCS
    let mut in_lcs = vec![false; m];
    let mut i = m;
    let mut j = n;
    while i > 0 && j > 0 {
        if src[i - 1] == oth[j - 1] {
            in_lcs[i - 1] = true;
            i -= 1;
            j -= 1;
        } else if table[i - 1][j] >= table[i][j - 1] {
            i -= 1;
        } else {
            j -= 1;
        }
    }

    (0..m).filter(|idx| !in_lcs[*idx]).collect()
}

const CONTEXT_LINES: usize = 3;

/// Render a YAML panel with changed lines highlighted and unchanged sections collapsed.
fn render_panel_collapsed(content: &str, changed_lines: &[usize], color: &str) -> String {
    let (highlight_bg, highlight_text) = match color {
        "blue" => ("bg-blue-50", "text-blue-800"),
        "violet" => ("bg-violet-50", "text-violet-800"),
        _ => ("bg-zinc-50", "text-zinc-800"),
    };

    let lines: Vec<&str> = content.lines().collect();
    let total = lines.len();

    if changed_lines.is_empty() || total == 0 {
        // Nothing changed — show a summary line
        return format!(
            "<div class=\"px-3 py-1.5 text-zinc-400 text-center italic\">No changes ({total} lines)</div>"
        );
    }

    // Build a set of visible line indices: changed lines + context
    let mut visible = vec![false; total];
    for &idx in changed_lines {
        let start = idx.saturating_sub(CONTEXT_LINES);
        let end = (idx + CONTEXT_LINES + 1).min(total);
        for v in &mut visible[start..end] {
            *v = true;
        }
    }

    let mut out = String::new();
    let mut i = 0;
    while i < total {
        if visible[i] {
            let is_changed = changed_lines.contains(&i);
            if is_changed {
                out.push_str(&format!(
                    "<div class=\"px-3 py-px {highlight_bg} {highlight_text}\">{}</div>",
                    html_escape(lines[i])
                ));
            } else {
                out.push_str(&format!(
                    "<div class=\"px-3 py-px text-zinc-600\">{}</div>",
                    html_escape(lines[i])
                ));
            }
            i += 1;
        } else {
            // Count consecutive hidden lines
            let start = i;
            while i < total && !visible[i] {
                i += 1;
            }
            let hidden = i - start;
            out.push_str(&format!(
                "<div class=\"px-3 py-1 text-zinc-400 bg-zinc-50 text-center text-[11px] border-y border-zinc-100\">{hidden} unchanged line{}</div>",
                if hidden == 1 { "" } else { "s" }
            ));
        }
    }
    out
}
