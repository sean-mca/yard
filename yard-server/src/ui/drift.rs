use dioxus::prelude::*;

use super::sheet::Sheet;
use crate::types::*;

fn mock_drift_data() -> DriftData {
    DriftData {
        in_sync: 3,
        drifted: 3,
        items: vec![
            DriftItem {
                name: "jdbc-test".to_string(),
                environment: "dev".to_string(),
                region: "us-east-2".to_string(),
                drift_type: DriftType::Modified,
                fields_changed: vec!["config".to_string(), "transforms".to_string(), "sink".to_string()],
                old_config: Some(
                    "type: glue\nconfig:\n  timeout: 30\n  max_retries: 3\n  worker_type: G.1X\n  num_workers: 2\nsources:\n  - name: users\n    type: jdbc\n    connection: pg-main\ntransforms:\n  - type: filter\n    condition: \"status = 'active'\"\nsink:\n  write_mode: overwrite\n  path: s3://lake/users/".to_string(),
                ),
                new_config: Some(
                    "type: glue\nconfig:\n  timeout: 60\n  max_retries: 5\n  worker_type: G.2X\n  num_workers: 4\nsources:\n  - name: users\n    type: jdbc\n    connection: pg-main\ntransforms:\n  - type: filter\n    condition: \"status = 'active'\"\n  - type: sql\n    query: \"SELECT *, NOW() as loaded_at FROM users\"\nsink:\n  write_mode: append\n  path: s3://lake/users/".to_string(),
                ),
            },
            DriftItem {
                name: "s3-ingest".to_string(),
                environment: "dev".to_string(),
                region: "us-east-2".to_string(),
                drift_type: DriftType::New,
                fields_changed: vec![],
                old_config: None,
                new_config: Some(
                    "type: glue\nconfig:\n  timeout: 30\n  worker_type: G.1X\nsources:\n  - name: events\n    type: s3\n    path: s3://raw/events/\nsink:\n  write_mode: append\n  path: s3://lake/events/".to_string(),
                ),
            },
            DriftItem {
                name: "legacy-etl".to_string(),
                environment: "prod".to_string(),
                region: "us-east-1".to_string(),
                drift_type: DriftType::Deleted,
                fields_changed: vec![],
                old_config: Some(
                    "type: glue\nconfig:\n  timeout: 120\n  worker_type: G.1X\nsources:\n  - name: orders\n    type: jdbc\n    connection: mysql-legacy\nsink:\n  write_mode: overwrite\n  path: s3://lake/orders/".to_string(),
                ),
                new_config: None,
            },
        ],
    }
}

#[component]
pub fn Drift() -> Element {
    let data = mock_drift_data();
    let mut selected = use_signal(|| None::<DriftItem>);

    let total = data.in_sync + data.drifted;

    rsx! {
        div { class: "p-6",
            // Summary cards
            div { class: "grid grid-cols-3 gap-4 mb-6",
                SummaryCard { label: "Total Jobs", value: format!("{total}") }
                SummaryCard { label: "In Sync", value: format!("{}", data.in_sync), accent: "emerald" }
                SummaryCard {
                    label: "Drifted",
                    value: format!("{}", data.drifted),
                    accent: if data.drifted > 0 { "amber" } else { "emerald" },
                }
            }

            if data.items.is_empty() {
                div { class: "rounded-lg border border-emerald-200 bg-emerald-50 px-4 py-8 text-center",
                    div { class: "flex items-center justify-center gap-2 text-emerald-700",
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
                DriftTable { items: data.items.clone(), selected }
            }

            DriftSheet { item: selected }
        }
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
        div { class: format!("rounded-lg border bg-white p-4 {border}"),
            p { class: "text-xs font-medium text-zinc-500 mb-1", "{label}" }
            p { class: format!("text-2xl font-semibold tracking-tight {value_color}"), "{value}" }
        }
    }
}

#[component]
fn DriftTable(items: Vec<DriftItem>, mut selected: Signal<Option<DriftItem>>) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 overflow-hidden",
            table { class: "w-full text-sm",
                thead {
                    tr { class: "border-b border-zinc-200 bg-zinc-50/50",
                        th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Name" }
                        th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Environment" }
                        th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Region" }
                        th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Drift" }
                        th { class: "text-left font-medium text-zinc-500 px-4 py-3", "Fields Changed" }
                    }
                }
                tbody {
                    for item in items.iter() {
                        {
                            let item = item.clone();
                            rsx! {
                                tr {
                                    class: "border-b border-zinc-100 hover:bg-zinc-50/50 cursor-pointer transition-colors",
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
                                    td { class: "px-4 py-3 text-zinc-500", "{item.region}" }
                                    td { class: "px-4 py-3", DriftBadge { drift_type: item.drift_type.clone() } }
                                    td { class: "px-4 py-3 text-zinc-500 text-xs",
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
                    div { class: "px-5 py-3 border-b border-zinc-100 flex items-center gap-3",
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
        "blue" => ("border-blue-200", "bg-blue-50", "text-blue-700", "bg-blue-400"),
        "violet" => ("border-violet-200", "bg-violet-50", "text-violet-700", "bg-violet-400"),
        _ => ("border-zinc-200", "bg-zinc-50", "text-zinc-700", "bg-zinc-400"),
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
        for i in start..end {
            visible[i] = true;
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
