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
                fields_changed: vec!["config".to_string(), "transforms".to_string()],
            },
            DriftItem {
                name: "s3-ingest".to_string(),
                environment: "dev".to_string(),
                region: "us-east-2".to_string(),
                drift_type: DriftType::New,
                fields_changed: vec![],
            },
            DriftItem {
                name: "legacy-etl".to_string(),
                environment: "prod".to_string(),
                region: "us-east-1".to_string(),
                drift_type: DriftType::Deleted,
                fields_changed: vec![],
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

                    // Diff placeholder
                    match &drift_item.drift_type {
                        DriftType::Modified => rsx! {
                            div { class: "px-5 py-4",
                                p { class: "text-xs font-medium text-zinc-500 mb-3", "Changed fields" }
                                div { class: "space-y-2",
                                    for field in drift_item.fields_changed.iter() {
                                        div { class: "rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 text-xs font-mono",
                                            "{field}"
                                        }
                                    }
                                }
                                p { class: "mt-6 text-xs text-zinc-400 italic",
                                    "Full diff view will be available when drift detection is wired up."
                                }
                            }
                        },
                        DriftType::New => rsx! {
                            div { class: "px-5 py-4",
                                div { class: "rounded-md border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm text-emerald-700",
                                    "This job exists in config but has not been deployed yet."
                                }
                            }
                        },
                        DriftType::Deleted => rsx! {
                            div { class: "px-5 py-4",
                                div { class: "rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700",
                                    "This job was deployed but is no longer present in config."
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
