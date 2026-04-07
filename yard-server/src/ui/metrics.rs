use dioxus::prelude::*;

#[derive(Clone, PartialEq)]
pub enum DriftStatus {
    Ok,
    Drifted(u32),
}

#[component]
pub fn MetricsBar(
    open_prs: u32,
    plans_running: u32,
    drift: DriftStatus,
    jobs_tracked: u32,
) -> Element {
    rsx! {
        div { class: "grid grid-cols-4 gap-4 mb-6",
            MetricCard { label: "Open PRs", value: "{open_prs}" }
            MetricCard { label: "Plans Running", value: "{plans_running}" }
            DriftCard { status: drift }
            MetricCard { label: "Jobs Tracked", value: "{jobs_tracked}" }
        }
    }
}

#[component]
fn MetricCard(label: &'static str, value: String) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 bg-white p-4",
            p { class: "text-xs font-medium text-zinc-500 mb-1", "{label}" }
            p { class: "text-2xl font-semibold tracking-tight", "{value}" }
        }
    }
}

#[component]
fn DriftCard(status: DriftStatus) -> Element {
    let (icon_color, bg, label, sublabel) = match &status {
        DriftStatus::Ok => (
            "text-emerald-500",
            "bg-emerald-50 border-emerald-200",
            "No Drift",
            "All jobs in sync",
        ),
        DriftStatus::Drifted(count) => (
            "text-amber-500",
            "bg-amber-50 border-amber-200",
            "Drift Detected",
            if *count == 1 { "1 job drifted" } else { "jobs drifted" },
        ),
    };

    rsx! {
        div { class: format!("rounded-lg border p-4 {bg}"),
            p { class: "text-xs font-medium text-zinc-500 mb-1", "Drift Status" }
            div { class: "flex items-center gap-2",
                match &status {
                    DriftStatus::Ok => rsx! {
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            width: "20", height: "20",
                            view_box: "0 0 24 24",
                            fill: "none",
                            stroke: "currentColor",
                            stroke_width: "2",
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            class: icon_color,
                            path { d: "M22 11.08V12a10 10 0 11-5.93-9.14" }
                            path { d: "M22 4L12 14.01l-3-3" }
                        }
                    },
                    DriftStatus::Drifted(_) => rsx! {
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            width: "20", height: "20",
                            view_box: "0 0 24 24",
                            fill: "none",
                            stroke: "currentColor",
                            stroke_width: "2",
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            class: icon_color,
                            path { d: "M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" }
                            line { x1: "12", y1: "9", x2: "12", y2: "13" }
                            line { x1: "12", y1: "17", x2: "12.01", y2: "17" }
                        }
                    },
                }
                div {
                    p { class: "text-sm font-semibold", "{label}" }
                    p { class: "text-xs text-zinc-500",
                        match &status {
                            DriftStatus::Ok => rsx! { "{sublabel}" },
                            DriftStatus::Drifted(count) => rsx! { "{count} {sublabel}" },
                        }
                    }
                }
            }
        }
    }
}
