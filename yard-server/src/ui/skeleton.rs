use dioxus::prelude::*;

#[component]
pub fn SkeletonCard(width: String, height: String) -> Element {
    rsx! {
        div {
            class: format!("rounded-lg {width} {height} bg-zinc-200 dark:bg-zinc-800 animate-pulse"),
        }
    }
}

#[component]
pub fn SkeletonTable(rows: u32, cols: u32) -> Element {
    rsx! {
        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-800 overflow-hidden",
            div { class: "h-[44px] bg-zinc-100 dark:bg-zinc-800 animate-pulse" }
            for _i in 0..rows {
                div { class: "h-[48px] border-t border-zinc-100 dark:border-zinc-800",
                    div { class: "flex items-center gap-4 h-full px-4",
                        for _j in 0..cols {
                            div { class: "h-4 flex-1 rounded bg-zinc-200 dark:bg-zinc-800 animate-pulse" }
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub fn SkeletonText(#[props(default = "w-full".to_string())] width: String) -> Element {
    rsx! {
        div {
            class: format!("h-4 rounded bg-zinc-200 dark:bg-zinc-800 animate-pulse {width}"),
        }
    }
}
