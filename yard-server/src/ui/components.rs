use dioxus::prelude::*;

#[component]
pub fn Pagination(
    page: u32,
    has_more: bool,
    on_prev: EventHandler<MouseEvent>,
    on_next: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        div { class: "flex items-center justify-between mt-4 text-sm",
            button {
                class: format!(
                    "px-3 py-1.5 rounded-md border border-zinc-200 dark:border-zinc-700 cursor-pointer transition-colors {}",
                    if page <= 1 {
                        "text-zinc-300 dark:text-zinc-600 cursor-not-allowed"
                    } else {
                        "text-zinc-700 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-800"
                    }
                ),
                disabled: page <= 1,
                onclick: move |e| on_prev.call(e),
                "Previous"
            }
            span { class: "text-zinc-500 dark:text-zinc-400", "Page {page}" }
            button {
                class: format!(
                    "px-3 py-1.5 rounded-md border border-zinc-200 dark:border-zinc-700 cursor-pointer transition-colors {}",
                    if !has_more {
                        "text-zinc-300 dark:text-zinc-600 cursor-not-allowed"
                    } else {
                        "text-zinc-700 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-800"
                    }
                ),
                disabled: !has_more,
                onclick: move |e| on_next.call(e),
                "Next"
            }
        }
    }
}

#[component]
pub fn TabBar(tabs: Vec<String>, active: Signal<usize>) -> Element {
    rsx! {
        div { class: "flex items-center gap-2 mb-4",
            for (i, tab) in tabs.iter().enumerate() {
                {
                    let tab = tab.clone();
                    let is_active = i == *active.read();
                    rsx! {
                        button {
                            class: format!(
                                "px-3 py-1 rounded-full text-xs font-semibold border cursor-pointer transition-colors {}",
                                if is_active {
                                    "bg-zinc-900 text-white dark:bg-zinc-100 dark:text-zinc-900 border-transparent"
                                } else {
                                    "bg-white text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300 border-zinc-200 dark:border-zinc-700 hover:bg-zinc-50 dark:hover:bg-zinc-700"
                                }
                            ),
                            onclick: {
                                let mut active = active;
                                move |_| active.set(i)
                            },
                            "{tab}"
                        }
                    }
                }
            }
        }
    }
}

#[component]
pub fn Breadcrumb(segments: Vec<(String, Option<String>)>) -> Element {
    let len = segments.len();
    rsx! {
        nav { class: "flex items-center gap-1.5 text-sm mb-4",
            for (i, (label, href)) in segments.iter().enumerate() {
                {
                    let is_last = i == len - 1;
                    let label = label.clone();
                    let href = href.clone();
                    rsx! {
                        if is_last {
                            span {
                                class: "text-zinc-950 dark:text-zinc-50 font-semibold",
                                "{label}"
                            }
                        } else if let Some(path) = href {
                            Link {
                                to: NavigationTarget::<String>::Internal(path),
                                class: "text-blue-600 hover:underline cursor-pointer",
                                "{label}"
                            }
                        } else {
                            span {
                                class: "text-zinc-500 dark:text-zinc-400",
                                "{label}"
                            }
                        }
                        if !is_last {
                            span {
                                class: "text-zinc-300 dark:text-zinc-600",
                                ">"
                            }
                        }
                    }
                }
            }
        }
    }
}
