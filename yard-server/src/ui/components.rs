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
                    "px-3 py-1.5 rounded-md border border-zinc-200 cursor-pointer transition-colors {}",
                    if page <= 1 {
                        "text-zinc-300 cursor-not-allowed"
                    } else {
                        "text-zinc-700 hover:bg-zinc-50"
                    }
                ),
                disabled: page <= 1,
                onclick: move |e| on_prev.call(e),
                "Previous"
            }
            span { class: "text-zinc-500", "Page {page}" }
            button {
                class: format!(
                    "px-3 py-1.5 rounded-md border border-zinc-200 cursor-pointer transition-colors {}",
                    if !has_more {
                        "text-zinc-300 cursor-not-allowed"
                    } else {
                        "text-zinc-700 hover:bg-zinc-50"
                    }
                ),
                disabled: !has_more,
                onclick: move |e| on_next.call(e),
                "Next"
            }
        }
    }
}
