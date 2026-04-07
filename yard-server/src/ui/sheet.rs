use dioxus::prelude::*;

#[component]
pub fn Sheet(
    open: bool,
    title: String,
    on_close: EventHandler<MouseEvent>,
    children: Element,
) -> Element {
    rsx! {
        // Backdrop
        if open {
            div {
                class: "fixed inset-0 bg-black/20 z-40 transition-opacity",
                onclick: move |e| on_close.call(e),
            }
        }
        // Panel
        div {
            class: format!(
                "fixed top-0 right-0 h-full w-[480px] bg-white border-l border-zinc-200 shadow-xl z-50 transform transition-transform duration-200 {}",
                if open { "translate-x-0" } else { "translate-x-full" }
            ),
            // Header
            div { class: "flex items-center justify-between h-14 px-5 border-b border-zinc-200",
                h2 { class: "text-sm font-semibold truncate", "{title}" }
                button {
                    class: "p-1.5 rounded-md text-zinc-400 hover:text-zinc-950 hover:bg-zinc-100 cursor-pointer transition-colors",
                    onclick: move |e| on_close.call(e),
                    svg {
                        xmlns: "http://www.w3.org/2000/svg",
                        width: "16", height: "16",
                        view_box: "0 0 24 24",
                        fill: "none",
                        stroke: "currentColor",
                        stroke_width: "2",
                        stroke_linecap: "round",
                        stroke_linejoin: "round",
                        line { x1: "18", y1: "6", x2: "6", y2: "18" }
                        line { x1: "6", y1: "6", x2: "18", y2: "18" }
                    }
                }
            }
            // Body
            div { class: "overflow-y-auto h-[calc(100%-3.5rem)]",
                {children}
            }
        }
    }
}
