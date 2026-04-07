use dioxus::prelude::*;

#[component]
pub fn Drift() -> Element {
    rsx! {
        div { class: "p-6",
            div { class: "rounded-lg border border-zinc-200 bg-zinc-50/50 px-4 py-8 text-center",
                p { class: "text-sm text-zinc-500", "Drift detection coming soon." }
            }
        }
    }
}
