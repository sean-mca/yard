use dioxus::prelude::*;

#[component]
pub fn Sidebar(open: Signal<bool>) -> Element {
    rsx! {
        aside {
            class: format!(
                "h-screen sticky top-0 flex flex-col border-r border-zinc-200 bg-zinc-50/75 transition-all duration-200 {}",
                if open() { "w-64" } else { "w-14" }
            ),
            div { class: "flex items-center h-14 px-3 border-b border-zinc-200",
                if open() {
                    span { class: "text-sm font-semibold tracking-tight pl-1", "yard" }
                }
                button {
                    class: "ml-auto p-1.5 rounded-md text-zinc-500 hover:text-zinc-950 hover:bg-zinc-200/50 cursor-pointer transition-colors",
                    onclick: move |_| open.toggle(),
                    if open() {
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            width: "16", height: "16",
                            view_box: "0 0 24 24",
                            fill: "none",
                            stroke: "currentColor",
                            stroke_width: "2",
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            path { d: "m15 18-6-6 6-6" }
                        }
                    } else {
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            width: "16", height: "16",
                            view_box: "0 0 24 24",
                            fill: "none",
                            stroke: "currentColor",
                            stroke_width: "2",
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            path { d: "m9 18 6-6-6-6" }
                        }
                    }
                }
            }
            nav { class: "flex-1 flex flex-col gap-1 p-2",
                SidebarItem { icon: "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1", label: "Dashboard", expanded: open(), active: true }
                SidebarItem { icon: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2", label: "Jobs", expanded: open(), active: false }
                SidebarItem { icon: "M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15", label: "Drift", expanded: open(), active: false }
                SidebarItem { icon: "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z", label: "Settings", expanded: open(), active: false }
            }
        }
    }
}

#[component]
fn SidebarItem(icon: &'static str, label: &'static str, expanded: bool, active: bool) -> Element {
    rsx! {
        div { class: "relative group",
            button {
                class: format!(
                    "flex items-center gap-3 rounded-md text-sm cursor-pointer transition-colors {} {}",
                    if expanded { "px-2.5 py-1.5" } else { "justify-center p-2" },
                    if active {
                        "bg-zinc-200/75 text-zinc-950 font-medium"
                    } else {
                        "text-zinc-500 hover:text-zinc-950 hover:bg-zinc-100"
                    }
                ),
                svg {
                    xmlns: "http://www.w3.org/2000/svg",
                    width: "16", height: "16",
                    view_box: "0 0 24 24",
                    fill: "none",
                    stroke: "currentColor",
                    stroke_width: "2",
                    stroke_linecap: "round",
                    stroke_linejoin: "round",
                    class: "shrink-0",
                    path { d: "{icon}" }
                }
                if expanded {
                    span { "{label}" }
                }
            }
            if !expanded {
                div {
                    class: "absolute left-full top-1/2 -translate-y-1/2 ml-2 px-2 py-1 rounded-md bg-zinc-950 text-zinc-50 text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-50",
                    "{label}"
                }
            }
        }
    }
}
