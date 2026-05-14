use dioxus::prelude::*;

use crate::Route;
use crate::ui::settings::Theme;

#[component]
pub fn Sidebar(open: Signal<bool>, route: Route) -> Element {
    rsx! {
        aside {
            class: format!(
                "h-screen sticky top-0 flex flex-col border-r border-zinc-200 dark:border-zinc-800 bg-zinc-50/75 dark:bg-zinc-900/75 transition-all duration-200 {}",
                if open() { "w-64" } else { "w-14" }
            ),
            div { class: "flex items-center h-14 px-3 border-b border-zinc-200 dark:border-zinc-800",
                if open() {
                    span { class: "text-sm font-semibold tracking-tight pl-1", "yard" }
                }
                button {
                    class: "ml-auto p-1.5 rounded-md text-zinc-500 hover:text-zinc-950 hover:bg-zinc-200/50 dark:text-zinc-400 dark:hover:text-zinc-50 dark:hover:bg-zinc-800 cursor-pointer transition-colors",
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
                SidebarLink {
                    to: Route::Dashboard {},
                    icon: "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1",
                    label: "Dashboard",
                    expanded: open(),
                    active: matches!(route, Route::Dashboard {}),
                }
                SidebarLink {
                    to: Route::Environments {},
                    icon: "M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z",
                    label: "Environments",
                    expanded: open(),
                    active: matches!(route, Route::Environments {} | Route::EnvironmentDetail { .. } | Route::RegionDetail { .. }),
                }
                SidebarLink {
                    to: Route::Jobs {},
                    icon: "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2",
                    label: "Jobs",
                    expanded: open(),
                    active: matches!(route, Route::Jobs {}),
                }
                SidebarLink {
                    to: Route::Drift { env: String::new() },
                    icon: "M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15",
                    label: "Drift",
                    expanded: open(),
                    active: matches!(route, Route::Drift { .. }),
                }
                SidebarLink {
                    to: Route::Settings {},
                    icon: "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z",
                    label: "Settings",
                    expanded: open(),
                    active: matches!(route, Route::Settings {}),
                }
            }
            // Sidebar footer: theme toggle
            div { class: "border-t border-zinc-200 dark:border-zinc-800 p-2",
                ThemeToggle { expanded: open() }
            }
        }
    }
}

#[component]
fn SidebarLink(
    to: Route,
    icon: &'static str,
    label: &'static str,
    expanded: bool,
    active: bool,
) -> Element {
    rsx! {
        div { class: "relative group",
            Link {
                to,
                class: format!(
                    "flex items-center gap-3 rounded-md text-sm cursor-pointer transition-colors {} {}",
                    if expanded { "px-2.5 py-1.5" } else { "justify-center p-2" },
                    if active {
                        "bg-zinc-200/75 text-zinc-950 font-medium dark:bg-zinc-800 dark:text-zinc-50"
                    } else {
                        "text-zinc-500 hover:text-zinc-950 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:text-zinc-50 dark:hover:bg-zinc-800"
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
                    class: "absolute left-full top-1/2 -translate-y-1/2 ml-2 px-2 py-1 rounded-md bg-zinc-950 text-zinc-50 dark:bg-zinc-50 dark:text-zinc-950 text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-50",
                    "{label}"
                }
            }
        }
    }
}

/// Compact sun/moon toggle in the sidebar footer. D-12: persists to localStorage,
/// toggles the `dark` CSS class on <html>. Uses dioxus document::eval to avoid
/// needing additional web-sys Cargo.toml features.
#[component]
fn ThemeToggle(expanded: bool) -> Element {
    let mut theme: Signal<Theme> = use_context();
    let is_dark = matches!(*theme.read(), Theme::Dark);
    let label = if is_dark { "Dark" } else { "Light" };

    rsx! {
        div { class: "relative group",
            button {
                class: format!(
                    "flex items-center gap-3 rounded-md text-sm cursor-pointer transition-colors w-full text-zinc-500 hover:text-zinc-950 hover:bg-zinc-100 dark:text-zinc-400 dark:hover:text-zinc-50 dark:hover:bg-zinc-800 {}",
                    if expanded { "px-2.5 py-1.5" } else { "justify-center p-2" }
                ),
                onclick: move |_| {
                    let new_theme = if is_dark { Theme::Light } else { Theme::Dark };
                    theme.set(new_theme);

                    // D-12: toggle dark class on <html> and persist to localStorage.
                    // T-44-07 mitigation: only write known enum values ("light", "dark")
                    // to classList and localStorage. Never pass raw user input.
                    #[cfg(target_arch = "wasm32")]
                    {
                        let theme_str = if matches!(new_theme, Theme::Dark) { "dark" } else { "light" };
                        let js = format!(
                            r#"
                            if ('{theme_str}' === 'dark') {{
                                document.documentElement.classList.add('dark');
                            }} else {{
                                document.documentElement.classList.remove('dark');
                            }}
                            localStorage.setItem('yard_theme', '{theme_str}');
                            "#
                        );
                        dioxus::prelude::document::eval(&js);
                    }
                },
                if is_dark {
                    // Moon icon
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
                        path { d: "M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z" }
                    }
                } else {
                    // Sun icon
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
                        circle { cx: "12", cy: "12", r: "5" }
                        line { x1: "12", y1: "1", x2: "12", y2: "3" }
                        line { x1: "12", y1: "21", x2: "12", y2: "23" }
                        line { x1: "4.22", y1: "4.22", x2: "5.64", y2: "5.64" }
                        line { x1: "18.36", y1: "18.36", x2: "19.78", y2: "19.78" }
                        line { x1: "1", y1: "12", x2: "3", y2: "12" }
                        line { x1: "21", y1: "12", x2: "23", y2: "12" }
                        line { x1: "4.22", y1: "19.78", x2: "5.64", y2: "18.36" }
                        line { x1: "18.36", y1: "5.64", x2: "19.78", y2: "4.22" }
                    }
                }
                if expanded {
                    span { "{label}" }
                }
            }
            if !expanded {
                div {
                    class: "absolute left-full top-1/2 -translate-y-1/2 ml-2 px-2 py-1 rounded-md bg-zinc-950 text-zinc-50 dark:bg-zinc-50 dark:text-zinc-950 text-xs whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity z-50",
                    "{label}"
                }
            }
        }
    }
}
