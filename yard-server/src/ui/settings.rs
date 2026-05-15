use dioxus::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::api_base;
use super::fetch::{get_json, post_json, post_no_body};

#[derive(Clone, Copy, PartialEq)]
pub enum Theme {
    Light,
    Dark,
    System,
}

impl Theme {
    fn from_str(s: &str) -> Self {
        match s {
            "dark" => Theme::Dark,
            "system" => Theme::System,
            _ => Theme::Light,
        }
    }
}

#[derive(Deserialize, Default)]
struct SettingsResponse {
    settings: HashMap<String, String>,
}

async fn fetch_settings() -> Result<HashMap<String, String>, String> {
    // 401-redirect, status, and parse-error handling are centralised in
    // ui::fetch::get_json (Plan 25-05 Gap A).
    let body =
        get_json::<SettingsResponse>(&format!("{}/api/settings", api_base())).await?;
    Ok(body.settings)
}

#[derive(Serialize)]
struct SettingsPayload {
    settings: HashMap<String, String>,
}

async fn save_setting(key: &str, value: &str) -> Result<(), String> {
    let mut settings = HashMap::new();
    settings.insert(key.to_string(), value.to_string());
    // 401-redirect, status handling are centralised in
    // ui::fetch::post_json (Plan 25-05 Gap A).
    post_json(
        &format!("{}/api/settings", api_base()),
        &SettingsPayload { settings },
    )
    .await
}

/// POST /api/auth/logout to clear the yard_session cookie. Server returns
/// 204 + Set-Cookie: yard_session=; Max-Age=0, which the browser uses to
/// drop the cookie immediately. Caller is responsible for navigating to
/// /login afterwards.
async fn post_logout() -> Result<(), String> {
    post_no_body(&format!("{}/api/auth/logout", api_base())).await
}

// ---- Types for auth providers display ----

#[derive(Deserialize, Default)]
struct ProvidersResponse {
    providers: Vec<ProviderInfo>,
}

#[derive(Deserialize, Clone)]
struct ProviderInfo {
    #[allow(dead_code)]
    id: String,
    name: String,
}

#[component]
pub fn Settings(theme: Signal<Theme>) -> Element {
    let mut drift_interval = use_signal(|| "3".to_string());
    let mut slack_arn = use_signal(String::new);
    let mut slack_enabled = use_signal(|| false);
    let mut alert_threshold = use_signal(String::new);
    let mut alert_cooldown = use_signal(String::new);
    let mut loaded = use_signal(|| false);

    // Load settings from API on mount
    use_effect(move || {
        spawn(async move {
            if let Ok(settings) = fetch_settings().await {
                if let Some(t) = settings.get("theme") {
                    theme.set(Theme::from_str(t));
                }
                if let Some(v) = settings.get("drift_interval") {
                    drift_interval.set(v.clone());
                }
                if let Some(v) = settings.get("slack_webhook_secret_arn") {
                    slack_arn.set(v.clone());
                }
                if let Some(v) = settings.get("slack_enabled") {
                    slack_enabled.set(v == "true");
                }
                if let Some(v) = settings.get("alert_drift_threshold") {
                    alert_threshold.set(v.clone());
                }
                if let Some(v) = settings.get("alert_cooldown_minutes") {
                    alert_cooldown.set(v.clone());
                }
            }
            loaded.set(true);
        });
    });

    rsx! {
        div { class: "p-6 max-w-2xl",
            div { class: "space-y-8",
                // Appearance
                SettingsSection {
                    title: "Appearance",
                    description: "Customize the look of the dashboard.",
                }
                ThemePicker { theme, loaded }

                Divider {}

                // Drift polling
                SettingsSection {
                    title: "Drift Detection",
                    description: "Configure how often yard checks for configuration drift.",
                }
                IntervalPicker { value: drift_interval, loaded }

                Divider {}

                // Notifications
                SettingsSection {
                    title: "Notifications",
                    description: "Get alerted when drift is detected or plans fail.",
                }

                // Slack
                NotificationCard {
                    label: "Slack",
                    description: "Post to a Slack channel via incoming webhook. The URL is loaded from AWS Secrets Manager — supply the secret ARN here. See docs/server.md.",
                    icon: "M14.5 10c-.83 0-1.5-.67-1.5-1.5v-5c0-.83.67-1.5 1.5-1.5s1.5.67 1.5 1.5v5c0 .83-.67 1.5-1.5 1.5zm-5 8c-.83 0-1.5-.67-1.5-1.5v-5c0-.83.67-1.5 1.5-1.5s1.5.67 1.5 1.5v5c0 .83-.67 1.5-1.5 1.5z",
                    enabled: slack_enabled,
                    field_label: "Secret ARN",
                    field_placeholder: "arn:aws:secretsmanager:us-east-1:123456789012:secret:yard/slack-webhook-AbCdEf",
                    field_value: slack_arn,
                    loaded,
                }

                // Alert threshold + cooldown (always visible per D-09)
                div { class: "rounded-lg border border-zinc-200 dark:border-zinc-700 p-4 mt-3",
                    div { class: "space-y-3",
                        div {
                            label { class: "text-xs font-medium text-zinc-500 block mb-1.5", "Alert threshold (jobs)" }
                            input {
                                r#type: "number",
                                min: "1",
                                placeholder: "1",
                                class: "w-full px-3 py-1.5 text-sm rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 focus:outline-none focus:ring-2 focus:ring-zinc-300 dark:focus:ring-zinc-600",
                                value: "{alert_threshold}",
                                oninput: move |e| alert_threshold.set(e.value()),
                                onchange: move |e| {
                                    let val = e.value();
                                    if loaded() {
                                        spawn(async move {
                                            let _ = save_setting("alert_drift_threshold", &val).await;
                                        });
                                    }
                                },
                            }
                        }
                        div {
                            label { class: "text-xs font-medium text-zinc-500 block mb-1.5", "Cooldown (minutes)" }
                            input {
                                r#type: "number",
                                min: "1",
                                placeholder: "10",
                                class: "w-full px-3 py-1.5 text-sm rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 focus:outline-none focus:ring-2 focus:ring-zinc-300 dark:focus:ring-zinc-600",
                                value: "{alert_cooldown}",
                                oninput: move |e| alert_cooldown.set(e.value()),
                                onchange: move |e| {
                                    let val = e.value();
                                    if loaded() {
                                        spawn(async move {
                                            let _ = save_setting("alert_cooldown_minutes", &val).await;
                                        });
                                    }
                                },
                            }
                        }
                    }
                }

                Divider {}

                // Authentication (Phase 45 Plan 04)
                SettingsSection {
                    title: "Authentication",
                    description: "Current authentication configuration.",
                }
                AuthStatusCard {}

                Divider {}

                // Sign Out (Plan 25-05 Gap A — closes "no Sign Out button"
                // tech-debt). POSTs /api/auth/logout (clears yard_session
                // cookie via Set-Cookie: Max-Age=0), then navigates the
                // browser to /login on WASM target.
                SettingsSection {
                    title: "Session",
                    description: "Sign out of the dashboard. CLI / automation callers using Authorization: Bearer are unaffected.",
                }
                SignOutButton {}

            }
        }
    }
}

#[component]
fn SignOutButton() -> Element {
    let mut signing_out = use_signal(|| false);
    let mut error = use_signal(String::new);

    rsx! {
        div { class: "mt-3",
            button {
                r#type: "button",
                disabled: signing_out(),
                class: "px-3 py-1.5 text-sm font-medium rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 text-zinc-700 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-800 disabled:opacity-50 cursor-pointer disabled:cursor-not-allowed transition-colors",
                onclick: move |_| {
                    signing_out.set(true);
                    error.set(String::new());
                    spawn(async move {
                        match post_logout().await {
                            Ok(()) => {
                                #[cfg(target_arch = "wasm32")]
                                {
                                    use crate::Route;
                                    use dioxus::prelude::navigator;
                                    navigator().push(Route::Login {});
                                }
                            }
                            Err(msg) => {
                                error.set(format!("Sign out failed: {msg}"));
                            }
                        }
                        signing_out.set(false);
                    });
                },
                if signing_out() { "Signing out..." } else { "Sign out" }
            }
            if !error().is_empty() {
                div { class: "mt-2 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700",
                    "{error}"
                }
            }
        }
    }
}

#[component]
fn SettingsSection(title: &'static str, description: &'static str) -> Element {
    rsx! {
        div {
            h2 { class: "text-sm font-semibold", "{title}" }
            p { class: "text-xs text-zinc-500 mt-0.5", "{description}" }
        }
    }
}

#[component]
fn Divider() -> Element {
    rsx! { hr { class: "border-zinc-200 dark:border-zinc-700" } }
}

#[component]
fn ThemePicker(mut theme: Signal<Theme>, loaded: Signal<bool>) -> Element {
    rsx! {
        div { class: "flex gap-2 mt-3",
            ThemeOption {
                label: "Light",
                active: theme() == Theme::Light,
                icon: "M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z",
                on_click: move |_| {
                    theme.set(Theme::Light);
                    if loaded() {
                        spawn(async move { let _ = save_setting("theme", "light").await; });
                    }
                },
            }
            ThemeOption {
                label: "Dark",
                active: theme() == Theme::Dark,
                icon: "M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z",
                on_click: move |_| {
                    theme.set(Theme::Dark);
                    if loaded() {
                        spawn(async move { let _ = save_setting("theme", "dark").await; });
                    }
                },
            }
            ThemeOption {
                label: "System",
                active: theme() == Theme::System,
                icon: "M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z",
                on_click: move |_| {
                    theme.set(Theme::System);
                    if loaded() {
                        spawn(async move { let _ = save_setting("theme", "system").await; });
                    }
                },
            }
        }
    }
}

#[component]
fn ThemeOption(
    label: &'static str,
    active: bool,
    icon: &'static str,
    on_click: EventHandler<MouseEvent>,
) -> Element {
    rsx! {
        button {
            class: format!(
                "flex items-center gap-2 px-4 py-2 rounded-lg border text-sm cursor-pointer transition-colors {}",
                if active {
                    "border-zinc-900 bg-zinc-900 text-white dark:border-white dark:bg-white dark:text-zinc-900"
                } else {
                    "border-zinc-200 text-zinc-600 hover:bg-zinc-50 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
                }
            ),
            onclick: move |e| on_click.call(e),
            svg {
                xmlns: "http://www.w3.org/2000/svg",
                width: "16", height: "16",
                view_box: "0 0 24 24",
                fill: "none",
                stroke: "currentColor",
                stroke_width: "2",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                path { d: "{icon}" }
            }
            "{label}"
        }
    }
}

#[component]
fn IntervalPicker(mut value: Signal<String>, loaded: Signal<bool>) -> Element {
    let options = [("1", "1 min"), ("3", "3 min"), ("5", "5 min"), ("10", "10 min")];

    rsx! {
        div { class: "mt-3",
            p { class: "text-xs font-medium text-zinc-500 mb-2", "Check interval" }
            div { class: "flex gap-2",
                for (val, label) in options {
                    {
                        let is_active = value() == val;
                        rsx! {
                            button {
                                class: format!(
                                    "px-3 py-1.5 rounded-md border text-xs font-medium cursor-pointer transition-colors {}",
                                    if is_active {
                                        "border-violet-600 bg-violet-500/10 text-violet-600 font-semibold dark:border-violet-400 dark:bg-violet-500/15 dark:text-violet-400"
                                    } else {
                                        "border-zinc-200 text-zinc-600 hover:bg-zinc-50 dark:border-zinc-700 dark:text-zinc-400 dark:hover:bg-zinc-800"
                                    }
                                ),
                                onclick: move |_| {
                                    value.set(val.to_string());
                                    if loaded() {
                                        spawn(async move { let _ = save_setting("drift_interval", val).await; });
                                    }
                                },
                                "{label}"
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn NotificationCard(
    label: &'static str,
    description: &'static str,
    icon: &'static str,
    mut enabled: Signal<bool>,
    field_label: &'static str,
    field_placeholder: &'static str,
    mut field_value: Signal<String>,
    loaded: Signal<bool>,
) -> Element {
    let testing = use_signal(|| false);
    let test_result: Signal<Option<Result<(), String>>> = use_signal(|| None);

    rsx! {
        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-700 p-4 mt-3",
            div { class: "flex items-center justify-between",
                div { class: "flex items-center gap-3",
                    svg {
                        xmlns: "http://www.w3.org/2000/svg",
                        width: "18", height: "18",
                        view_box: "0 0 24 24",
                        fill: "none",
                        stroke: "currentColor",
                        stroke_width: "2",
                        stroke_linecap: "round",
                        stroke_linejoin: "round",
                        class: "text-zinc-400",
                        path { d: "{icon}" }
                    }
                    div {
                        p { class: "text-sm font-medium", "{label}" }
                        p { class: "text-xs text-zinc-500", "{description}" }
                    }
                }
                // Toggle
                button {
                    class: format!(
                        "relative w-9 h-5 rounded-full cursor-pointer transition-colors {}",
                        if enabled() {
                            "bg-zinc-900 dark:bg-white"
                        } else {
                            "bg-zinc-200 dark:bg-zinc-700"
                        }
                    ),
                    onclick: move |_| {
                        enabled.toggle();
                        if loaded() {
                            let val = enabled().to_string();
                            spawn(async move { let _ = save_setting("slack_enabled", &val).await; });
                        }
                    },
                    span {
                        class: format!(
                            "absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white dark:bg-zinc-900 transition-transform {}",
                            if enabled() { "translate-x-4" } else { "" }
                        ),
                    }
                }
            }
            if enabled() {
                div { class: "mt-3 pt-3 border-t border-zinc-100 dark:border-zinc-700",
                    label { class: "text-xs font-medium text-zinc-500 block mb-1.5", "{field_label}" }
                    input {
                        r#type: "text",
                        placeholder: "{field_placeholder}",
                        class: "w-full px-3 py-1.5 text-sm rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 focus:outline-none focus:ring-2 focus:ring-zinc-300 dark:focus:ring-zinc-600",
                        value: "{field_value}",
                        oninput: move |e| field_value.set(e.value()),
                        onchange: move |e| {
                            let val = e.value();
                            if loaded() {
                                spawn(async move { let _ = save_setting("slack_webhook_secret_arn", &val).await; });
                            }
                        },
                    }
                    // Slack test button (Phase 45 Plan 04 — D-13)
                    SlackTestButton { testing, test_result }
                }
            }
        }
    }
}

/// Slack webhook test button (D-13). Sends a test message via the
/// `test_slack_webhook` settings action trigger. Shows Testing.../Sent/error
/// states inline.
#[component]
fn SlackTestButton(
    mut testing: Signal<bool>,
    mut test_result: Signal<Option<Result<(), String>>>,
) -> Element {
    rsx! {
        div { class: "mt-3",
            div { class: "flex items-center gap-2",
                button {
                    r#type: "button",
                    disabled: testing(),
                    class: "px-3 py-1.5 text-xs font-medium rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 text-zinc-700 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-800 cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed transition-colors",
                    onclick: move |_| {
                        testing.set(true);
                        test_result.set(None);
                        spawn(async move {
                            match save_setting("test_slack_webhook", "").await {
                                Ok(()) => {
                                    test_result.set(Some(Ok(())));
                                    // Clear success after 3 seconds.
                                    spawn(async move {
                                        #[cfg(target_arch = "wasm32")]
                                        {
                                            gloo_timers::future::TimeoutFuture::new(3_000).await;
                                        }
                                        #[cfg(not(target_arch = "wasm32"))]
                                        {
                                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                                        }
                                        test_result.set(None);
                                    });
                                }
                                Err(msg) => {
                                    test_result.set(Some(Err(msg)));
                                }
                            }
                            testing.set(false);
                        });
                    },
                    if testing() {
                        "Testing..."
                    } else {
                        "Test"
                    }
                }
                // Inline success indicator
                if let Some(Ok(())) = test_result() {
                    div { class: "flex items-center gap-1 text-xs text-emerald-500",
                        svg {
                            xmlns: "http://www.w3.org/2000/svg",
                            width: "14",
                            height: "14",
                            view_box: "0 0 24 24",
                            fill: "none",
                            stroke: "currentColor",
                            stroke_width: "2",
                            stroke_linecap: "round",
                            stroke_linejoin: "round",
                            path { d: "M20 6L9 17l-5-5" }
                        }
                        "Sent"
                    }
                }
            }
            // Inline error display
            if let Some(Err(msg)) = test_result() {
                div { class: "mt-2 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-300",
                    "Test failed: {msg}"
                }
            }
        }
    }
}

/// Authentication status card (D-11). Fetches /api/auth/providers on mount
/// and displays the current auth mode + active provider names. Read-only.
#[component]
fn AuthStatusCard() -> Element {
    let mut auth_mode = use_signal(String::new);
    let mut provider_names: Signal<Vec<String>> = use_signal(Vec::new);
    let mut loaded = use_signal(|| false);

    use_effect(move || {
        spawn(async move {
            match get_json::<ProvidersResponse>(&format!(
                "{}/api/auth/providers",
                api_base()
            ))
            .await
            {
                Ok(resp) => {
                    if resp.providers.is_empty() {
                        auth_mode.set("No authentication (VPN-only)".to_string());
                    } else {
                        auth_mode.set("OAuth2 SSO".to_string());
                        provider_names
                            .set(resp.providers.into_iter().map(|p| p.name).collect());
                    }
                }
                Err(_) => {
                    auth_mode.set("Unknown".to_string());
                }
            }
            loaded.set(true);
        });
    });

    if !loaded() {
        return rsx! {};
    }

    rsx! {
        div { class: "rounded-lg border border-zinc-200 dark:border-zinc-700 p-4 mt-3",
            p { class: "text-sm font-medium",
                "Mode: {auth_mode}"
            }
            if !provider_names().is_empty() {
                div { class: "mt-2",
                    p { class: "text-xs text-zinc-500 mb-1", "Active providers:" }
                    ul { class: "text-xs text-zinc-500 list-disc list-inside",
                        for name in provider_names() {
                            li { "{name}" }
                        }
                    }
                }
            }
        }
    }
}
