//! OAuth2 login page (Phase 45 Plan 04).
//!
//! On mount, fetches GET /api/auth/providers. If the list is empty (NoopAuth
//! mode), redirects to Dashboard immediately — the login page is never shown.
//! If providers exist, renders one `ProviderButton` per configured provider.
//! Clicking a button navigates the full browser to
//! `/api/auth/oauth/start?provider={id}` which triggers the server-side PKCE
//! flow. Error display handles OAuth callback failures via the `?error=1`
//! query parameter set by the callback redirect on failure.

use dioxus::prelude::*;
use serde::Deserialize;

use super::api_base;
use super::fetch::get_json;

#[derive(Deserialize, Default)]
struct ProvidersResponse {
    providers: Vec<ProviderInfo>,
}

#[derive(Deserialize, Clone)]
struct ProviderInfo {
    id: String,
    name: String,
}

#[component]
pub fn Login() -> Element {
    let mut error = use_signal(String::new);
    let submitting = use_signal(|| false);
    let mut providers: Signal<Vec<ProviderInfo>> = use_signal(Vec::new);
    let mut loaded = use_signal(|| false);

    // Fetch providers + check for callback error on mount.
    use_effect(move || {
        spawn(async move {
            // Check URL query param "error" for callback failures.
            #[cfg(target_arch = "wasm32")]
            {
                if let Some(window) = web_sys::window() {
                    if let Ok(search) = window.location().search() {
                        if search.contains("error") {
                            error.set(
                                "Sign-in could not be completed. Please try again.".to_string(),
                            );
                        }
                    }
                }
            }

            match get_json::<ProvidersResponse>(&format!(
                "{}/api/auth/providers",
                api_base()
            ))
            .await
            {
                Ok(resp) => {
                    if resp.providers.is_empty() {
                        // NoopAuth — redirect to dashboard immediately.
                        #[cfg(target_arch = "wasm32")]
                        {
                            use crate::Route;
                            use dioxus::prelude::navigator;
                            navigator().push(Route::Dashboard {});
                        }
                    } else {
                        providers.set(resp.providers);
                    }
                }
                Err(msg) => {
                    error.set(format!("Failed to load providers: {msg}"));
                }
            }
            loaded.set(true);
        });
    });

    // Before providers load, render nothing (brief flash).
    if !loaded() {
        return rsx! {};
    }

    // NoopAuth handled above via redirect. If we get here with empty
    // providers, the non-WASM build path just renders nothing.
    if providers().is_empty() {
        return rsx! {};
    }

    rsx! {
        div { class: "min-h-screen flex items-center justify-center bg-zinc-50 dark:bg-zinc-950",
            div {
                class: "w-full max-w-sm rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 p-6 shadow-sm",
                h1 { class: "text-base font-semibold mb-1 text-zinc-950 dark:text-zinc-50",
                    "yard-server"
                }
                p { class: "text-xs text-zinc-500 mb-4",
                    "Sign in to continue."
                }
                if !error().is_empty() {
                    div { class: "rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 mb-3 dark:border-red-800 dark:bg-red-950 dark:text-red-300",
                        "{error}"
                    }
                }
                div { class: "space-y-2",
                    for provider in providers() {
                        ProviderButton {
                            key: "{provider.id}",
                            name: provider.name.clone(),
                            provider_id: provider.id.clone(),
                            submitting,
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn ProviderButton(
    name: String,
    provider_id: String,
    submitting: Signal<bool>,
) -> Element {
    let display_name = name.clone();
    let pid = provider_id.clone();

    rsx! {
        button {
            r#type: "button",
            disabled: submitting(),
            class: "w-full flex items-center gap-3 px-4 py-2.5 text-sm font-medium rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 text-zinc-700 dark:text-zinc-300 hover:bg-zinc-50 dark:hover:bg-zinc-800 disabled:opacity-50 cursor-pointer disabled:cursor-not-allowed transition-colors",
            onclick: move |_| {
                submitting.set(true);
                let _provider = pid.clone();
                #[cfg(target_arch = "wasm32")]
                {
                    let base = api_base();
                    let url = format!("{base}/api/auth/oauth/start?provider={_provider}");
                    if let Some(window) = web_sys::window() {
                        let _ = window.location().set_href(&url);
                    }
                }
            },
            // Generic shield/key icon (16x16)
            svg {
                xmlns: "http://www.w3.org/2000/svg",
                width: "16",
                height: "16",
                view_box: "0 0 24 24",
                fill: "none",
                stroke: "currentColor",
                stroke_width: "2",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                path { d: "M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" }
            }
            if submitting() {
                "Signing in with {display_name}..."
            } else {
                "Sign in with {display_name}"
            }
        }
    }
}
