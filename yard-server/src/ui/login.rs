//! Operator login page (Gap A from 25-VERIFICATION.md §1).
//!
//! Renders a single password input + Sign In button. On submit:
//!   1. POST /api/auth/session with body { "token": "<typed value>" }
//!   2. The server (Plan 25-04) returns 200 + Set-Cookie: yard_session=...
//!      (HttpOnly + SameSite=Strict + Path=/ + Secure), or 401 on mismatch.
//!   3. On 200 navigate to / (Route::Dashboard).
//!   4. On 401 show inline error.
//!
//! The typed token is held in a Dioxus Signal<String> for the duration of
//! one submit cycle and is cleared (set to String::new()) immediately after
//! the POST resolves — success or failure. No persistence in JS-readable
//! storage; the only post-login token store is the browser's HttpOnly
//! cookie which WASM cannot read.

use dioxus::prelude::*;
use serde::Serialize;

use super::api_base;

#[derive(Serialize)]
struct SessionRequest<'a> {
    token: &'a str,
}

/// POST /api/auth/session with the typed token. Returns Ok(()) on 200,
/// Err with a user-facing message on 401 / network error.
async fn post_session(token: &str) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/api/auth/session", api_base()))
        .json(&SessionRequest { token })
        .send()
        .await
        .map_err(|e| format!("Network error: {e}"))?;

    match resp.status().as_u16() {
        200 => Ok(()),
        401 => Err("Invalid token — check the value and try again".to_string()),
        other => Err(format!("Unexpected server response: {other}")),
    }
}

#[component]
pub fn Login() -> Element {
    let mut token_input = use_signal(String::new);
    let mut error = use_signal(String::new);
    let mut submitting = use_signal(|| false);

    let on_submit = move |_evt: FormEvent| {
        // Capture the typed token, clear the input immediately so it's not
        // sitting in WASM memory longer than the one submit cycle.
        let typed = token_input();
        token_input.set(String::new());
        error.set(String::new());
        submitting.set(true);

        spawn(async move {
            match post_session(&typed).await {
                Ok(()) => {
                    // Drop the typed string from memory before navigating.
                    // (The local `typed` goes out of scope at the closure
                    // end; explicit drop below is belt-and-suspenders.)
                    drop(typed);
                    #[cfg(target_arch = "wasm32")]
                    {
                        use crate::Route;
                        use dioxus::prelude::navigator;
                        navigator().push(Route::Dashboard {});
                    }
                }
                Err(msg) => {
                    error.set(msg);
                }
            }
            submitting.set(false);
        });
    };

    rsx! {
        div { class: "min-h-screen flex items-center justify-center bg-zinc-50 dark:bg-zinc-950",
            form {
                class: "w-full max-w-sm rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 p-6 shadow-sm",
                onsubmit: on_submit,
                h1 { class: "text-base font-semibold mb-1 text-zinc-950 dark:text-zinc-50",
                    "yard-server"
                }
                p { class: "text-xs text-zinc-500 mb-4",
                    "Paste the YARD_API_TOKEN provided by your operator."
                }
                label { class: "text-xs font-medium text-zinc-500 dark:text-zinc-400 block mb-1.5",
                    "API token"
                }
                input {
                    r#type: "password",
                    autocomplete: "off",
                    autocapitalize: "off",
                    spellcheck: "false",
                    required: true,
                    class: "w-full px-3 py-1.5 text-sm rounded-md border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-800 dark:text-zinc-50 focus:outline-none focus:ring-2 focus:ring-zinc-300 dark:focus:ring-zinc-600 mb-3 font-mono",
                    value: "{token_input}",
                    oninput: move |e| token_input.set(e.value()),
                }
                if !error().is_empty() {
                    div { class: "rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700 mb-3",
                        "{error}"
                    }
                }
                button {
                    r#type: "submit",
                    disabled: submitting(),
                    class: "w-full px-3 py-1.5 text-sm font-medium rounded-md bg-zinc-900 text-white dark:bg-zinc-50 dark:text-zinc-900 hover:opacity-90 disabled:opacity-50 cursor-pointer disabled:cursor-not-allowed transition-opacity",
                    if submitting() { "Signing in..." } else { "Sign in" }
                }
            }
        }
    }
}
