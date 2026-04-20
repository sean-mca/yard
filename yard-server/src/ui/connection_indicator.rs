//! Connection status indicator — header-mounted icon dot.
//!
//! The component is native-compatible (SSR renders "Connecting" as the default
//! state). On wasm32 it consumes the `ConnectionCtx` provided by `Shell` via
//! `use_context`; on native no context exists so the default applies.

use dioxus::prelude::*;

/// Local mirror of `super::connection::ConnectionState`, compilable on native.
/// The wasm-only `connection` module defines the authoritative enum; this copy
/// ensures this file compiles on native too. Keep variants in lock-step.
///
/// On native, Shell does not provide a `ConnectionCtx` (the wasm-only types
/// can't exist here), so the internal `let state = ...;` branch always
/// resolves to `Connecting`. `Live` / `Offline` are only reached through the
/// wasm32 `cfg` branch that matches against `super::connection::ConnectionState`.
/// The narrow `dead_code` allow on native keeps clippy green without a cfg-gate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub enum ConnectionState {
    Live,
    Connecting,
    Offline,
}

#[component]
pub fn ConnectionIndicator() -> Element {
    // On wasm32, read the real state from the `ConnectionCtx` Shell provides.
    // On native, fall back to Connecting (SSR default).
    let state: ConnectionState = {
        #[cfg(target_arch = "wasm32")]
        {
            // Plan 05's Shell provides ConnectionCtx unconditionally (per RESEARCH.md Pitfall 3).
            // A bare use_context call panics if unprovided, which is the desired fail-fast behaviour.
            let ctx: super::connection::ConnectionCtx = use_context();
            match *ctx.state.read() {
                super::connection::ConnectionState::Live => ConnectionState::Live,
                super::connection::ConnectionState::Connecting => ConnectionState::Connecting,
                super::connection::ConnectionState::Offline => ConnectionState::Offline,
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            ConnectionState::Connecting
        }
    };

    let (dot_class, anim_class, tooltip): (&'static str, &'static str, &'static str) = match state {
        ConnectionState::Live => (
            "bg-emerald-500 dark:bg-emerald-400",
            "",
            "Live — real-time updates active",
        ),
        ConnectionState::Connecting => (
            "bg-amber-400 dark:bg-amber-300",
            "animate-pulse",
            "Connecting — reconnecting to server",
        ),
        ConnectionState::Offline => (
            "bg-zinc-400 dark:bg-zinc-600",
            "",
            "Offline — using polling fallback",
        ),
    };

    rsx! {
        span {
            class: "inline-flex items-center justify-center h-8 w-8 rounded-md cursor-default",
            title: "{tooltip}",
            aria_label: "{tooltip}",
            span { class: format!("w-2 h-2 rounded-full {dot_class} {anim_class}") }
        }
    }
}
