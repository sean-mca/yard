//! WASM-only WebSocket client for real-time updates.
//!
//! NOTE: The server declares the canonical `Event` enum in `api/events.rs`
//! (derives `Serialize`). This module's mirror `Event` derives `Deserialize`.
//! Variant names and fields MUST stay in lock-step between the two files.
//!
//! This module is gated at the module declaration site (`#[cfg(target_arch = "wasm32")] pub mod connection;` in `ui/mod.rs`)
//! so the native test build ignores it entirely (it depends on `gloo-net`, `gloo-timers`,
//! `futures-util`, which are WASM-only per Cargo.toml).

use std::time::Duration;

use dioxus::prelude::*;
use futures_util::StreamExt;
use gloo_net::websocket::Message;
use gloo_net::websocket::futures::WebSocket;
use gloo_timers::future::sleep;

/// Visible connection lifecycle states. Pairs with UI-SPEC State-to-Style
/// Contract — color, pulse, and tooltip copy are determined by state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ConnectionState {
    /// WebSocket open; polling paused.
    Live,
    /// WebSocket opening or in backoff between retries.
    Connecting,
    /// Closed without an active reconnect attempt in flight (transient — the
    /// reconnect loop moves back to Connecting before the next attempt).
    Offline,
}

/// Shared context provided at `Shell` scope. Consumed by `ConnectionIndicator`
/// (for state) and by dashboard/drift pages (for per-topic ticks that drive
/// query invalidation).
#[derive(Clone, Copy)]
pub struct ConnectionCtx {
    pub state: Signal<ConnectionState>,
    pub dashboard_tick: Signal<u64>,
    pub drift_tick: Signal<u64>,
}

/// Client mirror of the server's `api::events::Event` enum. Keep variants in
/// lock-step. Serialised shape (matches server): `{"event":"<snake_case>", ...}`.
#[derive(serde::Deserialize, Clone, Debug)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    DriftRefreshed,
    DriftFailed { reason: String },
    DashboardRefreshed,
    DashboardFailed { reason: String },
    WebhookReceived,
}

/// Build the WebSocket URL for the events endpoint.
///
/// Production mode (`YARD_API_BASE=""`): derive host from `window().location()`,
/// choosing `wss` if current protocol is HTTPS else `ws`.
///
/// Dev mode (`YARD_API_BASE="http://127.0.0.1:3001"`): swap `http://` → `ws://`
/// and `https://` → `wss://`, append `/api/ws/events`.
pub fn ws_url() -> String {
    let base = crate::ui::api_base();
    if base.is_empty() {
        if let Some(win) = web_sys::window() {
            let loc = win.location();
            let proto = if loc.protocol().unwrap_or_default() == "https:" {
                "wss"
            } else {
                "ws"
            };
            let host = loc.host().unwrap_or_default();
            return format!("{proto}://{host}/api/ws/events");
        }
        // Extremely unlikely — return a relative-ish string that will fail open
        // visibly rather than silently connecting to the wrong place.
        return "/api/ws/events".to_string();
    }
    let swapped = base
        .replacen("https://", "wss://", 1)
        .replacen("http://", "ws://", 1);
    format!("{swapped}/api/ws/events")
}

/// Spawn the reconnecting WebSocket task. Runs for the lifetime of the Dioxus
/// scope that created the `state` signal. Owned state transitions:
///   - set Connecting before each `WebSocket::open` attempt
///   - set Live on successful open, reset backoff to 1s
///   - set Offline on close/error, sleep `backoff` seconds, retry
///   - backoff doubles 1→2→4→8→16→30 (capped)
///
/// On each deserialised `Event`, invokes `on_event(event)`. The caller wires
/// ConnectionCtx tick signals into `on_event` so page-level `use_effect`
/// watchers fire.
pub fn spawn_ws_task(mut state: Signal<ConnectionState>, on_event: impl Fn(Event) + 'static) {
    spawn(async move {
        let mut backoff_secs: u64 = 1;
        loop {
            state.set(ConnectionState::Connecting);
            match WebSocket::open(&ws_url()) {
                Ok(mut ws) => {
                    state.set(ConnectionState::Live);
                    backoff_secs = 1;
                    while let Some(msg) = ws.next().await {
                        match msg {
                            Ok(Message::Text(txt)) => {
                                if let Ok(event) = serde_json::from_str::<Event>(&txt) {
                                    on_event(event);
                                }
                            }
                            Ok(Message::Bytes(_)) => {
                                // Server only sends text frames; ignore.
                            }
                            Err(_) => break,
                        }
                    }
                    // Ignore close errors — we're about to reconnect anyway.
                    let _ = ws.close(None, None).await;
                }
                Err(_) => {
                    // Fall through to the backoff sleep below.
                }
            }
            state.set(ConnectionState::Offline);
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
        }
    });
}
