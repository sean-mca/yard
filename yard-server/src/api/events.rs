//! WebSocket event bus: tagged Event enum + broadcast channel factory.
//!
//! NOTE: The WASM client declares a mirror `Event` enum in `ui/connection.rs`
//! (derives `Deserialize` instead of `Serialize`). Variant names and fields
//! MUST stay in lock-step between the two files.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::any;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, info, warn};

use crate::api::dashboard::ApiState;

/// Max chars retained in a failure `reason` string before truncation with `…`.
/// Prevents leaking long GitHub error bodies / stack traces over the WS wire.
#[allow(dead_code)] // Consumed via `sanitize_reason` — flagged until Plan 02 wires emission sites.
const REASON_MAX_CHARS: usize = 200;

/// Broadcast channel capacity. Small on purpose (see CONTEXT.md D-05):
/// lagged subscribers are force-closed so they reconnect and re-fetch caches fresh.
pub const EVENT_CHANNEL_CAPACITY: usize = 64;

/// Server → client WebSocket event. Each variant is a "refresh now" signal
/// (see CONTEXT.md D-01 / D-02). Serialised as `{"event":"<snake_case>", ...fields}`.
#[allow(dead_code)] // Variants constructed by Plan 02 emission sites; staged here for the type contract.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    DriftRefreshed,
    DriftFailed { reason: String },
    DashboardRefreshed,
    DashboardFailed { reason: String },
    WebhookReceived,
    AlertSent { drifted_count: u32 },
    EnvironmentHealthChanged,
}

/// Truncate a failure-reason string to at most `REASON_MAX_CHARS` characters,
/// replacing the last char with `…` when truncation occurs. Counts chars
/// (Unicode scalar values), not bytes, so multi-byte input behaves sensibly.
#[allow(dead_code)] // Called by Plan 02 emission sites before constructing failed-event variants.
pub fn sanitize_reason(s: &str) -> String {
    let char_count = s.chars().count();
    if char_count <= REASON_MAX_CHARS {
        return s.to_string();
    }
    let mut out: String = s.chars().take(REASON_MAX_CHARS - 1).collect();
    out.push('…');
    out
}

/// Construct a broadcast channel for `Event`. Caller keeps `Sender`; the initial
/// `Receiver` is typically dropped — new subscribers use `Sender::subscribe()`.
pub fn new_event_channel() -> (broadcast::Sender<Event>, broadcast::Receiver<Event>) {
    broadcast::channel(EVENT_CHANNEL_CAPACITY)
}

/// Construct the Axum sub-router for WebSocket real-time updates.
///
/// Mounts `/api/ws/events`. Inherits CORS + rate-limit + tracing layers from the
/// main router (see `main.rs::start_api_server`). The rate limiter applies to
/// the upgrade *handshake* only; once upgraded, frames flow freely per D-09.
pub fn events_router(state: Arc<ApiState>) -> Router {
    Router::new()
        .route("/api/ws/events", any(ws_handler))
        .with_state(state)
}

/// Upgrade extractor handler. Any HTTP verb that arrives with the WS upgrade
/// header will hit this path — `axum::routing::any` is the idiomatic choice.
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<ApiState>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Per-connection event loop. Subscribes a fresh `broadcast::Receiver`,
/// forwards events as JSON text frames, closes on lag, and ignores all
/// inbound client payloads except Close (see T-07-03 threat model).
async fn handle_socket(mut socket: WebSocket, state: Arc<ApiState>) {
    let mut rx = state.event_tx.subscribe();
    info!("WebSocket client connected");

    loop {
        tokio::select! {
            recv = rx.recv() => match recv {
                Ok(event) => {
                    let payload = match serde_json::to_string(&event) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(error = %e, "Failed to serialize event");
                            continue;
                        }
                    };
                    if socket.send(Message::Text(payload.into())).await.is_err() {
                        debug!("WebSocket sink closed, exiting loop");
                        break;
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    warn!(dropped = n, "WebSocket subscriber lagged; closing to force client reconnect");
                    let _ = socket.send(Message::Close(None)).await;
                    break;
                }
                Err(RecvError::Closed) => {
                    debug!("Event channel closed, terminating WebSocket");
                    break;
                }
            },
            msg = socket.recv() => match msg {
                None => {
                    debug!("WebSocket stream ended");
                    break;
                }
                Some(Ok(Message::Close(_))) => {
                    debug!("WebSocket client sent Close");
                    break;
                }
                Some(Ok(_)) => {
                    // Ignore Text / Binary / Ping / Pong — T-07-03: no inbound parsing.
                    // tungstenite auto-responds to Ping with Pong; we do nothing.
                }
                Some(Err(e)) => {
                    warn!(error = %e, "WebSocket read error");
                    break;
                }
            },
        }
    }

    info!("WebSocket client disconnected");
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[test]
    fn event_drift_refreshed_serializes_tagged() {
        let json = serde_json::to_string(&Event::DriftRefreshed).unwrap();
        assert_eq!(json, r#"{"event":"drift_refreshed"}"#);
    }

    #[test]
    fn event_dashboard_refreshed_serializes_tagged() {
        let json = serde_json::to_string(&Event::DashboardRefreshed).unwrap();
        assert_eq!(json, r#"{"event":"dashboard_refreshed"}"#);
    }

    #[test]
    fn event_webhook_received_serializes_tagged() {
        let json = serde_json::to_string(&Event::WebhookReceived).unwrap();
        assert_eq!(json, r#"{"event":"webhook_received"}"#);
    }

    #[test]
    fn event_alert_sent_serializes_with_count() {
        let json = serde_json::to_string(&Event::AlertSent { drifted_count: 5 }).unwrap();
        assert_eq!(json, r#"{"event":"alert_sent","drifted_count":5}"#);
    }

    #[test]
    fn event_alert_sent_serializes_with_zero_count() {
        // Edge case — the variant is constructed from drift_data.drifted (u32),
        // which could theoretically be 0 if the evaluator were bypassed. Verify
        // the shape is stable.
        let json = serde_json::to_string(&Event::AlertSent { drifted_count: 0 }).unwrap();
        assert_eq!(json, r#"{"event":"alert_sent","drifted_count":0}"#);
    }

    #[test]
    fn event_drift_failed_serializes_with_reason() {
        let json = serde_json::to_string(&Event::DriftFailed {
            reason: "boom".into(),
        })
        .unwrap();
        assert_eq!(json, r#"{"event":"drift_failed","reason":"boom"}"#);
    }

    #[test]
    fn event_dashboard_failed_serializes_with_reason() {
        let json = serde_json::to_string(&Event::DashboardFailed {
            reason: "x".into(),
        })
        .unwrap();
        assert_eq!(json, r#"{"event":"dashboard_failed","reason":"x"}"#);
    }

    #[test]
    fn event_environment_health_changed_serializes_tagged() {
        let json = serde_json::to_string(&Event::EnvironmentHealthChanged).unwrap();
        assert_eq!(json, r#"{"event":"environment_health_changed"}"#);
    }

    #[test]
    fn sanitize_reason_truncates_long_input_with_ellipsis() {
        let long = "a".repeat(250);
        let out = sanitize_reason(&long);
        assert_eq!(out.chars().count(), REASON_MAX_CHARS);
        assert!(out.ends_with('…'));
        assert_eq!(
            out.chars().filter(|&c| c == 'a').count(),
            REASON_MAX_CHARS - 1
        );
    }

    #[test]
    fn sanitize_reason_leaves_short_input_unchanged() {
        let s = "short";
        assert_eq!(sanitize_reason(s), "short");
    }

    #[test]
    fn sanitize_reason_boundary_at_max_chars_unchanged() {
        let boundary = "a".repeat(REASON_MAX_CHARS);
        let out = sanitize_reason(&boundary);
        assert_eq!(out.chars().count(), REASON_MAX_CHARS);
        assert_eq!(out, boundary);
    }

    #[tokio::test]
    async fn broadcast_round_trip_delivers_event() {
        let (tx, mut rx) = new_event_channel();
        let _ = tx.send(Event::DriftRefreshed);
        let got = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv err");
        assert!(matches!(got, Event::DriftRefreshed));
    }

    #[tokio::test]
    async fn broadcast_late_subscriber_receives_after_initial_drop() {
        let (tx, _rx) = new_event_channel();
        drop(_rx);
        let mut late_rx = tx.subscribe();
        let _ = tx.send(Event::WebhookReceived);
        let got = timeout(Duration::from_millis(100), late_rx.recv())
            .await
            .expect("recv timed out")
            .expect("recv err");
        assert!(matches!(got, Event::WebhookReceived));
    }

    #[test]
    fn event_channel_capacity_matches_constant() {
        assert_eq!(EVENT_CHANNEL_CAPACITY, 64);
    }

    #[test]
    fn events_router_compiles_with_api_state() {
        // Compile-check: constructing events_router() with a fake ApiState exercises
        // the full type graph (WebSocketUpgrade, State<Arc<ApiState>>, broadcast::Sender).
        // Deliberately minimal — we don't spin up a real Axum test server because
        // CONTEXT.md D-20 scopes end-to-end WS integration out of this phase.
        use crate::api::dashboard::ApiState;
        use crate::db::Database;
        use crate::db::test_support::InMemoryDb;
        use crate::secrets::test_support::InMemorySecretStore;
        use std::sync::Arc;
        let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
        let (event_tx, _rx) = new_event_channel();
        let secret_store: Arc<dyn crate::secrets::SecretStore> =
            Arc::new(InMemorySecretStore::new(std::collections::HashMap::new()));
        let state = Arc::new(ApiState {
            github_token: "t".into(),
            repo_owner: "o".into(),
            repo_name: "r".into(),
            db,
            event_tx,
            secret_store,
        });
        let _router: axum::Router = events_router(state);
        // If we got here, the router typechecked; success.
    }
}
