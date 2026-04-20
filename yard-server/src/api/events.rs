//! WebSocket event bus: tagged Event enum + broadcast channel factory.
//!
//! NOTE: The WASM client declares a mirror `Event` enum in `ui/connection.rs`
//! (derives `Deserialize` instead of `Serialize`). Variant names and fields
//! MUST stay in lock-step between the two files.

use serde::Serialize;
use tokio::sync::broadcast;

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
}
