//! Pure threshold-evaluator for drift alerts.
//!
//! `evaluate` takes the wall-clock time `now` as an explicit parameter so it is
//! unit-testable without a real clock (see CONTEXT.md D-05 / ALRT-02). No I/O,
//! no DB access, no reqwest — the caller (wave-2 `drift_poll_loop`) owns all
//! side effects.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::types::DriftData;

/// Operator-configured alert parameters + the last-sent checkpoint.
#[allow(dead_code)] // Constructed by plan 08-05 drift_poll_loop alert block; staged here for the type contract.
#[derive(Clone, Debug)]
pub struct AlertConfig {
    /// Minimum drifted-job count that triggers an alert (inclusive, D-01).
    pub threshold: u32,
    /// Cooldown window — repeat alerts suppressed within this duration (D-02).
    pub cooldown: Duration,
    /// Timestamp of the most recent successful alert send, or None on first run.
    pub last_sent: Option<DateTime<Utc>>,
}

/// Outcome of an evaluation — `Send` means the caller should POST to Slack.
#[allow(dead_code)] // Constructed by plan 08-05 drift_poll_loop alert block; staged here for the type contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertDecision {
    /// Drifted count is under the threshold; no alert.
    BelowThreshold,
    /// Drifted count meets/exceeds threshold but cooldown window is still active.
    Cooldown,
    /// Fire an alert now.
    Send,
}

/// Decide whether to send a drift alert given drift results, operator config,
/// and the current wall-clock time. Pure — takes `now` as a parameter and does
/// not observe `Utc::now()` internally (see CONTEXT.md D-05 and ALRT-02).
#[allow(dead_code)] // Called by plan 08-05 drift_poll_loop alert block; staged here for the type contract.
pub fn evaluate(drift: &DriftData, cfg: &AlertConfig, now: DateTime<Utc>) -> AlertDecision {
    if drift.drifted < cfg.threshold {
        return AlertDecision::BelowThreshold;
    }
    if let Some(last) = cfg.last_sent
        && now < last + cfg.cooldown
    {
        return AlertDecision::Cooldown;
    }
    AlertDecision::Send
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DriftItem, DriftType};

    fn drift(count: u32) -> DriftData {
        let items = (0..count)
            .map(|i| DriftItem {
                name: format!("job-{i}"),
                environment: "dev".into(),
                region: "us-east-1".into(),
                drift_type: DriftType::Modified,
                fields_changed: vec![],
                old_config: None,
                new_config: None,
            })
            .collect();
        DriftData {
            items,
            in_sync: 0,
            drifted: count,
        }
    }

    fn cfg(threshold: u32, cooldown_secs: u64, last_sent: Option<DateTime<Utc>>) -> AlertConfig {
        AlertConfig {
            threshold,
            cooldown: Duration::from_secs(cooldown_secs),
            last_sent,
        }
    }

    #[test]
    fn below_threshold_returns_below_threshold() {
        let c = cfg(5, 600, None);
        assert_eq!(evaluate(&drift(3), &c, Utc::now()), AlertDecision::BelowThreshold);
    }

    #[test]
    fn at_threshold_returns_send_when_no_prior_alert() {
        // D-01: inclusive comparison — drifted == threshold fires.
        let c = cfg(5, 600, None);
        assert_eq!(evaluate(&drift(5), &c, Utc::now()), AlertDecision::Send);
    }

    #[test]
    fn above_threshold_returns_send_when_no_prior_alert() {
        let c = cfg(5, 600, None);
        assert_eq!(evaluate(&drift(10), &c, Utc::now()), AlertDecision::Send);
    }

    #[test]
    fn cooldown_active_returns_cooldown() {
        // last_sent was 30s ago, cooldown is 600s → still in window.
        let now = Utc::now();
        let last = now - chrono::Duration::seconds(30);
        let c = cfg(5, 600, Some(last));
        assert_eq!(evaluate(&drift(10), &c, now), AlertDecision::Cooldown);
    }

    #[test]
    fn cooldown_elapsed_returns_send() {
        // last_sent was 700s ago, cooldown is 600s → elapsed.
        let now = Utc::now();
        let last = now - chrono::Duration::seconds(700);
        let c = cfg(5, 600, Some(last));
        assert_eq!(evaluate(&drift(10), &c, now), AlertDecision::Send);
    }

    #[test]
    fn cooldown_boundary_exact_returns_send() {
        // now == last_sent + cooldown → `now < last + cooldown` is false → Send.
        let now = Utc::now();
        let last = now - chrono::Duration::seconds(600);
        let c = cfg(5, 600, Some(last));
        assert_eq!(evaluate(&drift(10), &c, now), AlertDecision::Send);
    }

    #[test]
    fn no_last_sent_bootstrap_returns_send_at_threshold() {
        // Fresh install: last_sent is None → cooldown guard skipped → Send.
        let c = cfg(1, 600, None);
        assert_eq!(evaluate(&drift(1), &c, Utc::now()), AlertDecision::Send);
    }

    #[test]
    fn below_threshold_ignores_cooldown_state() {
        // Even if cooldown is active, BelowThreshold takes precedence.
        let now = Utc::now();
        let last = now - chrono::Duration::seconds(30);
        let c = cfg(5, 600, Some(last));
        assert_eq!(evaluate(&drift(2), &c, now), AlertDecision::BelowThreshold);
    }
}
