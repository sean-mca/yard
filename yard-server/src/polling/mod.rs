//! Polling-loop supervision for the drift / dashboard background tasks (SRV-03).
//!
//! Provides `supervised_iteration` — a thin wrapper around `tokio::time::timeout`
//! whose three outcomes (success / iteration-failed / iteration-timed-out) are
//! captured by the `SupervisedResult` enum so callers can branch on them
//! ergonomically and update an exponential-backoff counter.
//!
//! Mirror of the per-call timeout pattern at `main.rs:427-450` (WR-06), lifted
//! up to the iteration-body level so a stuck DDB / git / Slack call cannot
//! pin a polling task forever (CONTEXT.md D-02, D-05, D-08).
//!
//! Lives in its own module (rather than inline in `main.rs`) so the supervisor
//! helpers have a home for inline tests and future extension (e.g., dynamic
//! poll-interval reload), per CONTEXT.md D-08 + Claude's Discretion
//! recommendation. `main.rs` stays focused on bootstrap.

use std::future::Future;
use std::time::Duration;

/// Outcome of a single supervised iteration.
pub enum SupervisedResult<T, E> {
    /// Iteration completed successfully within the timeout.
    Ok(T),
    /// Iteration completed but returned an error.
    IterationFailed(E),
    /// Iteration future did not resolve before the timeout elapsed.
    IterationTimedOut,
}

/// Run `fut` to completion under `timeout`. Wraps `tokio::time::timeout` and
/// flattens the nested `Result<Result<T, E>, Elapsed>` shape into
/// `SupervisedResult<T, E>` for ergonomic match-arm handling at call sites.
pub async fn supervised_iteration<T, E, Fut>(
    timeout: Duration,
    fut: Fut,
) -> SupervisedResult<T, E>
where
    Fut: Future<Output = Result<T, E>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(Ok(value)) => SupervisedResult::Ok(value),
        Ok(Err(e)) => SupervisedResult::IterationFailed(e),
        Err(_) => SupervisedResult::IterationTimedOut,
    }
}

/// Compute the next sleep duration after a polling-loop iteration.
///
/// - `interval` is the configured tick interval (the "happy path" sleep AND
///   the upper cap — we never sleep longer than the configured interval so
///   operators always get a refresh attempt every interval at worst).
/// - `consecutive_errors == 0` returns `interval` unchanged.
/// - Otherwise: `min(interval, 30s * 2^min(consecutive_errors, 6))`.
///
/// CONTEXT.md D-05.
pub fn compute_backoff_sleep(interval: Duration, consecutive_errors: u32) -> Duration {
    if consecutive_errors == 0 {
        return interval;
    }
    let pow = 2u64.saturating_pow(consecutive_errors.min(6));
    let candidate = Duration::from_secs(30u64.saturating_mul(pow));
    std::cmp::min(candidate, interval)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn supervised_iteration_returns_ok_on_success() {
        let result: SupervisedResult<u32, anyhow::Error> =
            supervised_iteration(Duration::from_millis(50), async { Ok(42u32) }).await;
        assert!(matches!(result, SupervisedResult::Ok(42)));
    }

    #[tokio::test]
    async fn supervised_iteration_reports_failure() {
        let result: SupervisedResult<(), anyhow::Error> = supervised_iteration(
            Duration::from_millis(50),
            async { Err::<(), _>(anyhow::anyhow!("boom")) },
        )
        .await;
        assert!(matches!(result, SupervisedResult::IterationFailed(_)));
    }

    #[tokio::test]
    async fn supervised_iteration_times_out_on_stall() {
        // A future that never resolves on its own. The supervisor's
        // tokio::time::timeout drives the wall-clock timeout to force the
        // elapsed branch. Using real time with a small duration (10ms) keeps
        // the test fast and avoids the `tokio` `test-util` feature dep that
        // `tokio::time::pause()` requires.
        let stall = std::future::pending::<Result<(), anyhow::Error>>();
        let result: SupervisedResult<(), anyhow::Error> =
            supervised_iteration(Duration::from_millis(10), stall).await;
        assert!(matches!(result, SupervisedResult::IterationTimedOut));
    }

    #[test]
    fn compute_backoff_zero_errors_returns_interval() {
        let interval = Duration::from_secs(180);
        assert_eq!(compute_backoff_sleep(interval, 0), interval);
    }

    #[test]
    fn compute_backoff_grows_then_caps_at_interval() {
        let interval = Duration::from_secs(180); // 3-minute drift cadence
        // 1 error → 30s * 2 = 60s (under cap)
        assert_eq!(compute_backoff_sleep(interval, 1), Duration::from_secs(60));
        // 2 errors → 30s * 4 = 120s (under cap)
        assert_eq!(compute_backoff_sleep(interval, 2), Duration::from_secs(120));
        // 3 errors → 30s * 8 = 240s → capped at 180s
        assert_eq!(compute_backoff_sleep(interval, 3), interval);
        // 6 errors → 30s * 64 = 1920s → capped at 180s
        assert_eq!(compute_backoff_sleep(interval, 6), interval);
        // 100 errors → still capped at 180s (saturating_pow + min)
        assert_eq!(compute_backoff_sleep(interval, 100), interval);
    }

    #[test]
    fn compute_backoff_does_not_overflow() {
        // u32::MAX errors must not panic (saturating_pow + saturating_mul).
        let interval = Duration::from_secs(60);
        let _ = compute_backoff_sleep(interval, u32::MAX);
    }
}
