use axum::{
    extract::State,
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::db::Database;
use crate::github::client::GitHubApi;

/// Per-dependency probe timeout.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a readiness result is considered fresh.
const CACHE_TTL: Duration = Duration::from_secs(5);

/// Shared state for health endpoints. Constructed once at startup and passed
/// to the health router via `Arc<HealthState>`.
pub struct HealthState {
    pub db: Arc<dyn Database>,
    pub github_client: Arc<dyn GitHubApi>,
    pub cache: RwLock<Option<(Instant, ReadinessResult)>>,
    /// Phase 48 will flip this to `false` during graceful shutdown so the ALB
    /// stops routing traffic before in-flight requests finish draining.
    pub accepting_traffic: AtomicBool,
}

/// The result of probing both external dependencies.
#[derive(Debug, Clone, Serialize)]
pub struct ReadinessResult {
    dynamodb: &'static str,
    github: &'static str,
}

impl ReadinessResult {
    fn is_ready(&self) -> bool {
        self.dynamodb == "ok" && self.github == "ok"
    }
}

/// Build the health router with two routes:
/// - `GET /health` -- liveness probe (always 200)
/// - `GET /ready`  -- readiness probe (200 or 503)
pub fn health_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .with_state(state)
}

/// Liveness probe: returns 200 whenever the process is running.
async fn health_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Readiness probe: returns 200 when DynamoDB and GitHub are reachable,
/// 503 otherwise. Checks `accepting_traffic` first (Phase 48 shutdown hook).
async fn ready_handler(
    State(state): State<Arc<HealthState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    // Phase 48 shutdown hook: if draining, immediately return 503.
    if !state.accepting_traffic.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "details": {
                    "dynamodb": "draining",
                    "github": "draining",
                }
            })),
        );
    }

    let result = get_or_refresh_readiness(&state).await;

    if result.is_ready() {
        (
            StatusCode::OK,
            Json(serde_json::json!({"status": "ready"})),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "details": {
                    "dynamodb": result.dynamodb,
                    "github": result.github,
                }
            })),
        )
    }
}

/// Double-checked locking cache lookup. Acquires a read lock first; if the
/// cached result is still fresh, returns it without contention. Otherwise
/// acquires a write lock, re-checks (another request may have refreshed in
/// the meantime), and probes dependencies only when truly stale.
async fn get_or_refresh_readiness(state: &HealthState) -> ReadinessResult {
    // Fast path: read lock.
    {
        let cache = state.cache.read().await;
        if let Some((instant, ref result)) = *cache
            && instant.elapsed() < CACHE_TTL
        {
            return result.clone();
        }
    }

    // Slow path: write lock with double-check.
    let mut cache = state.cache.write().await;
    if let Some((instant, ref result)) = *cache
        && instant.elapsed() < CACHE_TTL
    {
        return result.clone();
    }

    let result = probe_dependencies(state).await;
    *cache = Some((Instant::now(), result.clone()));
    result
}

/// Run both dependency probes concurrently with per-probe timeouts.
async fn probe_dependencies(state: &HealthState) -> ReadinessResult {
    let (db_result, gh_result) = tokio::join!(
        tokio::time::timeout(PROBE_TIMEOUT, state.db.health_check()),
        tokio::time::timeout(PROBE_TIMEOUT, state.github_client.health_check()),
    );

    let dynamodb = match db_result {
        Ok(Ok(())) => "ok",
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "DynamoDB health check failed");
            "unreachable"
        }
        Err(_) => {
            tracing::warn!("DynamoDB health check timed out");
            "unreachable"
        }
    };

    let github = match gh_result {
        Ok(Ok(())) => "ok",
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "GitHub health check failed");
            "unreachable"
        }
        Err(_) => {
            tracing::warn!("GitHub health check timed out");
            "unreachable"
        }
    };

    ReadinessResult { dynamodb, github }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::db::test_support::InMemoryDb;
    use crate::github::client::test_support::InMemoryGitHubApi;

    fn build_health_state() -> Arc<HealthState> {
        Arc::new(HealthState {
            db: Arc::new(InMemoryDb::new()),
            github_client: Arc::new(InMemoryGitHubApi::new()),
            cache: RwLock::new(None),
            accepting_traffic: AtomicBool::new(true),
        })
    }

    #[tokio::test]
    async fn health_returns_200_with_ok_status() {
        let response = health_handler().await;
        assert_eq!(response.0["status"], "ok");
    }

    #[tokio::test]
    async fn ready_returns_200_when_all_deps_healthy() {
        let state = build_health_state();
        let (status, body) = ready_handler(State(state)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.0["status"], "ready");
    }

    #[tokio::test]
    async fn ready_returns_503_when_accepting_traffic_false() {
        let state = build_health_state();
        state.accepting_traffic.store(false, Ordering::Relaxed);

        let (status, body) = ready_handler(State(state)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.0["status"], "not_ready");
        let details = &body.0["details"];
        assert_eq!(details["dynamodb"], "draining");
        assert_eq!(details["github"], "draining");
    }

    #[tokio::test]
    async fn ready_caches_result_within_ttl() {
        let state = build_health_state();

        // First call: cache is empty, probes run.
        let (status, _) = ready_handler(State(state.clone())).await;
        assert_eq!(status, StatusCode::OK);

        // Verify cache is populated.
        let cache = state.cache.read().await;
        assert!(cache.is_some(), "cache should be populated after first call");
        let (instant, result) = cache.as_ref().unwrap();
        assert!(instant.elapsed() < CACHE_TTL, "cache entry should be fresh");
        assert!(result.is_ready());
        drop(cache);

        // Second call: should return from cache (no way to verify probes
        // didn't run with InMemoryDb, but we verify the cache is still present
        // and the response is consistent).
        let (status2, body2) = ready_handler(State(state.clone())).await;
        assert_eq!(status2, StatusCode::OK);
        assert_eq!(body2.0["status"], "ready");
    }
}
