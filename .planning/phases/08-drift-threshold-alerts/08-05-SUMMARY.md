---
phase: 08-drift-threshold-alerts
plan: 05
status: complete
---

# Plan 08-05 Summary — Integration glue

## Files

**Modified:**
- `yard-server/src/main.rs` — alert block in `drift_poll_loop` + AlertSent arm in Shell WS match

## Requirements

- **ALRT-01** (persistence wiring): `set_setting("alert_last_sent_at", ...)` persists the cooldown checkpoint on successful Slack POST.
- **ALRT-03** (actual delivery): Calls `alerting::slack::post_slack_alert` when `evaluate()` returns `Send`.
- **ALRT-04** (cooldown behavior): Checkpoint updated only on success; next poll retries on failure. Shell WS match bumps `drift_tick` on `AlertSent` so drift page re-fetches.

## Short-circuit order (D-07)

Cheap checks first, so a fresh install reads nothing unnecessary:
1. `slack_enabled == "true"`
2. `slack_webhook_url` non-empty
3. `alert_drift_threshold` parses as `u32`
   → only then read `alert_cooldown_minutes` and `alert_last_sent_at`

## Failure handling (D-12, D-16)

| Step | Failure behavior |
|------|------------------|
| Slack POST error | `warn!(error = %e, "Slack alert POST failed")` — webhook URL deliberately omitted (T-08-02-05). No cooldown update. No AlertSent emit. Next poll retries. |
| `set_setting` error after successful POST | `warn!(error = %e, "Failed to persist alert_last_sent_at ...")`. No AlertSent emit (invariant: AlertSent ⇔ last_sent_at persisted). |
| `AlertDecision::Cooldown` | `info!("Drift alert skipped (cooldown)")`. |
| `AlertDecision::BelowThreshold` | Silent (common case). |

## Security mitigations

- **T-08-03-01 (u64 overflow)**: `cooldown_mins.saturating_mul(60)` instead of plain `*` — attacker-set `u64::MAX` produces `u64::MAX` seconds, no panic/wrap.
- **T-08-02-05 (webhook URL in logs)**: Failure log uses `warn!(error = %e, "Slack alert POST failed")` — no URL reference. Verified by grep acceptance criterion.
- **T-08-05-06 (partial-failure consistency)**: Slack POST → set_setting → emit, gated strictly. No AlertSent unless both succeed.

## Verification

- `cargo build -p yard-server` — exit 0
- `cargo clippy -p yard-server -- -D warnings` — exit 0
- `cargo test -p yard-server` — 87 tests pass (80 pre-Phase-8 + 7 new Slack tests; 0 regressions)
- No `Cargo.toml` changes

## Manual UAT (Sean)

1. `slack_enabled=true`, `slack_webhook_url=<real hook>`, `alert_drift_threshold=1`, `alert_cooldown_minutes=1`.
2. Introduce drift and wait for `drift_poll_loop` to run.
3. Observe Slack message land.
4. Observe drift page re-fetches (via `drift_tick` bump).
5. Observe next poll within cooldown window → `"Drift alert skipped (cooldown)"` log.
6. Wait past cooldown → new alert fires.

No automated test covers the full integration per CONTEXT.md.

## Commits

- `0f8a064` — feat(08-05): integrate alerting into drift_poll_loop + Shell WS consumer
