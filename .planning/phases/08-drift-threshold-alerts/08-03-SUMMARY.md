---
phase: 08-drift-threshold-alerts
plan: 03
subsystem: yard-server/api
tags: [rust, settings, validation, persistence, alerting]
requirements: [ALRT-01]
dependency-graph:
  requires: []
  provides:
    - "validate_setting arms for alert_drift_threshold, alert_cooldown_minutes, alert_last_sent_at"
  affects:
    - "yard-server/src/api/settings.rs"
tech-stack:
  added: []
  patterns:
    - "match-guard positive-int validation (Ok(n) if n >= 1)"
    - "lenient pass-through for server-written keys"
key-files:
  created: []
  modified:
    - yard-server/src/api/settings.rs
decisions:
  - "Match-guard `Ok(n) if n >= 1` rather than separate zero-check — enforces D-08 inline"
  - "alert_last_sent_at accepts any string (D-03 lenient — server-written via plan 08-05)"
  - "u32 for threshold (roadmap caps at reasonable job count), u64 for cooldown (aligns with Duration construction)"
metrics:
  duration_minutes: 3
  completed_date: "2026-04-20"
  tasks_completed: 1
  tests_added: 6
  files_modified: 1
---

# Phase 08 Plan 03: alert_* Settings Validation Summary

Extended `validate_setting` with three new match arms (`alert_drift_threshold`, `alert_cooldown_minutes`, `alert_last_sent_at`) and 6 inline tests, unblocking persistence of alerting configuration via the existing `Setting` table — no new DB trait methods, no new Cargo deps, no migrations.

## What Changed

### File Modified: `yard-server/src/api/settings.rs`

Inserted three new match arms into `validate_setting` immediately after the existing `slack_webhook_url` arm and before the catch-all `_ => Err(...)`:

```rust
"alert_drift_threshold" => match value.parse::<u32>() {
    Ok(n) if n >= 1 => Ok(()),
    _ => Err(format!(
        "invalid alert_drift_threshold '{value}': must be a positive integer >= 1"
    )),
},
"alert_cooldown_minutes" => match value.parse::<u64>() {
    Ok(n) if n >= 1 => Ok(()),
    _ => Err(format!(
        "invalid alert_cooldown_minutes '{value}': must be a positive integer >= 1"
    )),
},
"alert_last_sent_at" => Ok(()),
```

Added 6 inline unit tests (test count went from 14 to 20 in the settings `mod tests` block):

- `rejects_invalid_alert_drift_threshold` — covers "0", "-1", "abc"
- `accepts_valid_alert_drift_threshold` — covers "1", "100"
- `rejects_invalid_alert_cooldown_minutes` — covers "0", "-5", "abc"
- `accepts_valid_alert_cooldown_minutes` — covers "1", "10", "1440"
- `accepts_any_alert_last_sent_at` — covers RFC3339 string, arbitrary string, empty
- `post_settings_rejects_invalid_alert_threshold_with_400` — end-to-end handler test proving the `validate_setting → ApiError::BadRequest → 400` wiring works for the new keys

## Key Decisions

1. **`>= 1` guard via match-guard, not separate zero-check.** Plan explicitly required rejecting `0` (D-08) rather than mirroring the existing `dashboard_interval` arm's permissive `parse::<u64>()`. Using `Ok(n) if n >= 1 => Ok(())` with a wildcard `_ => Err(...)` catch-all funnels both parse errors and `Ok(0)` to the same error branch — cleaner than `if n == 0 { Err }` inside the arm body.

2. **`alert_last_sent_at` lenient pass-through.** Per CONTEXT.md D-03 and the "Claude's Discretion" guidance, the key is server-written (by plan 08-05's `set_setting("alert_last_sent_at", utc_rfc3339)`) and never user-input. Strict RFC3339 parsing would add no security value and would complicate the server-side write path if the timestamp format ever changes. This mirrors the existing `slack_webhook_url` lenient arm.

3. **u32 / u64 split.** Threshold is `u32` (a drifted job count; even very large estates won't exceed 4B jobs). Cooldown is `u64` to match the `Duration::from_secs(mins * 60)` construction that plan 08-05 will perform downstream. Threat T-08-03-01 (u64::MAX cooldown overflow) is documented in the plan's threat model as a plan-08-05 mitigation responsibility.

## Verification

All plan acceptance criteria satisfied:

- `cargo build -p yard-server` exits 0
- `cargo test -p yard-server api::settings` — 20 tests pass (14 existing + 6 new)
- `cargo clippy -p yard-server -- -D warnings` exits 0
- All 12 grep checks in `<acceptance_criteria>` pass
- Specific test invocation `cargo test -- api::settings::tests::rejects_invalid_alert_drift_threshold` exits 0

## TDD Gate Compliance

Plan was executed with strict RED → GREEN sequencing:

- **RED commit** `861c9a6` — `test(08-03): add failing tests for alert_* settings validation`
- **GREEN commit** `be3f92b` — `feat(08-03): validate alert_drift_threshold, alert_cooldown_minutes, alert_last_sent_at`
- **REFACTOR** — not needed; implementation matched plan's exact shape and clippy was clean on first try.

## Deviations from Plan

None — plan executed exactly as written. No auto-fixes, no architectural decisions, no authentication gates.

## Threat Flags

No new threat surface introduced beyond what plan's `<threat_model>` already enumerated (T-08-03-01 through T-08-03-05). Plan 08-05 is responsible for the `saturating_mul` mitigation for T-08-03-01.

## Commits

| # | Hash | Type | Message |
|---|------|------|---------|
| 1 | `861c9a6` | test | add failing tests for alert_* settings validation (RED) |
| 2 | `be3f92b` | feat | validate alert_drift_threshold, alert_cooldown_minutes, alert_last_sent_at (GREEN) |

## Self-Check: PASSED

- `yard-server/src/api/settings.rs` exists on disk
- `.planning/phases/08-drift-threshold-alerts/08-03-SUMMARY.md` exists on disk
- Commit `861c9a6` (RED) exists in git log
- Commit `be3f92b` (GREEN) exists in git log
- All acceptance-criteria greps pass
- `cargo build`, `cargo test api::settings`, `cargo clippy -D warnings` all exit 0
