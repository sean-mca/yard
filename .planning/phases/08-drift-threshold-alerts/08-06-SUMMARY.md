---
phase: 08-drift-threshold-alerts
plan: 06
status: complete
---

# Plan 08-06 Summary — Settings page alert inputs

## Files

**Modified:**
- `yard-server/src/ui/settings.rs` — two new signals, two fetch arms, two number input elements

## Requirements

- **ALRT-01** (UI half): Operator can now view and edit `alert_drift_threshold` and `alert_cooldown_minutes` from the Settings page. Values persist through the existing `save_setting` → `POST /api/settings` → DynamoDB `Setting` table pipeline.

## Edits

1. **Two new signals** in `Settings`:
   - `let mut alert_threshold = use_signal(String::new);`
   - `let mut alert_cooldown = use_signal(String::new);`
2. **Two fetch arms** inside the mount `use_effect`: populate signals from fetched settings map.
3. **Two number inputs** in the Notifications section, rendered below the Slack card inside a new `div.rounded-lg.border…` container. Always visible per D-09.

## D-09 compliance

- Labels: exactly `"Alert threshold (jobs)"` and `"Cooldown (minutes)"`.
- Container is NOT wrapped in `if slack_enabled() { ... }` — operator can configure before enabling Slack.
- Input type `"number"` with `min="1"` (client hint only; plan 08-03 `validate_setting` is authoritative).

## Verification

- `cargo build -p yard-server` — exit 0
- `cargo clippy -p yard-server -- -D warnings` — exit 0
- No `Cargo.toml` changes

## Execution note

Original parallel subagent blocked on sandbox-denied worktree base-reset. Orchestrator executed this plan inline. Outcome is identical — same code, same build, same clippy status.

## Manual UAT (Sean)

1. Visit `/settings` in the dashboard.
2. Verify the two new inputs render in the Notifications section below the Slack card.
3. Type `5` into "Alert threshold (jobs)" and tab out → POST fires.
4. Reload the page → value reappears (round-trip through DynamoDB).
5. Type `0` → server returns 400 (ignored silently by UI per v1 save pattern).

## Commits

- `cd8f171` — feat(08-06): add alert threshold + cooldown number inputs to Settings page
