---
phase: 08-drift-threshold-alerts
plan: 04
status: complete
---

# Plan 08-04 Summary — AlertSent WebSocket event variant

## Files

**Modified:**
- `yard-server/src/api/events.rs` — added `Event::AlertSent { drifted_count: u32 }` + 2 serde round-trip tests
- `yard-server/src/ui/connection.rs` — added matching WASM mirror variant with `#[allow(dead_code)]` on `drifted_count`

## Requirements

- **ALRT-03** (type-contract portion): UI hook variant `AlertSent { drifted_count: u32 }` defined on both sides of the wire.
- **ALRT-04** (type-contract portion): Client-side `AlertSent` variant enables the UI tick that re-fetches drift data after an alert (actual `bump()` wiring is plan 08-05 scope).

## Lock-step contract

Server and WASM mirror agree exactly:
- Variant name: `AlertSent`
- Field: `drifted_count: u32`
- Serde tagging: `#[serde(tag = "event", rename_all = "snake_case")]` on both enums
- Wire format: `{"event":"alert_sent","drifted_count":N}`

## Tests

2 new inline tests in `api::events::tests`:
- `event_alert_sent_serializes_with_count` — pins exact wire format with `drifted_count: 5`
- `event_alert_sent_serializes_with_zero_count` — zero-value edge case

All passing. `cargo test -p yard-server api::events` — 14 tests (12 prior + 2 new).

## Verification

- `cargo build -p yard-server` — exit 0
- `cargo test -p yard-server api::events` — 14/14 passed
- `cargo clippy -p yard-server -- -D warnings` — exit 0
- No `Cargo.toml` changes, no new crates

## Known follow-up (by design)

WASM build (`cargo clippy --target wasm32-unknown-unknown`) currently emits a non-exhaustive match in `main.rs:287` because `Shell`'s WS match does not yet handle `Event::AlertSent`. This is plan 08-05's scope — it adds the consumer arm that does `drift_tick.bump()`. Not a defect in this plan.

## Commits

- `304c04d` — test(08-04): add failing serde round-trip tests for Event::AlertSent (RED)
- `2433d5f` — feat(08-04): add Event::AlertSent { drifted_count } to server enum (GREEN)
- `affa2f6` — feat(08-04): mirror AlertSent variant in WASM client Event enum
