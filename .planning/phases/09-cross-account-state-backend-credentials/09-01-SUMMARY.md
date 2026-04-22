---
phase: 09-cross-account-state-backend-credentials
plan: 01
subsystem: yard-structs
tags: [data-shape, serde, aws, cross-account]
requires:
  - "yard-structs (existing deps: serde, serde_json)"
provides:
  - "StateBackend::S3.aws (serde_json::Value) — optional per-state AWS creds sub-block"
  - "AirflowSection.aws (serde_json::Value) — optional per-DAG-bucket AWS creds sub-block"
  - "DagState.aws (serde_json::Value) — persisted apply-time AWS creds for destroy-path re-auth"
affects:
  - "yard-core/src/storage.rs (Plan 02 will read StateBackend::S3.aws in get_storage)"
  - "yard-core/src/dag_lifecycle.rs (Plan 03 will read AirflowSection.aws at apply, DagState.aws at destroy)"
tech-stack:
  added: []
  patterns:
    - "Untyped `serde_json::Value` for aws:* blocks (matches ProjectManifest.aws precedent)"
    - "`#[serde(default, skip_serializing_if = \"serde_json::Value::is_null\")]` for strictly-additive roundtrip"
    - "Inline `#[cfg(test)] mod tests` per Phase 6/7/8 convention"
key-files:
  created: []
  modified:
    - yard-structs/src/config.rs
    - yard-structs/src/state.rs
decisions:
  - "Drop `Eq` derive from AirflowSection and AirflowJobBlock — `serde_json::Value` does not implement `Eq` (NaN handling). Grepped workspace first: no callers rely on Eq-gated generics for these types."
  - "aws field on StateBackend::S3 has no `pub` qualifier (matches enum variant field convention — variant fields have implicit pub within the variant)."
  - "DagState.aws is pub (struct field convention)."
  - "No Cargo.toml changes — serde_json was already a yard-structs dep."
metrics:
  duration_seconds: 213
  tasks: 2
  files: 2
  completed: "2026-04-22T20:56:57Z"
---

# Phase 9 Plan 01: `aws:` sub-block data shape in yard-structs Summary

Added three optional `aws: serde_json::Value` fields — `StateBackend::S3.aws`, `AirflowSection.aws`, `DagState.aws` — with strictly-additive `#[serde(default, skip_serializing_if)]` attributes so existing yard.yaml and state files round-trip byte-identically.

## What Was Built

### 1. `StateBackend::S3.aws` (yard-structs/src/config.rs)

Extended the `S3` variant only; `Local` untouched.

```rust
S3 {
    bucket: String,
    region: String,
    key: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    aws: serde_json::Value,
}
```

### 2. `AirflowSection.aws` (yard-structs/src/config.rs)

New field appended after `triggered_by`:

```rust
#[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
pub aws: serde_json::Value,
```

Shape parallels `ProjectManifest.aws` and `StateBackend::S3.aws`. Readers will use `.get("assume_role").and_then(|v| v.as_str())` (and likewise `session_name`, `external_id`).

### 3. `DagState.aws` (yard-structs/src/state.rs)

New field appended to the persisted per-DAG state file struct:

```rust
#[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
pub aws: serde_json::Value,
```

Enables the destroy path to re-authenticate against the same account the apply used, closing the acknowledged limitation documented in `dag_lifecycle.rs:292-293`.

### 4. `Eq` derive dropped from `AirflowSection` and `AirflowJobBlock`

`serde_json::Value` does not implement `Eq` (NaN handling prevents total ordering on floats). Flattening a non-`Eq` type into an `Eq`-deriving struct fails to compile — so both types now derive `PartialEq` only.

Workspace grep before the change confirmed no callers rely on `Eq`-gated generics (`HashSet<AirflowSection>`, `BTreeSet<AirflowJobBlock>`, etc.) for these types:

```
grep -rn "AirflowSection\|AirflowJobBlock" --include="*.rs" \
  | grep -v ".planning" \
  | grep -iE "\beq\b|hashset|hashmap.*Airflow|btreeset.*Airflow"
# (no hits)
```

## Test Coverage

7 total tests in yard-structs after this plan (5 new in config.rs, 2 new in state.rs). All pass:

| Test | Module | Proves |
|------|--------|--------|
| `state_backend_s3_no_aws_roundtrip` | config | `aws: null` is skipped on serialize (byte-identical to today). |
| `state_backend_s3_with_aws` | config | `assume_role` and `external_id` round-trip through the untyped Value. |
| `state_backend_local_unchanged` | config | Local variant still has no `aws` field. |
| `airflow_section_no_aws_roundtrip` | config | Absent `aws` key in yaml stays absent on re-serialize. |
| `airflow_section_with_aws` | config | `assume_role` + `session_name` round-trip on AirflowSection. |
| `dag_state_no_aws_roundtrip` | state | Legacy state files (pre-Phase 9) deserialize with `aws: Null` via `#[serde(default)]`. |
| `dag_state_with_aws` | state | DagState with `aws: {assume_role: "..."}` serializes and round-trips. |

Round-trip tests are the D-02 "strictly additive" guarantee at the type layer.

## Expected Downstream Build Failure (Plans 02 & 03 resolve)

As documented in the plan's `<verification>` section, `cargo build --workspace` now fails with:

```
error[E0063]: missing field `aws` in initializer of `DagState`
error[E0063]: missing field `aws` in initializer of `StateBackend`
error[E0027]: pattern does not mention field `aws`
error[E0063]: missing field `aws` in initializer of `AirflowSection`
```

These originate from construction sites and pattern matches in `yard-core` (notably `dag_lifecycle.rs:172` for DagState; storage.rs and tests elsewhere). **This is expected and planned** — Plans 02 and 03 own the yard-core wiring and must:

- Plan 02: populate `StateBackend::S3 { ..., aws }` construction and pattern-match the new field in `get_storage`.
- Plan 03: populate `DagState { ..., aws }` construction (`dag_lifecycle.rs:172` and test fixtures in `storage.rs:774`) and `AirflowSection { ..., aws }` construction sites. Read `dag.config.aws` at apply, persisted `DagState.aws` at destroy.

yard-structs itself builds and tests cleanly (`cargo build -p yard-structs`, `cargo test -p yard-structs --lib`, `cargo clippy -p yard-structs -- -D warnings` all pass). This is the isolation boundary this plan targets.

## No Cargo.toml Changes

`yard-structs/Cargo.toml` is unchanged. `serde_json` was already a transitive dep; no new crates introduced. Project rule compliance verified by `git diff HEAD~4 HEAD -- yard-structs/Cargo.toml` returning empty.

## Deviations from Plan

None — plan executed exactly as written. TDD RED/GREEN gates followed for both tasks:

| Task | RED commit | GREEN commit |
|------|------------|--------------|
| 1 (config.rs) | `49b3ab9` | `4d35e0f` |
| 2 (state.rs) | `ba617c6` | `3a33bba` |

No REFACTOR commits needed — code written correctly on first GREEN iteration.

## Success Criteria (from PLAN.md)

- [x] `StateBackend::S3` has `aws: serde_json::Value` field with `#[serde(default, skip_serializing_if = "serde_json::Value::is_null")]`.
- [x] `AirflowSection` has `aws: serde_json::Value` field with the same serde attributes.
- [x] `DagState` has `aws: serde_json::Value` field with the same serde attributes.
- [x] `Eq` derive dropped from `AirflowSection` and `AirflowJobBlock` (because `serde_json::Value` is not `Eq`).
- [x] Inline `#[cfg(test)] mod tests` in both `config.rs` and `state.rs` prove legacy JSON handling, null-skip behavior, and round-trip with sub-fields.
- [x] `cargo test -p yard-structs --lib` passes (7 tests, 0 failures).
- [x] `cargo clippy -p yard-structs --all-targets -- -D warnings` exits 0.
- [x] `cargo fmt -p yard-structs --check` exits 0.
- [x] `yard-structs/Cargo.toml` UNCHANGED.
- [x] `StateBackend::Local` UNCHANGED (no `aws` field added).

## Threat Model Outcome

All STRIDE dispositions in the plan's threat register remain accurate. No new attack surface introduced at the type layer — all new fields accept `serde_json::Value` without validation (T-09-04 mitigation: downstream readers use `.get(...).and_then(|v| v.as_str())` which returns `None` for non-objects, no panic). T-09-02 mitigation (`tracing::debug!` for cred logging) is Plans 02/03's responsibility since no logging is added here.

## Requirements Marked Complete

From PLAN.md frontmatter `requirements:`:
- `CRED-01-TBD` — optional aws sub-block on StateBackend::S3 (data-shape only; wiring in Plan 02)
- `CRED-02-TBD` — optional aws sub-block on AirflowSection (data-shape only; wiring in Plan 03)
- `CRED-04-TBD` — persisted DagState.aws for destroy re-auth (data-shape only; writer/reader in Plan 03)

These are `TBD` suffixes because Phase 9 was added post v1.1 milestone close — Sean will formalize `CRED-0x` identifiers in REQUIREMENTS.md at phase commit time (per 09-CONTEXT.md "Requirements" note).

## TDD Gate Compliance

Plan is `type: execute` with `tdd="true"` on both tasks (not plan-level `type: tdd`). Each task follows RED → GREEN per-task:

- Task 1: `test(09-01)` commit `49b3ab9` precedes `feat(09-01)` commit `4d35e0f`.
- Task 2: `test(09-01)` commit `ba617c6` precedes `feat(09-01)` commit `3a33bba`.

Both RED commits produced compile-fail output (recorded in conversation log); GREEN commits produced all-green test output.

## Self-Check: PASSED

Verified files exist:
- `yard-structs/src/config.rs` — FOUND (modified)
- `yard-structs/src/state.rs` — FOUND (modified)

Verified commits exist:
- `49b3ab9` (test RED — config.rs tests) — FOUND
- `4d35e0f` (feat GREEN — config.rs impl) — FOUND
- `ba617c6` (test RED — state.rs tests) — FOUND
- `3a33bba` (feat GREEN — state.rs impl) — FOUND

Verification suite passes:
- `cargo test -p yard-structs --lib` — 7 passed, 0 failed
- `cargo clippy -p yard-structs --all-targets -- -D warnings` — clean
- `cargo fmt -p yard-structs --check` — clean
- `cargo build -p yard-structs` — clean
- `cargo build --workspace` — FAILS as documented (Plans 02/03 resolve)
