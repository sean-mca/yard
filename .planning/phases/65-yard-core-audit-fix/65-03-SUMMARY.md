---
phase: 65-yard-core-audit-fix
plan: 03
subsystem: audit
tags: [clippy, lint, rust, airflow, providers, storage, orchestrate]

requires:
  - phase: 64-yard-structs-yard-cli-audit-fix
    provides: yard-structs and yard-cli audit baseline
  - phase: 65-yard-core-audit-fix plan 01
    provides: codegen/ audit
  - phase: 65-yard-core-audit-fix plan 02
    provides: validation/ + parsing.rs audit
provides:
  - "All 21 remaining yard-core source files audited against 179 rules (14 categories)"
  - "7 clippy violations fixed (V-01 through V-07): unused import + integration test lint suppression"
  - "Full workspace clippy --all-targets clean (yard-structs, yard-core, yard-cli)"
affects: []

tech-stack:
  added: []
  patterns:
    - "Integration test crate roots use #![allow(clippy::unwrap_used, clippy::expect_used)] as first line"
    - "Non-crate-root test modules use #![allow(...)] inner attribute after #![allow(dead_code)]"

key-files:
  created: []
  modified:
    - yard-core/src/airflow_dag/mod.rs
    - yard-core/tests/glue_integration.rs
    - yard-core/tests/emr_integration.rs
    - yard-core/tests/phase9_integration.rs
    - yard-core/tests/plan_target_integration.rs
    - yard-core/tests/target_integration.rs
    - yard-core/tests/common/mod.rs
    - yard-structs/src/config.rs

key-decisions:
  - "V-06/V-07 added for consistency even though files have 0 unwrap/expect calls -- prevents future regressions"
  - "D-05 honored: HashMap<String, Deployment> TYPE-01 finding remains deferred for backward compat"
  - "D-06 honored: doc-examples-section deferred to future documentation phase"
  - "yard-server lint issues out of scope per v1.12 precedent (yard-server excluded from audit)"

patterns-established:
  - "Integration test lint suppression: #![allow(clippy::unwrap_used, clippy::expect_used)] as first line before //! doc comments"

requirements-completed: [OWN-01, ERR-01, MEM-01, API-01, ASYNC-01, OPT-01, NAME-01, TYPE-01, TEST-01, DOC-01, PERF-01, PROJ-01, LINT-01, ANTI-01]

coverage:
  - id: D1
    description: "V-01 fix: removed unused AirflowMajorVersion import from airflow_dag/mod.rs test module"
    requirement: LINT-01
    verification:
      - kind: automated_ui
        ref: "cargo clippy -p yard-core --all-targets -- -D warnings (zero warnings)"
        status: pass
    human_judgment: false
  - id: D2
    description: "V-02 through V-07 fix: added lint suppression attributes to 6 integration test files"
    requirement: TEST-01
    verification:
      - kind: automated_ui
        ref: "cargo clippy -p yard-core --all-targets -- -D warnings (zero warnings)"
        status: pass
    human_judgment: false
  - id: D3
    description: "All 21 remaining yard-core source files audited against 14 rule categories -- no new violations found"
    verification:
      - kind: other
        ref: "Per-file audit findings documented below"
        status: pass
    human_judgment: false
  - id: D4
    description: "yard-structs needless borrow regression fixed (lines 1418, 1431 in config.rs test code)"
    requirement: LINT-01
    verification:
      - kind: automated_ui
        ref: "cargo clippy -p yard-structs --all-targets -- -D warnings (zero warnings)"
        status: pass
    human_judgment: false

duration: 4min
completed: 2026-08-18
status: complete
---

# Phase 65 Plan 03: yard-core Remaining Files Audit & Integration Test Fixes Summary

**7 clippy violations fixed (unused import + lint suppression on 6 integration test files), 21 source files audited against 179 rules with zero new violations**

## Performance

- **Duration:** 4 min
- **Started:** 2026-08-18T01:30:52Z
- **Completed:** 2026-08-18T01:35:34Z
- **Tasks:** 2
- **Files modified:** 8

## Accomplishments

- Fixed V-01: removed unused `AirflowMajorVersion` import from `airflow_dag/mod.rs` test module
- Fixed V-02 through V-07: added `#![allow(clippy::unwrap_used, clippy::expect_used)]` to all 6 integration test files
- Audited all 21 remaining yard-core source files against 14 rule categories -- zero new violations found
- Fixed yard-structs regression: 2 needless borrows in `config.rs` test code (missed in Phase 64 P01)
- Full audit-scoped clippy clean: `cargo clippy -p yard-structs -p yard-core -p yard --all-targets -- -D warnings` passes with zero warnings

## Per-File Audit Findings

### airflow_dag/ (8 files)

| File | LOC | Findings |
|------|-----|----------|
| `mod.rs` | 30+2943 | V-01 fixed (unused import). //! module doc, pub use re-exports, cfg(test) + lint suppression on test module. Compliant. |
| `version.rs` | 122+88 | N-01 confirmed: 5 unreachable!() wildcard arms required for #[non_exhaustive] AirflowMajorVersion. #[inline] on small methods, #[must_use] on all trait methods. Compliant. |
| `connections.rs` | 168 | pub visibility on parse_account_from_role_arn intentional (re-exported in mod.rs). //! module doc, proper error propagation with anyhow. Compliant. |
| `collection.rs` | 247 | //! module doc, /// on public items, iterator patterns, proper error context. Compliant. |
| `generation.rs` | 450 | //! module doc, # Errors on public functions, no prod unwrap. Compliant. |
| `helpers.rs` | 79 | //! module doc, #[must_use] on pure functions, #[inline] on small helpers. Compliant. |
| `resolve.rs` | 219 | //! module doc, topological sort with proper error handling. Compliant. |
| `triggers.rs` | 614+1212 | //! module doc, comprehensive trigger rendering, cfg(test) + lint suppression. Compliant. |

### providers/ (3 files)

| File | LOC | Findings |
|------|-----|----------|
| `mod.rs` | 289 | N-05 confirmed: Pin<Box<dyn Future>> on Provider trait required for object safety with Box<dyn Provider>. //! module doc, trait docs with # Errors. Compliant. |
| `glue.rs` | 534+66 | tokio::fs usage confirmed (not std::fs). anyhow::Result with context. No prod unwrap. Compliant. |
| `emr.rs` | 308+64 | Same patterns as glue.rs. tokio::fs, proper error handling. Compliant. |

### Root source files (10 files)

| File | LOC | Findings |
|------|-----|----------|
| `storage.rs` | 1038+869 | N-06 confirmed: Pin<Box<dyn Future>> on StorageBackend required for object safety. tokio::fs usage. //! module doc, trait docs. Compliant. |
| `orchestrate.rs` | 780+551 | Async patterns correct (no lock-across-await). //! module doc, # Errors on public functions. cfg(test) + lint suppression. Compliant. |
| `resolve.rs` | 743+584 | +2 LOC parse_mask_pii integration follows existing patterns. //! module doc, # Errors. Compliant. |
| `dag_lifecycle.rs` | 644+502 | Async patterns match orchestrate.rs. //! module doc, # Errors. Compliant. |
| `config_merge.rs` | 70+112 | //! module doc, clean merge logic. Compliant. |
| `diff.rs` | 105+150 | //! module doc, iterator patterns, no unnecessary collect(). Compliant. |
| `show.rs` | 77 | //! module doc, proper error propagation. Compliant. |
| `list_targets.rs` | 111+171 | //! module doc, iterator patterns. Compliant. |
| `utils.rs` | 122+157 | N-02 confirmed: expect("static regex") valid per err-expect-bugs-only. //! module doc. Compliant. |
| `lib.rs` | 65 | #![warn(missing_docs)] present. pub use parse_mask_pii re-export present. //! module doc. Compliant. |

### Deferred Findings

| ID | File | Rule | Disposition |
|----|------|------|-------------|
| D-05 | state types (consumed by storage.rs, orchestrate.rs) | TYPE-01 | HashMap<String, Deployment> deferred for backward compat |
| D-06 | All public items | doc-examples-section | Zero # Examples sections; deferred to future documentation phase |
| N-09 | yard-server/ | LINT-01 | Out of scope per v1.12 precedent (yard-server excluded from audit) |

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix V-01 through V-07 + audit airflow_dag/** - `11d2d9e` (fix)
2. **Task 2: Audit providers/, storage, orchestrate, resolve, and remaining root files + final gate** - `ad2b273` (fix)

## Files Created/Modified

- `yard-core/src/airflow_dag/mod.rs` - Removed unused AirflowMajorVersion import from test module (V-01)
- `yard-core/tests/glue_integration.rs` - Added lint suppression inner attribute (V-02)
- `yard-core/tests/emr_integration.rs` - Added lint suppression inner attribute (V-03)
- `yard-core/tests/phase9_integration.rs` - Added lint suppression inner attribute (V-04)
- `yard-core/tests/common/mod.rs` - Added lint suppression inner attribute (V-05)
- `yard-core/tests/plan_target_integration.rs` - Added lint suppression inner attribute (V-06)
- `yard-core/tests/target_integration.rs` - Added lint suppression inner attribute (V-07)
- `yard-structs/src/config.rs` - Removed needless borrows in test serde calls (Rule 3 deviation)

## Decisions Made

- V-06/V-07 added for consistency even though files have 0 unwrap/expect calls -- prevents future regressions when tests are added
- D-05 honored: HashMap<String, Deployment> TYPE-01 finding remains deferred for backward compat
- D-06 honored: doc-examples-section deferred to future documentation phase
- yard-server lint issues out of scope per v1.12 precedent

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Fixed needless borrows in yard-structs test code**
- **Found during:** Task 2 (workspace clippy gate)
- **Issue:** `serde_json::to_value(&parsed)` on Copy type `AirflowMajorVersion` at lines 1418, 1431 in `yard-structs/src/config.rs` -- triggers `clippy::needless_borrows_for_generic_args`. These were missed in Phase 64 P01 fix (commit 99b0f83).
- **Fix:** Changed `&parsed` to `parsed` in both locations
- **Files modified:** yard-structs/src/config.rs
- **Verification:** `cargo clippy -p yard-structs --all-targets -- -D warnings` passes with zero warnings
- **Committed in:** ad2b273

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Fix was necessary to pass the workspace clippy gate. Minimal scope (2 character deletions in test code).

## Issues Encountered

- yard-server has pre-existing clippy warnings (unwrap/expect in tests without lint suppression). These are out of scope for this audit per v1.12 precedent (yard-server excluded). The workspace-wide `--workspace` clippy gate must be scoped to `yard-structs + yard-core + yard` (CLI) for this phase.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 65 complete: all 31 yard-core source files + 6 integration test files audited against 179 rules
- Combined with Phase 64 (yard-structs + yard-cli), the full v1.16 Rules Compliance Audit milestone is complete
- All audit-scoped crates pass clippy --all-targets with -D warnings
- All tests pass with zero failures

---
*Phase: 65-yard-core-audit-fix*
*Completed: 2026-08-18*
