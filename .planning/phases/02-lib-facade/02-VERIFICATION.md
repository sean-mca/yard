---
phase: 02-lib-facade
verified: 2026-04-18T17:30:00Z
status: gaps_found
score: 3/4
overrides_applied: 0
gaps:
  - truth: "Each extracted sub-module is a self-contained file of 100-400 lines"
    status: partial
    reason: "4 of 6 modules fall outside the 100-400 line range. orchestrate.rs (901 lines total, 470 prod + 431 test), dag_lifecycle.rs (634 total, 414 prod + 220 test), parsing.rs (417 lines) exceed 400. diff.rs (70 lines) and show.rs (28 lines) are under 100. The overages are primarily from co-located tests (which the CONTEXT.md left to Claude's discretion), and production code is only slightly over for dag_lifecycle (414) and parsing (417). orchestrate.rs production code at 470 lines is the largest deviation."
    artifacts:
      - path: "yard-core/src/orchestrate.rs"
        issue: "901 total lines (470 prod + 431 test) exceeds 400-line ceiling"
      - path: "yard-core/src/dag_lifecycle.rs"
        issue: "634 total lines (414 prod + 220 test) exceeds 400-line ceiling"
      - path: "yard-core/src/parsing.rs"
        issue: "417 total lines, slightly over 400-line ceiling"
      - path: "yard-core/src/diff.rs"
        issue: "70 lines, under 100-line floor"
      - path: "yard-core/src/show.rs"
        issue: "28 lines, under 100-line floor"
    missing:
      - "Decision: accept current sizes or extract tests to separate files to bring totals closer to 100-400 range"
---

# Phase 2: lib.rs Facade Extraction Verification Report

**Phase Goal:** lib.rs contains only mod declarations and re-exports; all business logic lives in focused sub-modules
**Verified:** 2026-04-18T17:30:00Z
**Status:** gaps_found
**Re-verification:** No -- initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | lib.rs is under 100 lines with only mod declarations, pub use re-exports, and no business logic | VERIFIED | 31 lines, 0 fn declarations, 13 pub mod + 5 pub use blocks |
| 2 | Each extracted sub-module is a self-contained file of 100-400 lines | FAILED | 4 of 6 modules outside range: orchestrate.rs=901, dag_lifecycle.rs=634, parsing.rs=417, diff.rs=70, show.rs=28. Production-only counts: orchestrate=470, dag_lifecycle=414, parsing=417, diff=70, show=28 |
| 3 | yard-cli and yard-server compile without any source changes | VERIFIED | `cargo check -p yard -p yard-server` succeeds with zero errors |
| 4 | All existing tests pass without changes to test logic | VERIFIED | 208 tests pass via `cargo test -p yard-core --lib`, clippy clean |

**Score:** 3/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `yard-core/src/parsing.rs` | All parse functions and private helpers, min 250 lines | VERIFIED | 417 lines, 12 pub fns, 5 private helpers, 9 tests |
| `yard-core/src/config_merge.rs` | Config merge functions, min 40 lines | VERIFIED | 161 lines, 3 pub fns, 9 tests |
| `yard-core/src/diff.rs` | Diff calculation logic, min 40 lines | VERIFIED | 70 lines, 1 pub fn + 1 private fn |
| `yard-core/src/show.rs` | Show/preview functions, min 15 lines | VERIFIED | 28 lines, 2 pub fns |
| `yard-core/src/orchestrate.rs` | Core apply/destroy orchestration, min 200 lines | VERIFIED | 901 lines (470 prod), 9 pub fns/structs, 11 tests |
| `yard-core/src/dag_lifecycle.rs` | DAG lifecycle logic, min 300 lines | VERIFIED | 634 lines (414 prod), 8 pub fns/structs, 3 private helpers, 5 tests |
| `yard-core/src/lib.rs` | Facade with mod/pub use only | VERIFIED | 31 lines, 0 fn declarations |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| lib.rs | parsing.rs | `pub use parsing::` | WIRED | Re-exports 12 public parse functions |
| lib.rs | config_merge.rs | `pub use config_merge::` | WIRED | Re-exports build_provider_config, is_task_only, merge_provider_config |
| lib.rs | diff.rs | `pub use diff::calculate_diff` | WIRED | Re-export present |
| lib.rs | show.rs | `pub use show::{show, show_dag}` | WIRED | Re-export present |
| lib.rs | orchestrate.rs | `pub use orchestrate::` | WIRED | Re-exports apply, destroy_all, destroy_job, force_unlock, init_state_backend, load_state, verify_deployed_resources, ApplyResult, DestroyResult |
| lib.rs | dag_lifecycle.rs | `pub use dag_lifecycle::` | WIRED | Re-exports apply_dags, calculate_dag_diffs, destroy_all_dags, destroy_dag, load_dag_state, DagApplyResult, DagDestroyResult |
| orchestrate.rs | diff.rs | `use crate::diff::calculate_diff` | WIRED | Import at line 17 |
| orchestrate.rs | config_merge.rs | `use crate::config_merge::{build_provider_config, is_task_only}` | WIRED | Import at line 16 |
| orchestrate.rs | dag_lifecycle.rs | `use crate::dag_lifecycle::{apply_dags, destroy_all_dags}` | WIRED | Import at line 18 |
| dag_lifecycle.rs | config_merge.rs | `use crate::config_merge::merge_provider_config` | WIRED | Import at line 15 |
| dag_lifecycle.rs | parsing.rs | `use crate::parsing::parse_airflow_section` | WIRED | Import at line 16 |
| diff.rs | codegen.rs | `use crate::codegen` | WIRED | Import at line 6 |
| show.rs | airflow_dag.rs | `crate::airflow_dag::` inline paths | WIRED | Used at lines 7, 15 |

### Data-Flow Trace (Level 4)

Not applicable -- this is a structural refactor with no new data rendering.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All lib tests pass | `cargo test -p yard-core --lib` | 208 passed, 0 failed | PASS |
| Clippy clean | `cargo clippy --workspace -- -D warnings` | Zero warnings | PASS |
| Downstream compiles | `cargo check -p yard -p yard-server` | Success | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| MOD-01 | 02-01, 02-02, 02-03 | lib.rs split into focused sub-modules with lib.rs as facade of re-exports only | SATISFIED | lib.rs is 31-line facade; 6 sub-modules created with all business logic |
| MOD-05 | 02-03 (enforced) | All existing tests pass after module split with zero changes to test logic | SATISFIED | 208 tests pass |
| MOD-06 | 02-03 (enforced) | No public API changes -- yard-cli and yard-server compile without modifications | SATISFIED | `cargo check -p yard -p yard-server` succeeds |
| MOD-07 | 02-03 (enforced) | All code passes cargo clippy -D warnings | SATISFIED | Zero warnings |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | - | - | - | No anti-patterns found. All unwrap() calls are in #[cfg(test)] blocks only. No unsafe, no TODO/FIXME/PLACEHOLDER in new modules. |

### Human Verification Required

No human verification items identified. This is a pure mechanical refactor with no UI, external services, or runtime behavior changes.

### Gaps Summary

One roadmap success criterion is not met: SC #2 requires each sub-module to be "a self-contained file of 100-400 lines." Four modules fall outside this range.

The overages in orchestrate.rs (901) and dag_lifecycle.rs (634) are primarily from co-located test code (431 and 220 test lines respectively). Production code alone is 470 and 414 lines -- close to the 400-line ceiling. The CONTEXT.md explicitly left test placement to Claude's discretion, so co-locating tests was a valid choice.

diff.rs (70 lines) and show.rs (28 lines) are under the 100-line floor, but these modules are inherently small -- they contain complete, self-contained functionality that doesn't warrant padding.

**This looks intentional.** The line counts reflect natural code boundaries rather than arbitrary size targets. To accept this deviation, add to VERIFICATION.md frontmatter:

```yaml
overrides:
  - must_have: "Each extracted sub-module is a self-contained file of 100-400 lines"
    reason: "Module sizes follow natural code boundaries. Overages are from co-located tests (per CONTEXT.md discretion). Production code is within 15% of ceiling. Under-100 modules (diff, show) are complete and self-contained."
    accepted_by: "sean"
    accepted_at: "2026-04-18T00:00:00Z"
```

---

_Verified: 2026-04-18T17:30:00Z_
_Verifier: Claude (gsd-verifier)_
