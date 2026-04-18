---
phase: "02-lib-facade"
plan: "03"
subsystem: "yard-core"
tags: [refactor, module-extraction, facade]
dependency-graph:
  requires: [02-01, 02-02]
  provides: [orchestrate-module, dag-lifecycle-module, lib-rs-facade]
  affects: []
tech-stack:
  added: []
  patterns: [module-extraction, pub-use-re-export, facade-pattern]
key-files:
  created:
    - yard-core/src/orchestrate.rs
    - yard-core/src/dag_lifecycle.rs
  modified:
    - yard-core/src/lib.rs
decisions:
  - Made apply_dags pub during Task 1 interim state so orchestrate.rs could call it while dag_lifecycle code still lived in lib.rs
  - Used crate::dag_lifecycle imports in orchestrate.rs for apply_dags and destroy_all_dags cross-module calls
  - Removed unused Context import from lib.rs anyhow usage (Rule 1 - clippy compliance)
metrics:
  duration: "585s"
  completed: "2026-04-18T16:00:00Z"
  tasks: 2
  files: 3
---

# Phase 02 Plan 03: Extract orchestrate.rs, dag_lifecycle.rs, Finalize lib.rs Facade Summary

Extracted the two remaining business logic modules from lib.rs and reduced lib.rs to a 31-line pure facade with only pub mod declarations and pub use re-exports, completing the MOD-01 requirement.

## What Changed

### orchestrate.rs (901 lines)
- `load_state()` - reads per-job state files from backend
- `verify_deployed_resources()` - checks deployed resources via providers
- `apply()` - validates, diffs, deploys, updates state with locking
- `init_state_backend()` - validates backend reachability
- `force_unlock()` - force-unlocks a locked job
- `destroy_job()` - tears down single job resources/state/scripts
- `destroy_all()` - destroys all jobs and DAGs
- `ApplyResult` / `DestroyResult` structs
- 11 tests: 6 diff_detects_* tests, 3 async apply/destroy tests, 2 destroy edge case tests
- 4 test helpers: make_job, job_hash, make_deployment, empty_state

### dag_lifecycle.rs (634 lines)
- `load_dag_state()` - reads DAG deployment state from backend
- `calculate_dag_diffs()` - computes diff between resolved DAGs and stored state
- `apply_dags()` - generates, uploads, persists DAG changes
- `destroy_dag()` / `destroy_all_dags()` - tears down DAG resources/state/scripts
- `DagApplyResult` / `DagDestroyResult` structs
- 3 private helpers: compare_dag_config, resolve_aws_for_dir, extract_airflow_region
- 3 private async helpers: upload_dag_to_s3, delete_dag_from_s3, apply_dags internal state loading
- 5 tests: 4 dag_diff_* tests, 1 dag_upload_credentials_ignore_job_aws invariant test
- 3 test helpers: make_job, make_resolved_dag, make_dag_deployment

### lib.rs (1478 -> 31 lines, -1447 lines)
- Pure facade: 13 pub mod declarations, 5 pub use re-export blocks
- Zero fn declarations, zero use imports, zero business logic
- All 6 extracted modules (parsing, config_merge, diff, show, orchestrate, dag_lifecycle) plus 6 pre-existing modules (airflow_dag, codegen, providers, resolve, storage, utils, validation)

## Verification Results

- `cargo test -p yard-core --lib`: 208 tests passed (unchanged count)
- `cargo test --workspace --lib --tests`: all tests pass across all crates
- `cargo check --workspace`: all crates compile (yard-cli, yard-server, yard-core)
- `cargo clippy --workspace -- -D warnings`: zero warnings
- `wc -l yard-core/src/lib.rs`: 31 lines (under 100 threshold)
- `grep -c 'fn ' yard-core/src/lib.rs`: 0 (zero function declarations)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed unused Context import from lib.rs**
- **Found during:** Task 1
- **Issue:** After moving all orchestrate functions to orchestrate.rs, the `Context` trait import in lib.rs was unused, causing a clippy warning
- **Fix:** Changed `use anyhow::{Context, Result, anyhow}` to `use anyhow::{Result, anyhow}` in lib.rs
- **Files modified:** yard-core/src/lib.rs
- **Commit:** 9828d1c

**2. [Rule 3 - Blocking] Made apply_dags pub for cross-module access**
- **Found during:** Task 1
- **Issue:** `apply_dags` was private (`async fn`) in lib.rs but needed by orchestrate.rs. During the interim state between Task 1 and Task 2, orchestrate.rs needed to call it across module boundaries.
- **Fix:** Changed to `pub async fn apply_dags` in lib.rs (Task 1 interim), then moved to dag_lifecycle.rs where it remains pub (Task 2 final)
- **Files modified:** yard-core/src/lib.rs, yard-core/src/orchestrate.rs
- **Commit:** 9828d1c (interim), 260cc4c (final)

## Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Extract orchestrate.rs from lib.rs | 9828d1c | orchestrate.rs, lib.rs |
| 2 | Extract dag_lifecycle.rs and finalize lib.rs facade | 260cc4c | dag_lifecycle.rs, lib.rs, orchestrate.rs |

## Self-Check: PASSED
