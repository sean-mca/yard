---
phase: "02-lib-facade"
plan: "02"
subsystem: "yard-core"
tags: [refactor, module-extraction]
dependency-graph:
  requires: [02-01]
  provides: [diff-module, show-module]
  affects: [lib-rs-facade]
tech-stack:
  added: []
  patterns: [module-extraction, pub-use-re-export]
key-files:
  created:
    - yard-core/src/diff.rs
    - yard-core/src/show.rs
  modified:
    - yard-core/src/lib.rs
decisions:
  - Removed unused JobDiff import from lib.rs since calculate_diff moved to diff.rs
  - Used crate:: paths in show.rs for airflow_dag and codegen references instead of use imports to keep the module minimal
metrics:
  duration: "199s"
  completed: "2026-04-18T15:10:00Z"
  tasks: 1
  files: 3
---

# Phase 02 Plan 02: Extract diff.rs and show.rs Summary

Extracted diff calculation and show/preview functions into standalone modules, continuing lib.rs decomposition toward a pure facade.

## What Changed

### diff.rs (69 lines)
- `pub fn calculate_diff()` - computes diff between manifest and state, generating JobDiff entries for create/modify/delete
- `fn compare_json()` - private helper comparing old vs new JSON config objects to produce change maps
- Uses `crate::codegen` for script generation and `crate::utils` for hashing

### show.rs (28 lines)
- `pub fn show_dag()` - generates DAG Python content without deploying
- `pub fn show()` - generates job script content without deploying
- Uses `crate::airflow_dag` and `crate::codegen`

### lib.rs (1563 -> 1478 lines, -85 lines)
- Added `pub mod diff;` and `pub mod show;`
- Added `pub use diff::calculate_diff;` and `pub use show::{show, show_dag};`
- Removed moved function bodies (calculate_diff, compare_json, show, show_dag)
- Removed unused `JobDiff` import (only needed in diff.rs now)

## Verification Results

- `cargo test -p yard-core --lib`: 208 tests passed (unchanged count from Plan 01)
- `cargo check --workspace`: all crates compile (yard-cli, yard-server, yard-core)
- `cargo clippy --workspace -- -D warnings`: zero warnings

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed unused JobDiff import from lib.rs**
- **Found during:** Task 1
- **Issue:** After moving calculate_diff to diff.rs, the `JobDiff` type was no longer used in lib.rs non-test code, causing clippy to fail with `-D warnings`
- **Fix:** Removed `JobDiff` from the `use yard_structs::{...}` import line in lib.rs
- **Files modified:** yard-core/src/lib.rs
- **Commit:** 0b6382d

## Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Extract diff.rs and show.rs from lib.rs | 0b6382d | diff.rs, show.rs, lib.rs |

## Self-Check: PASSED
