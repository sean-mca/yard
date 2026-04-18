---
phase: "02-lib-facade"
plan: "01"
subsystem: "yard-core"
tags: [refactor, module-extraction]
dependency-graph:
  requires: []
  provides: [parsing-module, config-merge-module]
  affects: [lib-rs-facade]
tech-stack:
  added: []
  patterns: [module-extraction, pub-use-re-export]
key-files:
  created:
    - yard-core/src/config_merge.rs
    - yard-core/src/parsing.rs
  modified:
    - yard-core/src/lib.rs
decisions:
  - Removed unused type imports from lib.rs (AirflowJobBlock, AirflowSection, Import, Sink, Source, Transform) since they are only needed in parsing.rs now
  - Kept pub use re-exports in lib.rs so all downstream call sites remain unchanged
metrics:
  duration: "549s"
  completed: "2026-04-18T15:00:00Z"
  tasks: 1
  files: 3
---

# Phase 02 Plan 01: Extract Leaf Modules (parsing.rs, config_merge.rs) Summary

Extracted two leaf modules from lib.rs with zero cross-dependencies, establishing the foundation for orchestrate.rs and dag_lifecycle.rs extraction in later plans.

## What Changed

### config_merge.rs (161 lines)
- `is_task_only()` - single source of truth for task-only job types
- `build_provider_config()` - provider defaults merged with job overrides plus _aws injection
- `merge_provider_config()` - recursive deep merge for provider config
- 9 tests covering all merge scenarios (deep merge, arrays, scalars, multiple levels) and is_task_only behavior

### parsing.rs (417 lines)
- 12 public parse functions: `parse_body`, `parse_job_file`, `parse_airflow_section`, `parse_airflow_job_block`, `merge_airflow_sections`, `parse_partition_by`, `parse_partition_timestamp_column`, `parse_create_timestamp`, `parse_imports`, `parse_sources`, `parse_sink`, `parse_transforms`
- 5 private helpers: `str_field`, `str_array_field`, `str_map_field`, `order_by_field`, `parse_single_source`
- 9 tests covering airflow section parsing, job block parsing, and section merging

### lib.rs (2114 -> 1563 lines, -551 lines)
- Added `pub mod config_merge;` and `pub mod parsing;`
- Added `pub use` re-exports for all extracted public functions
- Removed moved function bodies and tests
- Removed type imports only used by moved code

## Verification Results

- `cargo test -p yard-core --lib`: 208 tests passed (9 config_merge + 9 parsing + 190 remaining)
- `cargo check --workspace`: all crates compile (yard-cli, yard-server, yard-core)
- `cargo clippy --workspace -- -D warnings`: zero warnings

## Deviations from Plan

None - plan executed exactly as written.

## Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Extract parsing.rs and config_merge.rs from lib.rs | f92f861 | config_merge.rs, parsing.rs, lib.rs |

## Self-Check: PASSED
