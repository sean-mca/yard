# Phase 65: yard-core Audit & Fix - Pattern Map

**Mapped:** 2026-08-17
**Files analyzed:** 31 production + 6 integration test files
**Analogs found:** 7 / 7 (all fixes have direct analogs)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `yard-core/src/airflow_dag/mod.rs` | test-module | N/A (unused import removal) | self (line 146) | exact |
| `yard-core/tests/glue_integration.rs` | test | request-response | `src/parsing.rs` lines 681-683 | exact |
| `yard-core/tests/emr_integration.rs` | test | request-response | `src/parsing.rs` lines 681-683 | exact |
| `yard-core/tests/phase9_integration.rs` | test | request-response | `src/parsing.rs` lines 681-683 | exact |
| `yard-core/tests/plan_target_integration.rs` | test | request-response | `src/parsing.rs` lines 681-683 | exact |
| `yard-core/tests/target_integration.rs` | test | request-response | `src/parsing.rs` lines 681-683 | exact |
| `yard-core/tests/common/mod.rs` | test-utility | N/A | `src/parsing.rs` lines 681-683 | exact |

## Pattern Assignments

### `yard-core/src/airflow_dag/mod.rs` (test-module, unused import fix)

**Analog:** self (the same file's import block)

**Fix pattern** (line 145-148 current):
```rust
// BEFORE:
    use yard_structs::{
        AirflowJobBlock, AirflowMajorVersion, AwsCredentialConfig, Deployment, DeploymentStatus,
        JobName, JobType, ProjectManifest, Resource, StateBackend,
    };

// AFTER: remove AirflowMajorVersion
    use yard_structs::{
        AirflowJobBlock, AwsCredentialConfig, Deployment, DeploymentStatus,
        JobName, JobType, ProjectManifest, Resource, StateBackend,
    };
```

---

### Integration test files: `glue_integration.rs`, `emr_integration.rs`, `phase9_integration.rs`, `plan_target_integration.rs`, `target_integration.rs` (test, lint suppression)

**Analog:** `yard-core/src/parsing.rs` lines 681-683

**Lint suppression pattern for integration test crate roots** (add as first line of file):
```rust
#![allow(clippy::unwrap_used, clippy::expect_used)]
```

Note: Integration test files are separate crates, so they use `#!` (inner attribute) syntax at file top, matching the workspace lint convention where `unwrap_used = "deny"` is set globally and test modules opt out.

---

### `yard-core/tests/common/mod.rs` (test-utility, lint suppression)

**Analog:** `yard-core/src/parsing.rs` lines 681-683

**Lint suppression pattern for non-crate-root test module** (add before module items):
```rust
#[allow(clippy::unwrap_used, clippy::expect_used)]
```

Note: `common/mod.rs` is not a crate root, so it uses `#[allow]` (outer attribute), not `#![allow]` (inner attribute). However, since it is included via `mod common;` from integration test files, it can alternatively use inner attribute syntax. The established yard-core pattern for test modules uses outer `#[allow]` on the `mod tests` block (see `src/parsing.rs:682`, `src/orchestrate.rs:781`, etc.).

## Shared Patterns

### Test Module Lint Suppression
**Source:** `yard-core/src/parsing.rs` lines 681-683
**Apply to:** All test modules and integration test files

```rust
// For inline test modules (src/*.rs):
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
```

```rust
// For integration test crate roots (tests/*.rs):
#![allow(clippy::unwrap_used, clippy::expect_used)]
```

### Audit Verification (no code pattern -- process pattern)
**Source:** RESEARCH.md "Sampling Rate" section
**Apply to:** All 3 plans as gate check

Each plan must verify:
1. `cargo clippy -p yard-core --all-targets -- -D warnings` (zero warnings)
2. `cargo test -p yard-core` (all tests pass)

### New Code Exemplar (for deep audit comparison)
**Source:** `yard-core/src/codegen/pii.rs` (entire file, 151 LOC)
**Apply to:** Deep audit of all new code (codegen, validation, parsing additions)

This file demonstrates full compliance across all 14 rule categories:
- `String::with_capacity(256)` (mem-with-capacity)
- `write!`/`writeln!` over `format!` (mem-write-over-format)
- `#[must_use]` (api-must-use)
- `pub(super)` visibility (proj-pub-super-parent)
- Complete `//!` module docs + `///` function docs (doc-all-public)
- Descriptive test names with AAA pattern (test-descriptive-names)

## No Analog Found

No files lack analogs. All modifications have direct precedent in the existing codebase.

## Metadata

**Analog search scope:** `yard-core/src/`, `yard-core/tests/`
**Files scanned:** 37 (31 src + 6 test)
**Pattern extraction date:** 2026-08-17
