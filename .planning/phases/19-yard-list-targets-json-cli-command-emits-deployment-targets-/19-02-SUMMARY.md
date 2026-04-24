---
phase: 19-yard-list-targets-json-cli-command-emits-deployment-targets
plan: 02
subsystem: yard-core/dag_lifecycle
tags: [rust, visibility, yard-core, dag-lifecycle, refactor]
dependency-graph:
  requires: []
  provides:
    - "yard-core::dag_lifecycle::resolve_effective_dag_aws callable from sibling modules in yard-core"
  affects:
    - "Unblocks Plan 19-03 (yard-core/src/list_targets.rs) to call resolve_effective_dag_aws for DAG-side aws_account_id derivation (D-02)"
tech-stack:
  added: []
  patterns:
    - "Minimum-exposure visibility bump (pub(crate), not pub) per PRES-05"
key-files:
  created: []
  modified:
    - yard-core/src/dag_lifecycle.rs
decisions:
  - "Chose pub(crate) over pub — Plan 03's list_targets module lives in yard-core/src/ as a sibling of dag_lifecycle.rs, so pub(crate) is sufficient; pub would needlessly leak the resolver to yard-cli and yard-server."
  - "Did not touch the sibling resolve_destroy_dag_aws (line 311) — it remains private; Plan 03 does not need it."
  - "Did not add new tests — existing dag_upload_credentials_* tests exercise resolve_effective_dag_aws through upload_dag_to_s3, which is unchanged. A visibility change introduces no new behavior that requires coverage."
metrics:
  duration: "~3min"
  completed: "2026-04-24T15:39:38Z"
  tasks: 1
  files: 1
---

# Phase 19 Plan 02: Bump resolve_effective_dag_aws to pub(crate) Summary

Single-keyword visibility bump on `yard-core/src/dag_lifecycle.rs::resolve_effective_dag_aws` from module-private `fn` to `pub(crate) fn`, unblocking Plan 19-03 to import the authoritative DAG-side AWS resolver for the new `list_targets` module.

## What Changed

`yard-core/src/dag_lifecycle.rs` line 295:

**Before:**

```rust
fn resolve_effective_dag_aws(manifest: &ProjectManifest, dag: &airflow_dag::ResolvedDag) -> Value {
```

**After:**

```rust
pub(crate) fn resolve_effective_dag_aws(manifest: &ProjectManifest, dag: &airflow_dag::ResolvedDag) -> Value {
```

Function body, doc comment (lines 282–294), signature shape, and the in-file caller (`upload_dag_to_s3`, line 336) are all unchanged.

Commit: `4d7e2f5` — `refactor(19-02): bump resolve_effective_dag_aws to pub(crate)`
Diff scope: 1 file, 1 insertion(+), 1 deletion(-). Exact match to the plan's acceptance criterion.

## Why

Phase 19 D-02 says the DAG row's `aws_account_id` is derived by calling the authoritative DAG aws resolver — `resolve_effective_dag_aws` — and reading `.assume_role` off the returned `Value`. That function was module-private with only `upload_dag_to_s3` as a consumer. Bumping to `pub(crate)` is the minimum exposure that unblocks Plan 03 without leaking the function to downstream crates (yard-cli, yard-server) — PRES-05.

## Verification

All gates green, captured 2026-04-24:

| Gate | Command | Result |
|------|---------|--------|
| `pub(crate)` present | `grep -c "pub(crate) fn resolve_effective_dag_aws" yard-core/src/dag_lifecycle.rs` | `1` |
| Private `fn` gone | `grep -c "^fn resolve_effective_dag_aws" yard-core/src/dag_lifecycle.rs` | `0` |
| Not over-bumped to `pub` | `grep -c "^pub fn resolve_effective_dag_aws" yard-core/src/dag_lifecycle.rs` | `0` |
| Existing caller intact | `grep -c "let effective_aws = resolve_effective_dag_aws(manifest, dag);" yard-core/src/dag_lifecycle.rs` | `1` |
| yard-core builds | `cargo build --package yard-core` | exit 0 |
| yard-core tests pass | `cargo test --package yard-core` | 241 passed, 0 failed (+ 4+4 integration, 0 doctests) |
| Workspace clippy zero | `cargo clippy --all-targets --workspace -- -D warnings` | exit 0 |
| Diff is 1/1/1 | `git diff --stat yard-core/src/dag_lifecycle.rs` (pre-commit) | `1 file changed, 1 insertion(+), 1 deletion(-)` |

## Plan 03 Unblocked

Plan 19-03 (`yard-core/src/list_targets.rs`) can now call:

```rust
use crate::dag_lifecycle;
// ...
let effective = dag_lifecycle::resolve_effective_dag_aws(manifest, dag);
```

as a sibling-module crate-internal call — no further visibility work needed on this resolver.

## Deviations from Plan

None — plan executed exactly as written. Single-line `fn` → `pub(crate) fn` swap, no doc changes, no body changes, no new tests, no Cargo.toml changes.

**Worktree-shared-index note (informational, not a deviation):** the worktree's index also carried staged changes to `yard-core/src/airflow_dag/connections.rs` and a subsequent unstaged change to `yard-core/src/airflow_dag/mod.rs` originating from the sibling wave-1 plan (19-01, `parse_account_from_role_arn` factor-out per D-03). Those were left untouched — my commit staged only `yard-core/src/dag_lifecycle.rs` by explicit path, matching the plan's single-file scope. The sibling agent owns its own commit boundary for 19-01.

## Threat Flags

None — this is a crate-internal visibility change with no new surface beyond `yard-core` (T-19-05 disposition: mitigate via `pub(crate)` choice, not `pub`; achieved).

## Self-Check: PASSED

- **File exists:** `yard-core/src/dag_lifecycle.rs` — FOUND (modified)
- **Commit exists:** `4d7e2f5` — FOUND in branch `gsd/phase-19-yard-list-targets-json-cli-command-emits-deployment-targets`
- **Author:** `Sean McAuliffe <smcauliffe240@gmail.com>` — verified via `git log -1 --format='%an <%ae>'`
- **Diff stat:** `1 file changed, 1 insertion(+), 1 deletion(-)` — exact match to acceptance criterion
- **yard-core tests:** 241 passed, 0 failed
- **Workspace clippy:** zero warnings
