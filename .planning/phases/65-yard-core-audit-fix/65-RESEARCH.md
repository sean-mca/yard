# Phase 65: yard-core Audit & Fix - Research

**Researched:** 2026-08-18
**Domain:** Rust coding standards compliance audit (179 rules across 14 categories) -- yard-core crate
**Confidence:** HIGH

## Summary

Phase 65 is a full re-audit of all 31 source files in yard-core against the 179 Rust coding rules in `rules/`. This is the second audit of yard-core (Phase 59/v1.12 was the baseline, June 2026). Since v1.12, 685 lines were added across 13 files: PII codegen (`codegen/pii.rs`, `codegen/mod.rs`), PII validation (`validation/mod.rs`, `validation/rules.rs`), Iceberg column reorder (`codegen/sink.rs`), `parse_mask_pii` in `parsing.rs`, and minor integration touches across `resolve.rs`, `orchestrate.rs`, `dag_lifecycle.rs`, `lib.rs`, `airflow_dag/connections.rs`, `airflow_dag/mod.rs`, and `codegen/helpers.rs`.

The production code is in strong shape. `cargo clippy -p yard-core --lib -- -D warnings` passes with zero warnings. All 470 unit tests and 4 doc tests pass. No `unwrap()` in production code. The two `expect()` calls in production code (`utils.rs` line 17 and `validation/rules.rs` line 44) are static regex patterns -- valid per `err-expect-bugs-only`. All 31 files have `//!` module-level documentation. `# Errors` sections cover all public fallible functions.

The primary work is: (1) fix clippy `--all-targets` failures in integration test files (missing lint suppression attributes and one unused import), (2) deep audit the ~685 LOC of new code against all 14 rule categories, (3) lightweight re-verify unchanged files to confirm v1.12 findings still hold. The new code is well-written: `codegen/pii.rs` uses `with_capacity`, `write!` over `format!`, proper docs; `validation/rules.rs` uses `LazyLock<Regex>`, `HashSet::with_capacity`; `parsing.rs` uses idiomatic `Option` chaining.

**Primary recommendation:** Split into 3 plans per D-01 (codegen, validation+parsing, everything else), all in wave 1. Each plan audits its file set, fixes violations, and gates on `cargo clippy -p yard-core --all-targets -- -D warnings` + `cargo test -p yard-core`.

<user_constraints>

## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Split into 3 plans grouped by change density:
  - Plan 1: `codegen/` (6 files, ~3k LOC) -- has the most new code (PII codegen + Iceberg column reorder)
  - Plan 2: `validation/` + `parsing.rs` (4 files, ~3.7k LOC) -- second-most new code (PII validation rules)
  - Plan 3: everything else -- `airflow_dag/`, `providers/`, `storage.rs`, `resolve.rs`, `orchestrate.rs`, `dag_lifecycle.rs`, `config_merge.rs`, `diff.rs`, `show.rs`, `list_targets.rs`, `utils.rs`, `lib.rs` (21 files, ~14k LOC)
- **D-02:** All 3 plans run in wave 1 (parallel). No cross-plan dependencies.
- **D-03:** Deep audit on new code (~685 LOC across 13 changed files), lightweight re-verify on unchanged files.
- **D-04:** Fix anything contained within yard-core that doesn't change public API signatures or wire format. Defer findings that would require yard-cli changes, serde format changes, or structural rewrites spanning 5+ files.
- **D-05:** Carrying forward from Phase 64: `HashMap<String, Deployment>` TYPE-01 finding remains deferred for backward compat. Stringly-typed wire format fields are a design choice, not a violation.

### Claude's Discretion
None specified.

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.

</user_constraints>

<phase_requirements>

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OWN-01 | All production code passes the 12 ownership/borrowing rules | Per-file inventory below; no violations found in new or existing code |
| ERR-01 | All production code passes the 12 error handling rules | Two `expect()` calls verified valid (static regex, `err-expect-bugs-only`); consistent `anyhow::Result` + `?` usage throughout |
| MEM-01 | All production code passes the 15 memory rules | New code uses `with_capacity`, `write!`; `format!` in validation is per-error construction (not hot path) |
| API-01 | All public APIs pass the 15 API design rules | `#[must_use]` on pure functions, `#[non_exhaustive]` on cross-crate enums, common traits derived |
| ASYNC-01 | All async code passes the 15 async rules | `Pin<Box<dyn Future>>` pattern for object-safe traits, `tokio::fs` in storage.rs, no lock-across-await |
| OPT-01 | Release build config passes the 12 optimization rules | `lto = true`, `codegen-units = 1` in workspace profile; `#[inline]` on small hot methods in version.rs |
| NAME-01 | All identifiers pass the 16 naming rules | Types CamelCase, functions snake_case, consts SCREAMING_SNAKE; new `SCREAMING_SNAKE_RE` follows convention |
| TYPE-01 | All types pass the 10 type safety rules | `JobName`/`DagName` newtypes, enums for states, `Option` for nullable; D-05 deferred finding noted |
| TEST-01 | Test code follows the 13 testing rules | `#[cfg(test)]` modules with `use super::*`, descriptive names, AAA pattern; integration tests need lint suppression |
| DOC-01 | Public items have documentation per the 11 doc rules | All files have `//!` module docs; `# Errors` covers all public `Result` functions; zero `# Examples` sections (carry-forward gap) |
| PERF-01 | Performance patterns pass the 11 rules | Iterator-based patterns, `HashSet::with_capacity` for duplicate detection, no unnecessary `collect()` |
| PROJ-01 | Project structure follows the 11 rules | Feature-based modules, `pub use` re-exports in lib.rs, workspace deps inherited |
| LINT-01 | Lint configuration follows the 11 rules | Workspace lints: correctness=deny, suspicious=deny, style/complexity/perf=warn, unwrap_used=deny |
| ANTI-01 | No anti-pattern violations across the 15 rules | No unwrap abuse, no excessive cloning, no lock-across-await, no stringly-typed abuse |

</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| PySpark codegen (Tera templates) | yard-core/codegen | -- | Template rendering, PII masking block |
| Config validation | yard-core/validation | -- | Schema + semantic + PII entity validation rules |
| YAML parsing | yard-core/parsing | -- | YAML to typed struct conversions |
| DAG codegen (generation, triggers, version) | yard-core/airflow_dag | yard-structs (types) | All rendering logic; structs define shapes |
| Cloud provider deployment | yard-core/providers | -- | AWS Glue / EMR API calls |
| State persistence (local FS, S3) | yard-core/storage | -- | StorageBackend trait + Storage wrapper |
| Config resolution + hierarchy merging | yard-core/resolve | -- | Project discovery, YAML cascade |
| Apply/plan/destroy orchestration | yard-core/orchestrate | -- | Top-level entry points |
| Diff computation | yard-core/diff | -- | Manifest-vs-state comparison |
| DAG lifecycle | yard-core/dag_lifecycle | -- | DAG apply/destroy/diff operations |

## Standard Stack

This phase requires no new libraries. It is a code-quality audit against existing rules.

### Core (already in workspace)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| anyhow | 1.0.102 | Error handling | [VERIFIED: Cargo.toml] Application-level error propagation |
| serde | 1.0.228 | Serialization | [VERIFIED: Cargo.toml] Config/state serialization |
| serde_json | 1.0.149 | JSON | [VERIFIED: Cargo.toml] Wire format for state files |
| tokio | 1.50.0 | Async runtime | [VERIFIED: Cargo.toml] All async operations |
| tera | 1.20.0 | Template engine | [VERIFIED: Cargo.toml] PySpark script generation |
| regex | (workspace) | Regular expressions | [VERIFIED: Cargo.toml] PII entity type validation |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Adding SmallVec/CompactString | Flag as finding only | REQUIREMENTS.md excludes adding external crates for compliance |

## Architecture Patterns

### System Architecture Diagram

```
yard.yaml / job.yaml files
       |
       v
 [yard-structs types]  <-- Phase 64 (DONE)
       |
       v
 [yard-core logic]     <-- AUDIT TARGET (Phase 65)
  |--- parsing.rs         (YAML -> typed structs, incl. parse_mask_pii)
  |--- resolve.rs         (config cascade, project discovery)
  |--- config_merge.rs    (provider config deep-merge)
  |--- validation/        (schema + semantic + PII entity validation)
  |--- codegen/           (PySpark generation, PII masking block)
  |--- airflow_dag/       (DAG generation + triggers + version)
  |--- providers/         (Glue + EMR deployment via AWS SDK)
  |--- storage.rs         (state persistence: local FS + S3)
  |--- orchestrate.rs     (apply/plan/destroy entry points)
  |--- diff.rs            (manifest-vs-state diff)
  |--- dag_lifecycle.rs   (DAG apply/destroy lifecycle)
  |--- show.rs            (preview commands)
  |--- list_targets.rs    (CI target enumeration)
  |--- utils.rs           (hashing, variable resolution)
       |
       v
 [yard-cli handlers]   <-- Phase 64 (DONE)
       |
       v
 Terminal output / State files
```

### Source File Inventory

#### Plan 1: codegen/ (6 files)

| File | Prod LOC | Test LOC | New Since v1.12 | Key Audit Focus |
|------|----------|----------|-----------------|-----------------|
| `codegen/mod.rs` | 372 | 1603 | +294 (PII integration + Iceberg reorder tests) | Deep: new code paths, import management |
| `codegen/helpers.rs` | 401 | 0 | +17 (needs_pii_imports fn) | Deep: new function; verify docs, #[must_use] |
| `codegen/pii.rs` | 57 | 94 | NEW (151 total) | Deep: entire file is new code |
| `codegen/sink.rs` | 206 | 0 | +8 (Iceberg column reorder) | Deep: new format! block in render_sink |
| `codegen/source.rs` | 196 | 0 | None | Verify: confirm v1.12 compliance holds |
| `codegen/transform.rs` | 194 | 0 | None | Verify: confirm v1.12 compliance holds |

#### Plan 2: validation/ + parsing.rs (4 files)

| File | Prod LOC | Test LOC | New Since v1.12 | Key Audit Focus |
|------|----------|----------|-----------------|-----------------|
| `validation/mod.rs` | 280 | 1967 | +136 (PII validation tests) | Deep: new test code only |
| `validation/rules.rs` | 621 | 0 | +58 (PII entity validation + LazyLock regex) | Deep: new validation logic |
| `validation/syntax.rs` | 61 | 0 | None | Verify: confirm v1.12 compliance holds |
| `parsing.rs` | 681 | 361 | +14 (parse_mask_pii fn) | Deep: new function |

#### Plan 3: everything else (21 files + integration tests)

| File | Prod LOC | Test LOC | New Since v1.12 | Key Audit Focus |
|------|----------|----------|-----------------|-----------------|
| `airflow_dag/mod.rs` | 30 | 2943 | +2 (re-export visibility) | Fix: unused import in test module |
| `airflow_dag/connections.rs` | 168 | 0 | +2 (pub visibility change) | Verify |
| `airflow_dag/collection.rs` | 247 | 0 | None | Verify |
| `airflow_dag/generation.rs` | 450 | 0 | None | Verify |
| `airflow_dag/helpers.rs` | 79 | 0 | None | Verify |
| `airflow_dag/resolve.rs` | 219 | 0 | None | Verify |
| `airflow_dag/triggers.rs` | 614 | 1212 | None | Verify |
| `airflow_dag/version.rs` | 122 | 88 | None | Verify: confirm unreachable!() is valid |
| `providers/mod.rs` | 289 | 0 | None | Verify |
| `providers/glue.rs` | 534 | 66 | None | Verify |
| `providers/emr.rs` | 308 | 64 | None | Verify |
| `storage.rs` | 1038 | 869 | None | Verify |
| `orchestrate.rs` | 780 | 551 | +1 (mask_pii field in test) | Verify |
| `dag_lifecycle.rs` | 644 | 502 | +1 (mask_pii field in test) | Verify |
| `resolve.rs` | 743 | 584 | +2 (parse_mask_pii call) | Verify |
| `config_merge.rs` | 70 | 112 | None | Verify |
| `diff.rs` | 105 | 150 | None | Verify |
| `list_targets.rs` | 111 | 171 | None | Verify |
| `show.rs` | 77 | 0 | None | Verify |
| `utils.rs` | 122 | 157 | None | Verify |
| `lib.rs` | 65 | 0 | +4 (parse_mask_pii re-export) | Verify |

Integration test files (need lint fixes):

| File | LOC | Issue |
|------|-----|-------|
| `tests/glue_integration.rs` | 409 | Missing `#![allow(clippy::unwrap_used, clippy::expect_used)]` |
| `tests/emr_integration.rs` | 343 | Missing `#![allow(clippy::unwrap_used, clippy::expect_used)]` |
| `tests/phase9_integration.rs` | 348 | Missing `#![allow(clippy::unwrap_used, clippy::expect_used)]` |
| `tests/plan_target_integration.rs` | 140 | Missing attribute (0 unwrap/expect calls, but add for consistency) |
| `tests/target_integration.rs` | 168 | Missing attribute (0 unwrap/expect calls, but add for consistency) |
| `tests/common/mod.rs` | 194 | Missing `#[allow(clippy::unwrap_used, clippy::expect_used)]` |

### Changes Since v1.12 (Deep Audit Targets)

**1. codegen/pii.rs (NEW, 151 LOC total)**
New file implementing `render_pii()` for PII masking codegen. Uses `String::with_capacity(256)`, `write!`/`writeln!` over `format!`, `pub(super)` visibility, `#[must_use]`, complete module-level and function-level docs. Well-structured tests with descriptive names. No violations found on inspection. [VERIFIED: file read]

**2. codegen/mod.rs (+294 LOC, mostly tests)**
Production additions: `use pii::render_pii`, `needs_pii_imports()` conditional, PII masking block insertion after partition derivation. All changes follow existing patterns. New tests for Iceberg column reorder and PII masking. [VERIFIED: git diff]

**3. codegen/helpers.rs (+17 LOC)**
New `needs_pii_imports()` function with `#[inline]`, `#[must_use]`, complete docs including "Covers catalog sources/sinks..." addition to `needs_dynamic_frame_import` docs. [VERIFIED: git diff]

**4. codegen/sink.rs (+8 LOC)**
Iceberg column reorder: new `column_order` format string for `_existing_cols`/`_ordered`/`_new` variables. Uses `format!` for Python code string construction -- appropriate here since it's building a code template, not a hot path. [VERIFIED: git diff]

**5. validation/rules.rs (+58 LOC)**
PII validation: `SCREAMING_SNAKE_RE` static regex via `LazyLock`, `expect("static regex")` with `#[allow(clippy::expect_used)]`, `HashSet::with_capacity(job.mask_pii.len())` for duplicate detection. Three validation checks (job-type, format, duplicates) all emit errors independently (no short-circuit). Uses `format!` for per-error field paths -- not a hot path. [VERIFIED: git diff]

**6. validation/mod.rs (+136 LOC, all tests)**
Five new tests for mask_pii validation. All in existing `#[cfg(test)]` module. [VERIFIED: git diff]

**7. parsing.rs (+14 LOC)**
New `parse_mask_pii()` function with `#[must_use]`, returns `Vec<String>`. Uses idiomatic `Option` chaining with `and_then`/`map`/`filter_map`/`unwrap_or_default`. No `# Errors` needed (returns Vec, not Result). [VERIFIED: git diff]

**8. Minor touches (resolve.rs +2, orchestrate.rs +1, dag_lifecycle.rs +1, lib.rs +4, airflow_dag/connections.rs +2, airflow_dag/mod.rs +2)**
All minimal integration points: adding `mask_pii` field to test constructors, adding `parse_mask_pii` call in resolve pipeline, re-exporting in lib.rs, visibility change on `parse_account_from_role_arn`. [VERIFIED: git diff]

## Known Violations (Pre-Audit Findings)

### Confirmed Violations

| # | File | Line(s) | Rule | Severity | Fix |
|---|------|---------|------|----------|-----|
| V-01 | `airflow_dag/mod.rs` | 146 | `unused-imports` | LOW | Remove `AirflowMajorVersion` from test module `use` statement |
| V-02 | `tests/glue_integration.rs` | -- | `clippy::expect_used` | LOW | Add `#![allow(clippy::unwrap_used, clippy::expect_used)]` at file top |
| V-03 | `tests/emr_integration.rs` | -- | `clippy::expect_used` | LOW | Add `#![allow(clippy::unwrap_used, clippy::expect_used)]` at file top |
| V-04 | `tests/phase9_integration.rs` | -- | `clippy::unwrap_used` | LOW | Add `#![allow(clippy::unwrap_used, clippy::expect_used)]` at file top |
| V-05 | `tests/common/mod.rs` | -- | `clippy::unwrap_used` | LOW | Add `#[allow(clippy::unwrap_used, clippy::expect_used)]` on module |
| V-06 | `tests/plan_target_integration.rs` | -- | consistency | LOW | Add `#![allow(clippy::unwrap_used, clippy::expect_used)]` for consistency |
| V-07 | `tests/target_integration.rs` | -- | consistency | LOW | Add `#![allow(clippy::unwrap_used, clippy::expect_used)]` for consistency |

### Confirmed Non-Violations

| # | Area | Why Not a Violation |
|---|------|---------------------|
| N-01 | `version.rs` 5x `unreachable!()` | `AirflowMajorVersion` is `#[non_exhaustive]` in yard-structs -- wildcard arm is REQUIRED by the compiler. `unreachable!()` is the correct choice since new variants should trigger a compile-time todo, not silently return wrong data. [VERIFIED: enum definition in yard-structs/src/config.rs] |
| N-02 | `utils.rs:17` `expect("static regex")` | Static regex pattern compilation -- programming error if this fails. Valid per `err-expect-bugs-only`. [VERIFIED] |
| N-03 | `validation/rules.rs:44` `expect("static regex")` | Same pattern as N-02, with explicit `#[allow(clippy::expect_used)]`. [VERIFIED] |
| N-04 | `format!` in validation/rules.rs | Used for per-error field paths (`mask_pii[0]`). Validation is not a hot path -- called once per `validate_job` invocation. Not an `anti-format-hot-path` violation. |
| N-05 | Provider trait `Pin<Box<dyn Future>>` | Required for object safety with `Box<dyn Provider>`. Not in scope for Phase 65 (no D-10 decision). [VERIFIED: providers/mod.rs] |
| N-06 | StorageBackend trait `Pin<Box<dyn Future>>` | Same pattern as N-05. Required for `Box<dyn StorageBackend + Send + Sync>`. [VERIFIED: storage.rs] |
| N-07 | Zero `# Examples` sections | Phase 59 identified this gap. Phase 65 D-03 specifies "lightweight re-verify" on unchanged code, and D-04 says fix things "contained within yard-core." Adding examples is D-04 compliant but is a significant doc effort across ~30 public types/functions. Flag as finding; planner should scope appropriately. |
| N-08 | `codegen/sink.rs` `format!` for column reorder | Template code construction, not a hot path. Appropriate use. |
| N-09 | D-05 `HashMap<String, Deployment>` | Deferred per D-05 for backward compat. Not a violation for this audit. |

### V1.12 Baseline: Corrections to Prior Research

The Phase 59 (v1.12) research contained an error: it claimed `AirflowMajorVersion` has no `#[non_exhaustive]` and recommended removing the `unreachable!()` wildcard arms in `version.rs`. In fact, `AirflowMajorVersion` has been `#[non_exhaustive]` since Phase 55 (commit `3e8090c`). The wildcard arms are required by the compiler and the `unreachable!()` calls are the correct handling. This finding was never acted on during Phase 59 execution, so no incorrect change was made.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Lint suppression checking | Manual grep for unwrap/expect | Workspace `unwrap_used = "deny"` + per-module `#[allow]` | Compiler enforces at build time [VERIFIED] |
| Documentation compliance | Manual inspection | `#![warn(missing_docs)]` lint (already in lib.rs) | Compiler catches missing docs [VERIFIED] |
| PII entity format validation | Custom char-by-char check | `Regex` with `LazyLock` (already done in rules.rs) | Standard pattern, correct implementation |

## Common Pitfalls

### Pitfall 1: Mistaking Non-Exhaustive Enum Wildcards for Violations
**What goes wrong:** Auditor flags `_ => unreachable!(...)` arms as violations of `err-result-over-panic`.
**Why it happens:** Without checking whether the enum is `#[non_exhaustive]`, the wildcard arm appears unnecessary.
**How to avoid:** Always check the enum definition in yard-structs for `#[non_exhaustive]` before flagging match wildcards. The v1.12 research made this exact error.
**Warning signs:** Attempting to remove wildcard arms from matches on `AirflowMajorVersion`, `DiffType`, or other cross-crate enums.

### Pitfall 2: Audit Churn Without Real Violations
**What goes wrong:** Auditor "finds" issues that aren't violations and introduces unnecessary changes.
**Why it happens:** The 179 rules are guidelines with applicability conditions. Not every rule applies to every function.
**How to avoid:** For each potential violation, verify the rule's applicability conditions. Don't change stringly-typed wire format fields. Don't add deps for marginal memory gains.
**Warning signs:** Changes to serde attributes, new derive macros on types that didn't need them.

### Pitfall 3: Integration Test Lint Attribute Placement
**What goes wrong:** Using `#[allow(...)]` instead of `#![allow(...)]` in integration test files, or placing the attribute in the wrong position.
**Why it happens:** Integration tests in `tests/` are separate crates. Module-level inner attributes use `#!` syntax at the file top, before any other items.
**How to avoid:** For integration test files, use `#![allow(clippy::unwrap_used, clippy::expect_used)]` as the first line (after any `//!` doc comments). For `common/mod.rs`, use `#[allow(...)]` since it's not a crate root.
**Warning signs:** Clippy still firing after adding attributes.

### Pitfall 4: Package Name for Clippy
**What goes wrong:** Running `cargo clippy -p yard-core` uses the wrong package name.
**Why it happens:** The package name in Cargo.toml might differ from directory name.
**How to avoid:** The package name is `yard-core` (confirmed: matches Cargo.toml `[package] name`). Use `-p yard-core`.
**Warning signs:** `package ID specification did not match any packages` errors.

## Code Examples

### Fix V-01: Remove Unused Import in Test Module (airflow_dag/mod.rs)

```rust
// BEFORE (line 145-148):
    use yard_structs::{
        AirflowJobBlock, AirflowMajorVersion, AwsCredentialConfig, Deployment, DeploymentStatus,
        JobName, JobType, ProjectManifest, Resource, StateBackend,
    };

// AFTER:
    use yard_structs::{
        AirflowJobBlock, AwsCredentialConfig, Deployment, DeploymentStatus,
        JobName, JobType, ProjectManifest, Resource, StateBackend,
    };
```

### Fix V-02 through V-07: Integration Test Lint Suppression

```rust
// For integration test files (crate root), add as first line:
#![allow(clippy::unwrap_used, clippy::expect_used)]

// For common/mod.rs (not a crate root), add as module-level attribute:
#[allow(clippy::unwrap_used, clippy::expect_used)]
```

### Pattern: New Code Quality (codegen/pii.rs as exemplar)

```rust
// Source: yard-core/src/codegen/pii.rs
// Demonstrates compliance with multiple rules:
// - MEM-01: String::with_capacity(256)
// - MEM-01: write!/writeln! over format!
// - API-01: #[must_use]
// - DOC-01: complete module + function docs
// - PROJ-01: pub(super) visibility
#[must_use]
pub(super) fn render_pii(mask_pii: &[String], source_var: &str) -> String {
    let mut out = String::with_capacity(256);
    let _ = writeln!(out, "    _yard_pii_dyf = DynamicFrame.fromDF(...)");
    // ...
    out
}
```

## Prior Audit Baseline (Phase 59, v1.12)

Phase 59 was completed 2026-06-17. Key outcomes:
- Module-level `//!` documentation added to 14 files that lacked it
- `# Errors` sections added to fallible public functions across all modules
- `PartialEq` added where meaningful (not on types with `serde_json::Value`)
- `#[must_use]` added to pure functions
- Provider trait `Pin<Box<dyn Future>>` pattern confirmed as correct (object safety)
- `#[non_exhaustive]` skipped on yard-core enums (workspace-internal per D-07)
- All gate checks passed (clippy clean, tests green, no unsafe/unwrap in prod)

**What's new since Phase 59:** PII masking codegen + validation (v1.14), Iceberg column reorder (v1.14.2), and the Phase 64 audit fixed the `AirflowMajorVersion` test serde clippy warning. All new code follows established patterns.

## Workspace Lint Configuration

```toml
[workspace.lints.rust]
unsafe_code = "deny"

[workspace.lints.clippy]
correctness = { level = "deny", priority = -1 }
suspicious = { level = "deny", priority = -1 }
style = { level = "warn", priority = -1 }
complexity = { level = "warn", priority = -1 }
perf = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "warn"
```

Per-crate: `yard-core/src/lib.rs` has `#![warn(missing_docs)]`. All test modules have `#[allow(clippy::unwrap_used, clippy::expect_used)]`. Integration test files in `tests/` are missing the equivalent `#![allow(...)]` -- this is the primary clippy fix needed.

### Current Clippy Status

| Target | Command | Status |
|--------|---------|--------|
| Production (--lib) | `cargo clippy -p yard-core --lib -- -D warnings` | PASS (0 warnings) [VERIFIED] |
| All targets | `cargo clippy -p yard-core --all-targets -- -D warnings` | FAIL (V-01 through V-07) [VERIFIED] |

### Test Status

| Suite | Command | Result |
|-------|---------|--------|
| Unit tests | `cargo test -p yard-core` | 470 passed, 0 failed [VERIFIED] |
| Doc tests | `cargo test -p yard-core --doc` | 4 passed (2 in airflow_dag docs) [VERIFIED] |
| Integration tests | `cargo test -p yard-core --test '*'` | 19 ignored (require Docker) [VERIFIED] |

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (built-in) |
| Config file | Workspace Cargo.toml |
| Quick run command | `cargo test -p yard-core` |
| Full suite command | `cargo test --workspace` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| OWN-01 through ANTI-01 | All rules compliance | manual audit + lint | `cargo clippy -p yard-core --all-targets -- -D warnings` | N/A (audit) |
| GATE (clippy) | Zero clippy warnings | lint | `cargo clippy --all-targets --workspace -- -D warnings` | N/A |
| GATE (tests) | Zero test failures | unit + integration | `cargo test --workspace` | Existing (470+ tests) |

### Sampling Rate
- **Per task commit:** `cargo clippy -p yard-core --all-targets -- -D warnings && cargo test -p yard-core`
- **Per wave merge:** `cargo test --workspace && cargo clippy --all-targets --workspace -- -D warnings`
- **Phase gate:** Full workspace clippy + tests green

### Wave 0 Gaps
None -- existing test infrastructure covers all phase requirements.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | N/A (CLI tool, no auth in yard-core) |
| V3 Session Management | no | N/A |
| V4 Access Control | no | N/A |
| V5 Input Validation | yes | Typed enums via serde, validation module, `deny_unknown_fields` on config structs |
| V6 Cryptography | no | N/A (no crypto in yard-core) |

### Known Threat Patterns for Rust CLI + serde

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Malformed YAML injection | Tampering | Typed deserialization + validation rules (already present) |
| PII entity type injection | Tampering | SCREAMING_SNAKE_CASE regex validation (added v1.14) |
| Unbounded allocation via large config | DoS | Practical file size limits; no explicit bounds needed |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `format!` in validation/rules.rs is not a hot path and does not violate `anti-format-hot-path` | Non-Violations N-04 | LOW -- validation runs once per job definition; would need profiling to prove otherwise |
| A2 | Integration test files `plan_target_integration.rs` and `target_integration.rs` should get lint suppression for consistency even though they have 0 unwrap/expect calls | Violations V-06, V-07 | LOW -- adding the attribute is harmless; not adding it means future test additions could trigger clippy |

## Open Questions (RESOLVED)

1. **Should `# Examples` sections be added as part of Phase 65?** (RESOLVED — deferred via CONTEXT.md D-06)
   - What we know: Zero `# Examples` sections exist in yard-core production code. Phase 59 D-04 mandated adding them but the current state shows none. Phase 65 D-03 says "lightweight re-verify on unchanged files."
   - What's unclear: Whether D-04 scope (fix anything "contained within yard-core") extends to adding examples on existing unchanged functions, or only to fixing violations found in new/changed code.
   - Resolution: Explicitly deferred to a future documentation phase. Adding `# Examples` to 30+ public types/functions is a documentation effort, not an audit-fix. See CONTEXT.md D-06.

## Project Constraints (from CLAUDE.md)

- Never modify `Cargo.toml` without asking first
- Never bump versions unless explicitly asked
- `unwrap()` is fine in tests, never in production code -- enforced by workspace lint
- `unsafe {}` never, anywhere -- enforced by workspace lint
- Every PR must pass `cargo clippy -D warnings` with zero issues
- Prefer stdlib over adding crates for simple tasks
- All logic in yard-core; CLI just parses args and displays
- All code must adhere to the 179 rules defined in `rules/`
- .planning/ is gitignored, must stay local, never force-add

## Sources

### Primary (HIGH confidence)
- [Codebase] Direct inspection of all 31 source files in yard-core/src/
- [Codebase] `cargo clippy -p yard-core --lib -- -D warnings` -- 0 warnings [VERIFIED: 2026-08-18]
- [Codebase] `cargo clippy -p yard-core --all-targets -- -D warnings` -- violations catalogued [VERIFIED: 2026-08-18]
- [Codebase] `cargo test -p yard-core` -- 470 pass, 0 fail [VERIFIED: 2026-08-18]
- [Codebase] `git diff v1.12..HEAD -- yard-core/src/` -- 685 insertions, 5 deletions across 13 files [VERIFIED: 2026-08-18]
- [Codebase] `AirflowMajorVersion` `#[non_exhaustive]` attribute confirmed in yard-structs/src/config.rs [VERIFIED: 2026-08-18]
- [Codebase] Phase 59 RESEARCH.md baseline findings [VERIFIED: read in full]
- [Codebase] Phase 64 RESEARCH.md audit methodology [VERIFIED: read in full]

### Secondary (MEDIUM confidence)
- [Codebase] Workspace `Cargo.toml` lint and release profile configuration
- [Codebase] Phase 59 CONTEXT.md decisions (D-07 no `#[non_exhaustive]` on yard-core enums)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new deps, all versions verified from Cargo.toml
- Architecture: HIGH -- codebase structure unchanged, all files inventoried with LOC counts
- Pitfalls: HIGH -- v1.12 baseline error identified and corrected, all violations confirmed via clippy
- Violations: HIGH -- clippy output verified for both --lib and --all-targets

**Research date:** 2026-08-18
**Valid until:** 2026-09-18 (stable codebase, no fast-moving dependencies)
