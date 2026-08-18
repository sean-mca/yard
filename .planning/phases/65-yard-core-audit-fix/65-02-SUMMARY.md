---
phase: 65-yard-core-audit-fix
plan: 02
subsystem: validation
tags: [rust, clippy, validation, parsing, audit, ownership, borrowing]

requires:
  - phase: 64-yard-structs-yard-cli-audit-fix
    provides: "yard-structs + yard-cli audit baseline"
  - phase: 65-yard-core-audit-fix plan 01
    provides: "codegen/ audit baseline"
provides:
  - "validation/ and parsing.rs fully audited against 179 rules (14 categories)"
  - "~29 needless .to_string() allocations removed from validation/rules.rs"
affects: [65-yard-core-audit-fix]

tech-stack:
  added: []
  patterns: ["own-borrow-over-clone: use &prefix instead of &prefix.to_string() when prefix is String"]

key-files:
  created: []
  modified: ["yard-core/src/validation/rules.rs"]

key-decisions:
  - "D-06 honored: # Examples sections explicitly deferred to future documentation phase"
  - "format! in validation error paths confirmed as non-hot-path usage (N-04)"
  - "expect('static regex') with #[allow(clippy::expect_used)] confirmed valid per err-expect-bugs-only (N-03)"

patterns-established:
  - "own-borrow-over-clone: when a variable is already String, use &var not &var.to_string() for &str arguments"

requirements-completed: [OWN-01, ERR-01, MEM-01, API-01, ASYNC-01, OPT-01, NAME-01, TYPE-01, TEST-01, DOC-01, PERF-01, PROJ-01, LINT-01, ANTI-01]

coverage:
  - id: D1
    description: "validation/rules.rs audited against 14 categories, ~29 own-borrow-over-clone violations fixed"
    requirement: "OWN-01"
    verification:
      - kind: unit
        ref: "cargo test -p yard-core (470 passed)"
        status: pass
      - kind: other
        ref: "cargo clippy -p yard-core --lib -- -D warnings (zero warnings)"
        status: pass
    human_judgment: false
  - id: D2
    description: "validation/mod.rs audited against 14 categories, no violations found"
    verification:
      - kind: unit
        ref: "cargo test -p yard-core (470 passed)"
        status: pass
    human_judgment: false
  - id: D3
    description: "parsing.rs audited against 14 categories, no violations found"
    verification:
      - kind: unit
        ref: "cargo test -p yard-core (470 passed)"
        status: pass
    human_judgment: false
  - id: D4
    description: "validation/syntax.rs re-verified, no violations found"
    verification:
      - kind: unit
        ref: "cargo test -p yard-core (470 passed)"
        status: pass
    human_judgment: false

duration: 5min
completed: 2026-08-18
status: complete
---

# Phase 65 Plan 02: Validation + Parsing Audit Summary

**Audited 4 validation/ + parsing.rs files against 179 Rust rules; fixed ~29 own-borrow-over-clone violations in rules.rs (needless .to_string() on String variables)**

## Performance

- **Duration:** 5 min
- **Started:** 2026-08-18T01:22:19Z
- **Completed:** 2026-08-18T01:27:39Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

- Deep audit of validation/rules.rs (621 prod LOC) against all 14 rule categories -- found and fixed ~29 instances of `&prefix.to_string()` where `prefix` is already `String`, violating `own-borrow-over-clone`
- Deep audit of validation/mod.rs (280 prod LOC + 1967 test LOC) -- zero violations; PII tests follow `#[cfg(test)]`, `use super::*`, descriptive names, AAA pattern
- Deep audit of parsing.rs (681 prod LOC + 361 test LOC) -- zero violations; `parse_mask_pii` has `#[must_use]`, uses idiomatic `and_then`/`map`/`filter_map` chaining
- Lightweight re-verify of validation/syntax.rs (61 LOC) -- zero violations; `//!` module doc, `///` with `# Errors`, `#[must_use]` all present

## Per-File Audit Findings

### validation/rules.rs (621 prod LOC, +58 new since v1.12)

| Category | Finding | Status |
|----------|---------|--------|
| OWN-01 | ~29 instances of `&prefix.to_string()` where `prefix: String` -- creates needless allocation. Fixed to `&prefix` | **FIXED** |
| ERR-01 | `expect("static regex")` at line 44 with `#[allow(clippy::expect_used)]` -- valid per `err-expect-bugs-only` (N-03) | Compliant |
| MEM-01 | `Vec::with_capacity(4)`, `HashSet::with_capacity(job.mask_pii.len())` -- proper pre-allocation | Compliant |
| MEM-01 | `format!` for per-error field paths (`mask_pii[{i}]`) -- validation is not a hot path (N-04) | Compliant |
| NAME-01 | `SCREAMING_SNAKE_RE` follows SCREAMING_SNAKE_CASE; functions snake_case | Compliant |
| API-01 | `#[must_use]` on `validate_job` | Compliant |
| DOC-01 | `//!` module doc, `///` on all functions; no `# Errors` needed (returns Vec, not Result) | Compliant |
| TEST-01 | Tests in mod.rs, N/A here | N/A |
| ASYNC-01 | No async code | N/A |
| OPT-01 | No hot-path functions needing `#[inline]` | Compliant |
| TYPE-01 | Typed `JobType` enum, `ValidationError` struct | Compliant |
| PERF-01 | Iterator-based patterns, `collect()` at final results | Compliant |
| PROJ-01 | Appropriate visibility, `pub use` re-export | Compliant |
| LINT-01 | `#[allow(clippy::expect_used)]` on static regex | Compliant |
| ANTI-01 | No `unwrap()` in production, no excessive cloning (after fix) | Compliant |

### validation/mod.rs (280 prod LOC + 1967 test LOC, +136 new since v1.12)

| Category | Finding | Status |
|----------|---------|--------|
| DOC-01 | `//!` module doc present (lines 1-13), `///` on all public functions | Compliant |
| API-01 | `#[must_use]` on `validate_job_full`, `validate_dag_full`, `validate_project` | Compliant |
| OPT-01 | `#[inline]` on `check_mutual_exclusion` and `check_max_active_runs` | Compliant |
| MEM-01 | `Vec::with_capacity(4)` and `Vec::with_capacity(1)` for error collectors | Compliant |
| TEST-01 | `#[cfg(test)]` with `#[allow(clippy::unwrap_used, clippy::expect_used)]`, `use super::*`, descriptive names, AAA pattern | Compliant |
| All others | No violations | Compliant |

### parsing.rs (681 prod LOC + 361 test LOC, +14 new since v1.12)

| Category | Finding | Status |
|----------|---------|--------|
| DOC-01 | `//!` module doc (lines 1-14), `# Errors` on all fallible public functions | Compliant |
| API-01 | `#[must_use]` on all pure public functions including `parse_mask_pii` | Compliant |
| MEM-01 | `Vec::with_capacity(arr.len())` in `parse_sources` and `parse_transforms` | Compliant |
| ERR-01 | All fallible functions return `anyhow::Result` with `?` propagation, lowercase error messages | Compliant |
| OWN-01 | `clone()` only where serde needs ownership (line 229) | Compliant |
| TEST-01 | `#[cfg(test)]` with `#[allow(...)]`, `use super::*`, descriptive names | Compliant |
| All others | No violations | Compliant |

### validation/syntax.rs (61 LOC, no changes since v1.12)

| Category | Finding | Status |
|----------|---------|--------|
| DOC-01 | `//!` module doc, `///` with `# Errors` section on public function | Compliant |
| API-01 | `#[must_use]` on `validate_python_syntax` | Compliant |
| All others | No violations | Compliant |

## Task Commits

Each task was committed atomically:

1. **Task 1: Deep audit validation/rules.rs and validation/mod.rs** - `b858aeb` (fix: remove ~29 needless .to_string() calls)
2. **Task 2: Audit parsing.rs and validation/syntax.rs + gate checks** - no commit (zero violations found; gate checks pass)

## Files Created/Modified

- `yard-core/src/validation/rules.rs` - Removed ~29 instances of `&prefix.to_string()` where `prefix` is already a `String`, replacing with `&prefix` (auto-deref to `&str`)

## Decisions Made

- D-06 honored: `# Examples` sections on public items explicitly deferred to a future documentation phase (adding examples to 30+ public functions is a documentation effort, not an audit fix)
- `format!` in validation error paths (e.g., `mask_pii[{i}]`) confirmed as non-hot-path usage -- validation runs once per job definition, not in a tight loop

## Deviations from Plan

None -- plan executed as written. The `&prefix.to_string()` violation was found during the planned audit sweep and fixed per D-04 (fix anything contained within yard-core that doesn't change public API signatures).

## Confirmed Non-Violations

| ID | Area | Rationale |
|----|------|-----------|
| N-03 | `validation/rules.rs:44` `expect("static regex")` | Static regex compilation -- programming error if fails. Valid per `err-expect-bugs-only`. Has explicit `#[allow(clippy::expect_used)]` |
| N-04 | `format!` in validation/rules.rs error paths | Used for per-error field paths. Validation runs once per `validate_job` call, not a hot path |

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- All 4 validation/ + parsing.rs files verified compliant
- Gate checks pass: `cargo clippy -p yard-core --lib -- -D warnings` (zero warnings), `cargo test -p yard-core` (470 pass, 0 fail)
- Ready for Plan 03 (remaining yard-core files)

## Self-Check: PASSED

- 65-02-SUMMARY.md: FOUND
- validation/rules.rs: FOUND
- Commit b858aeb: FOUND
- cargo clippy -p yard-core --lib -- -D warnings: zero warnings
- cargo test -p yard-core: 470 passed, 0 failed

---
*Phase: 65-yard-core-audit-fix*
*Completed: 2026-08-18*
