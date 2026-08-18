# Phase 65: yard-core Audit & Fix - Context

**Gathered:** 2026-08-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Full rules compliance audit of yard-core (~21k LOC, 31 files) against all 179 Rust coding standards in `rules/`. This is the second phase of the v1.16 audit milestone, following Phase 64 (yard-structs + yard-cli). The audit catches drift since the v1.12 audit (June 2026) and verifies existing code still holds. yard-server excluded per precedent.

</domain>

<decisions>
## Implementation Decisions

### Plan Structure
- **D-01:** Split into 3 plans grouped by change density:
  - Plan 1: `codegen/` (6 files, ~3k LOC) — has the most new code (PII codegen + Iceberg column reorder)
  - Plan 2: `validation/` + `parsing.rs` (4 files, ~3.7k LOC) — second-most new code (PII validation rules)
  - Plan 3: everything else — `airflow_dag/`, `providers/`, `storage.rs`, `resolve.rs`, `orchestrate.rs`, `dag_lifecycle.rs`, `config_merge.rs`, `diff.rs`, `show.rs`, `list_targets.rs`, `utils.rs`, `lib.rs` (21 files, ~14k LOC)
- **D-02:** All 3 plans run in wave 1 (parallel). No cross-plan dependencies — each plan audits its own file set independently.

### Audit Depth
- **D-03:** Deep audit on new code (~685 LOC across 13 changed files), lightweight re-verify on unchanged files. Specifically:
  - **Deep (per-line, all 14 categories):** `codegen/pii.rs` (new, 151 LOC), `codegen/mod.rs` (+294 LOC), `codegen/helpers.rs` (+17), `codegen/sink.rs` (+8), `validation/mod.rs` (+136), `validation/rules.rs` (+58), `parsing.rs` (+14), and minor touches in `resolve.rs`, `orchestrate.rs`, `dag_lifecycle.rs`, `lib.rs`, `airflow_dag/connections.rs`, `airflow_dag/mod.rs`
  - **Verify (confirm v1.12 findings still hold, scan for regressions):** all other unchanged files

### Fix Threshold
- **D-04:** Fix anything contained within yard-core that doesn't change public API signatures or wire format. Defer findings that would require yard-cli changes, serde format changes, or structural rewrites spanning 5+ files.
- **D-05:** Carrying forward from Phase 64: `HashMap<String, Deployment>` TYPE-01 finding remains deferred for backward compat. Stringly-typed wire format fields are a design choice, not a violation.
- **D-06:** Defer `doc-examples-section` (DOC-01 sub-rule) — adding `# Examples` sections to ~30 public types/functions across yard-core is a documentation effort, not a code quality fix. Zero `# Examples` sections exist today (carry-forward gap from v1.12). Addressing this requires a dedicated documentation phase, not an audit-fix phase. Explicitly deferred with rationale, same pattern as D-05.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Audit Standards
- `rules/` — all 179 Rust coding rules organized by category (own-*, err-*, mem-*, api-*, async-*, opt-*, name-*, type-*, test-*, doc-*, perf-*, proj-*, lint-*, anti-*)

### Prior Audit Precedent
- `.planning/phases/64-yard-structs-yard-cli-audit-fix/64-RESEARCH.md` — Phase 64 research with per-file analysis approach and confirmed findings
- `.planning/phases/64-yard-structs-yard-cli-audit-fix/64-01-PLAN.md` — yard-structs plan structure (model for per-file audit approach)
- `.planning/milestones/v1.12-phases/59-yard-core-audit/59-RESEARCH.md` — v1.12 yard-core audit research (baseline findings)

### Requirements
- `.planning/REQUIREMENTS.md` — 14 requirements (OWN-01 through ANTI-01), all mapped to Phase 65

### New Code Since v1.12
- `yard-core/src/codegen/pii.rs` — PII detection codegen (new file, 151 LOC)
- `yard-core/src/codegen/mod.rs` — PII integration + Iceberg column reorder (+294 LOC)
- `yard-core/src/validation/mod.rs` — PII validation rules (+136 LOC)
- `yard-core/src/validation/rules.rs` — PII entity type validation (+58 LOC)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- Phase 64 audit approach: per-file walkthrough checking all 14 rule categories, with `must_haves.truths` documenting each audited finding
- v1.12 Phase 59 plans: 6-plan structure organized by module directory — now consolidated to 3 plans

### Established Patterns
- Workspace lint configuration already excellent: `correctness=deny`, `suspicious=deny`, `unwrap_used=deny`, `unsafe_code=deny`
- `#![warn(missing_docs)]` on all production crates
- All async code uses `Pin<Box<dyn Future>>` for Provider trait (not native async fn) — documented decision from v1.12
- `tokio::fs::` calls use inline paths without `use` statement (matches `storage.rs` established pattern)

### Integration Points
- yard-core's public API is consumed by yard-cli — any signature changes would cascade (D-04 says defer these)
- Serde wire format for state files — changes would break backward compat (D-05 says defer these)

</code_context>

<specifics>
## Specific Ideas

No specific requirements — follows the established audit methodology from Phase 64 and v1.12 Phase 59.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 65-yard-core-audit-fix*
*Context gathered: 2026-08-18*
