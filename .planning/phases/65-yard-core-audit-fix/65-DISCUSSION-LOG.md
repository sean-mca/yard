# Phase 65: yard-core Audit & Fix - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-18
**Phase:** 65-yard-core-audit-fix
**Areas discussed:** Plan granularity, New-code focus depth, Fix vs defer threshold

---

## Plan Granularity

| Option | Description | Selected |
|--------|-------------|----------|
| 3 plans | Group by change density: codegen/, validation/+parsing, everything else | ✓ |
| 6 plans (v1.12 precedent) | Same module-group split as Phase 59 | |
| 2 plans | New code vs unchanged code | |

**User's choice:** 3 plans grouped by change density
**Notes:** None

### Wave Structure (follow-up)

| Option | Description | Selected |
|--------|-------------|----------|
| All wave 1 | No cross-plan dependencies, parallel execution safe | ✓ |
| Sequential waves | Run codegen first, then validation, then everything else | |

**User's choice:** All wave 1 (parallel)
**Notes:** None

---

## New-Code Focus Depth

| Option | Description | Selected |
|--------|-------------|----------|
| Deep on new, verify unchanged | Full per-line audit of ~685 LOC new code, lightweight re-verify for unchanged | ✓ |
| Equal depth everywhere | Full per-line audit of all ~21k LOC | |
| New code only | Only audit the 13 changed files, skip unchanged entirely | |

**User's choice:** Deep on new, verify unchanged
**Notes:** None

---

## Fix vs Defer Threshold

| Option | Description | Selected |
|--------|-------------|----------|
| Fix local, defer cross-cutting | Fix anything within yard-core not changing public API or wire format | ✓ |
| Fix everything possible | Fix all findings including public API changes | |
| Conservative — fix only trivial | Only clippy, docs, naming; defer function body/signature changes | |

**User's choice:** Fix local, defer cross-cutting
**Notes:** None

---

## Claude's Discretion

None — all areas had clear user selections.

## Deferred Ideas

None — discussion stayed within phase scope.
