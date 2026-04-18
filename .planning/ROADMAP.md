# Roadmap: yard CLI Hardening v1

## Overview

This milestone hardens yard-core by fixing a silent S3 pagination bug and decomposing four oversized modules into focused sub-modules. S3 pagination ships first (standalone correctness fix), then module splits proceed in dependency order: lib.rs facade first (leaf extractions unblock everything), followed by codegen/, airflow_dag/, and validation/ in parallel-safe order. Every phase enforces the full test suite, public API stability, and clippy cleanliness.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: S3 Pagination** - Fix silent truncation of state listings beyond 1,000 objects
- [ ] **Phase 2: lib.rs Facade Extraction** - Split lib.rs into focused sub-modules with lib.rs as re-export facade
- [ ] **Phase 3: codegen.rs Decomposition** - Split codegen.rs into directory module with per-concern sub-modules
- [ ] **Phase 4: airflow_dag.rs Decomposition** - Split airflow_dag.rs into directory module with collection/generation/connections sub-modules
- [ ] **Phase 5: validation.rs Decomposition** - Split validation.rs into directory module and verify all quality gates across full codebase

## Phase Details

### Phase 1: S3 Pagination
**Goal**: S3 state backend correctly lists all job and DAG state files regardless of project size
**Depends on**: Nothing (first phase)
**Requirements**: STOR-01, STOR-02
**Success Criteria** (what must be TRUE):
  1. Running `yard show` on a project with >1,000 jobs lists every job without truncation
  2. Running `yard show` on a project with >1,000 DAGs lists every DAG without truncation
  3. All existing tests pass unchanged
**Plans:** 1 plan

Plans:
- [x] 01-01-PLAN.md — Fix S3 pagination with shared helper + full workspace verification

### Phase 2: lib.rs Facade Extraction
**Goal**: lib.rs contains only mod declarations and re-exports; all business logic lives in focused sub-modules
**Depends on**: Phase 1
**Requirements**: MOD-01
**Success Criteria** (what must be TRUE):
  1. lib.rs is under 100 lines and contains only `mod` declarations, `pub use` re-exports, and no business logic
  2. Each extracted sub-module (orchestrate, diff, dag_lifecycle, parsing, config_merge, show) is a self-contained file of 100-400 lines
  3. `yard-cli` and `yard-server` compile without any source changes
  4. All 246+ existing tests pass without changes to test logic
**Plans:** 3 plans

Plans:
- [x] 02-01-PLAN.md — Extract leaf modules (parsing.rs, config_merge.rs) from lib.rs
- [ ] 02-02-PLAN.md — Extract independent modules (diff.rs, show.rs) from lib.rs
- [ ] 02-03-PLAN.md — Extract orchestrate.rs, dag_lifecycle.rs and finalize lib.rs facade

### Phase 3: codegen.rs Decomposition
**Goal**: codegen.rs is replaced by a directory module with sub-modules split by rendering concern
**Depends on**: Phase 2
**Requirements**: MOD-02
**Success Criteria** (what must be TRUE):
  1. `codegen/` directory module exists with sub-modules for source, sink, transform, helpers, partition, and secrets rendering
  2. No file in `codegen/` exceeds 400 lines
  3. `yard-cli` and `yard-server` compile without any source changes
  4. All existing tests pass without changes to test logic
**Plans**: TBD

Plans:
- [ ] 03-01: TBD
- [ ] 03-02: TBD

### Phase 4: airflow_dag.rs Decomposition
**Goal**: airflow_dag.rs is replaced by a directory module with sub-modules split by DAG pipeline stage
**Depends on**: Phase 2
**Requirements**: MOD-03
**Success Criteria** (what must be TRUE):
  1. `airflow_dag/` directory module exists with sub-modules for collection, generation, connections, and helpers
  2. No file in `airflow_dag/` exceeds 400 lines
  3. `yard-cli` and `yard-server` compile without any source changes
  4. All existing tests pass without changes to test logic
**Plans**: TBD

Plans:
- [ ] 04-01: TBD
- [ ] 04-02: TBD

### Phase 5: validation.rs Decomposition
**Goal**: validation.rs is replaced by a directory module, and all cross-cutting quality gates are verified across the full codebase
**Depends on**: Phase 3, Phase 4
**Requirements**: MOD-04, MOD-05, MOD-06, MOD-07
**Success Criteria** (what must be TRUE):
  1. `validation/` directory module exists with sub-modules for rules, syntax checking, and full validation orchestration
  2. No file in `validation/` exceeds 400 lines
  3. Full test suite passes with zero changes to any test logic across all phases (MOD-05 final gate)
  4. `yard-cli` and `yard-server` compile without any source modifications across all phases (MOD-06 final gate)
  5. `cargo clippy -D warnings` passes clean across entire workspace (MOD-07 final gate)
**Plans**: TBD

Plans:
- [ ] 05-01: TBD
- [ ] 05-02: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5
Note: Phases 3 and 4 depend only on Phase 2 (not each other) and could theoretically execute in either order.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. S3 Pagination | 1/1 | Complete | 2026-04-18 |
| 2. lib.rs Facade Extraction | 0/3 | Not started | - |
| 3. codegen.rs Decomposition | 0/2 | Not started | - |
| 4. airflow_dag.rs Decomposition | 0/2 | Not started | - |
| 5. validation.rs Decomposition | 0/2 | Not started | - |
