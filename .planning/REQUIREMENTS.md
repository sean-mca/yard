# Requirements: yard v1.16 Rules Compliance Audit

**Defined:** 2026-08-17
**Core Value:** The CLI must remain correct and easy to reason about — every refactor must preserve existing behavior and pass the full test suite.

## v1.16 Requirements

Full codebase audit of yard-cli, yard-core, and yard-structs against the 179 Rust coding standards in `rules/`. One requirement per rule category.

### Ownership & Borrowing (CRITICAL)

- [x] **OWN-01**: All production code passes the 12 ownership/borrowing rules (borrow-over-clone, slice-over-vec, cow, arc, rc, refcell, mutex, rwlock, copy-small, clone-explicit, move-large, lifetime-elision)

### Error Handling (CRITICAL)

- [x] **ERR-01**: All production code passes the 12 error handling rules (thiserror/anyhow split, result-over-panic, context-chain, no-unwrap, expect-bugs-only, question-mark, from-impl, source-chain, lowercase-msg, doc-errors, custom-type)

### Memory Optimization (CRITICAL)

- [x] **MEM-01**: All production code passes the 15 memory rules (with-capacity, smallvec, arrayvec, box-large-variant, boxed-slice, thinvec, clone-from, reuse-collections, avoid-format, write-over-format, arena, zero-copy, compact-string, smaller-integers, assert-type-size)

### API Design (HIGH)

- [x] **API-01**: All public APIs pass the 15 API design rules (builder, must-use, newtype, typestate, sealed, extension, parse-dont-validate, impl-into, impl-asref, must-use-result, non-exhaustive, from-not-into, default, common-traits, serde-optional)

### Async (HIGH)

- [x] **ASYNC-01**: All async code passes the 15 async rules (tokio-runtime, no-lock-await, spawn-blocking, tokio-fs, cancellation, join-parallel, try-join, select, bounded-channel, mpsc, broadcast, watch, oneshot, joinset, clone-before-await)

### Compiler Optimization (HIGH)

- [x] **OPT-01**: Release build config and hot paths pass the 12 optimization rules (inline, cold, likely, lto, codegen-units, pgo, target-cpu, bounds-check, simd, cache-friendly)

### Naming (MEDIUM)

- [x] **NAME-01**: All identifiers pass the 16 naming rules (types-camel, variants-camel, funcs-snake, consts-screaming, lifetime-short, type-param, as/to/into prefixes, no-get, is-has-bool, iter conventions, acronym-word, crate-no-rs)

### Type Safety (MEDIUM)

- [x] **TYPE-01**: All types pass the 10 type safety rules (newtype-ids, newtype-validated, enum-states, option-nullable, result-fallible, phantom, never, generic-bounds, no-stringly, repr-transparent)

### Testing (MEDIUM)

- [x] **TEST-01**: Test code follows the 13 testing rules (cfg-test-module, use-super, integration-dir, descriptive-names, arrange-act-assert, proptest, mockall, mock-traits, fixture-raii, tokio-async, should-panic, criterion, doctest)

### Documentation (MEDIUM)

- [x] **DOC-01**: Public items have documentation per the 11 doc rules (all-public, module-inner, examples, errors, panics, safety, question-mark, hidden-setup, intra-links, link-types, cargo-metadata)

### Performance (MEDIUM)

- [x] **PERF-01**: Performance patterns pass the 11 rules (iter-over-index, iter-lazy, collect-once, entry-api, drain-reuse, extend-batch, chain-avoid, collect-into, black-box, release-profile, profile-first)

### Project Structure (LOW)

- [x] **PROJ-01**: Project structure follows the 11 rules (lib-main-split, mod-by-feature, flat-small, mod-rs-dir, pub-crate, pub-super, pub-use, prelude, bin-dir, workspace-large, workspace-deps)

### Linting (LOW)

- [x] **LINT-01**: Lint configuration follows the 11 rules (deny-correctness, warn-suspicious/style/complexity/perf, pedantic-selective, missing-docs, unsafe-doc, cargo-metadata, rustfmt-check, workspace-lints)

### Anti-patterns (REFERENCE)

- [x] **ANTI-01**: No anti-pattern violations across the 15 rules (unwrap-abuse, expect-lazy, clone-excessive, lock-across-await, string-for-str, vec-for-slice, index-over-iter, panic-expected, empty-catch, over-abstraction, premature-optimize, type-erasure, format-hot-path, collect-intermediate, stringly-typed)

## Future Requirements

None — this is a point-in-time audit milestone.

## Out of Scope

| Feature | Reason |
|---------|--------|
| yard-server audit | Excluded per precedent (v1.12); server has its own lifecycle |
| New feature work | Audit only — no new capabilities |
| Adding external crates for compliance | Fixes must use existing deps (e.g. won't add SmallVec/ArrayVec/CompactString unless separately approved) |
| Performance benchmarking | Rules like profile-first and criterion-bench are advisory; no benchmark suite being created |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| OWN-01 | Phase 64, Phase 65 | Complete |
| ERR-01 | Phase 64, Phase 65 | Complete |
| MEM-01 | Phase 64, Phase 65 | Complete |
| API-01 | Phase 64, Phase 65 | Complete |
| ASYNC-01 | Phase 64, Phase 65 | Complete |
| OPT-01 | Phase 64, Phase 65 | Complete |
| NAME-01 | Phase 64, Phase 65 | Complete |
| TYPE-01 | Phase 64, Phase 65 | Complete |
| TEST-01 | Phase 64, Phase 65 | Complete |
| DOC-01 | Phase 64, Phase 65 | Complete |
| PERF-01 | Phase 64, Phase 65 | Complete |
| PROJ-01 | Phase 64, Phase 65 | Complete |
| LINT-01 | Phase 64, Phase 65 | Complete |
| ANTI-01 | Phase 64, Phase 65 | Complete |

**Coverage:**

- v1.16 requirements: 14 total
- Mapped to phases: 14 (all 14 map to both Phase 64 and Phase 65)
- Unmapped: 0

**Coverage model:** Each requirement spans both phases. Phase 64 covers the requirement across yard-structs + yard-cli; Phase 65 covers it across yard-core. A requirement is complete when both phases have audited and fixed their crate subset.

---
*Requirements defined: 2026-08-17*
*Last updated: 2026-08-17 after roadmap creation*
