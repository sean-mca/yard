---
phase: 64
slug: yard-structs-yard-cli-audit-fix
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-08-17
---

# Phase 64 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (built-in Rust test framework) |
| **Config file** | Cargo.toml workspace |
| **Quick run command** | `cargo test --workspace` |
| **Full suite command** | `cargo test --workspace && cargo clippy --all-targets --workspace -- -D warnings` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cargo test --workspace`
- **After every plan wave:** Run `cargo test --workspace && cargo clippy --all-targets --workspace -- -D warnings`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 64-01-T1 | 01 | 1 | OWN-01..ANTI-01 | T-64-01 | clippy clean, tests pass, V-01/V-02 fixed | unit + lint | `cargo clippy -p yard-structs --all-targets -- -D warnings && cargo test -p yard-structs` | yard-structs/src/config.rs | ⬜ pending |
| 64-01-T2 | 01 | 1 | OWN-01..ANTI-01 | T-64-02 | clippy clean, all tests + doc tests pass | unit + lint + doctest | `cargo clippy -p yard-structs --all-targets -- -D warnings && cargo test -p yard-structs && cargo test --doc -p yard-structs` | yard-structs/src/diff.rs | ⬜ pending |
| 64-02-T1 | 02 | 1 | OWN-01..ANTI-01 | T-64-03 | clippy clean, tests pass, A-02/A-03/A-04 assessed | unit + lint | `cargo clippy -p yard --all-targets -- -D warnings && cargo test -p yard` | yard-cli/src/utils.rs | ⬜ pending |
| 64-02-T2 | 02 | 1 | OWN-01..ANTI-01 | T-64-04,T-64-05 | clippy clean, all tests pass, workspace green | unit + lint + workspace | `cargo clippy -p yard --all-targets -- -D warnings && cargo test -p yard && cargo test --workspace` | yard-cli/src/main.rs | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved
