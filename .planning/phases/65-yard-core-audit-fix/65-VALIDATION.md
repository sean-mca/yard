---
phase: 65
slug: yard-core-audit-fix
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-17
---

# Phase 65 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | cargo test (Rust built-in) |
| **Config file** | Cargo.toml (workspace) |
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
| 65-01-01 | 01 | 1 | OWN-01 | — | N/A | unit | `cargo test --workspace` | ✅ | ⬜ pending |
| 65-01-02 | 01 | 1 | ERR-01 | — | N/A | unit | `cargo test --workspace` | ✅ | ⬜ pending |
| 65-01-03 | 01 | 1 | LINT-01 | — | N/A | lint | `cargo clippy --all-targets --workspace -- -D warnings` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
