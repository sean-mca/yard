---
phase: 65
name: yard-core-audit-fix
threats_open: 0
asvs_level: 1
block_on: high
audited: 2026-08-18
---

# Security Audit: Phase 65 -- yard-core Audit & Fix

## Scope

Mechanical lint fixes across yard-core and yard-structs:
- ~29 needless `.to_string()` calls removed in `validation/rules.rs`
- Unused `AirflowMajorVersion` import removed in `airflow_dag/mod.rs`
- Lint suppression attributes added to 6 integration test files
- 2 needless borrows removed in `yard-structs/src/config.rs` test code

No behavioral changes, no new features, no API changes.

## Threat Verification

| Threat ID | Category | Component | Severity | Disposition | Status | Evidence |
|-----------|----------|-----------|----------|-------------|--------|----------|
| T-65-01 | Tampering | codegen/mod.rs generate_script | medium | accept | CLOSED | Accepted risk: YAML validated by validation/ before codegen; audit phase makes no architectural change |
| T-65-02 | Information Disclosure | codegen/pii.rs render_pii | low | accept | CLOSED | Accepted risk: PII codegen runs in customer Glue environment, not yard; no secrets handled |
| T-65-03 | Tampering | validation/rules.rs validate_mask_pii | low | mitigate | CLOSED | `SCREAMING_SNAKE_RE` regex at rules.rs:41; match gate at rules.rs:88 rejects malformed PII entity types before codegen |
| T-65-04 | Tampering | parsing.rs parse_mask_pii | low | accept | CLOSED | Accepted risk: serde typed deserialization rejects malformed YAML before parse_mask_pii runs |
| T-65-05 | Tampering | storage.rs state deserialization | medium | accept | CLOSED | Accepted risk: serde typed structs limit attack surface; tampered state causes stale diff, not code execution; operator owns state files |
| T-65-06 | Tampering | providers/ AWS SDK calls | low | accept | CLOSED | Accepted risk: AWS SDK handles request signing and TLS; no raw HTTP in provider code |
| T-65-07 | Denial of Service | orchestrate.rs locking | low | mitigate | CLOSED | `LOCK_TTL_MINUTES` constant at storage.rs:29; stale lock reclamation at storage.rs:298; `LockGuard` RAII at orchestrate.rs:285 with best-effort release and TTL backstop at orchestrate.rs:500-502 |

## Accepted Risks

| Threat ID | Severity | Rationale |
|-----------|----------|-----------|
| T-65-01 | medium | YAML input is validated by the validation/ module before reaching codegen. Codegen trusts validated input. This audit phase does not change the architecture. |
| T-65-02 | low | PII masking codegen generates AWS Glue EntityDetector calls that run in the customer's environment, not within yard itself. No secrets are handled by yard. |
| T-65-04 | low | serde typed deserialization from YAML rejects malformed input before it reaches parse_mask_pii. The function only processes already-deserialized serde_yaml::Value. |
| T-65-05 | medium | State files are deserialized via serde with typed Rust structs. A tampered state file would cause an incorrect diff (stale deploy), not arbitrary code execution. yard is a CLI tool run by the operator who owns the state files. |
| T-65-06 | low | AWS SDK handles request signing and TLS. Provider code constructs API calls from validated config. No raw HTTP requests. |

## Unregistered Flags

None. No `## Threat Flags` sections present in any SUMMARY.md for this phase.

## Result

All 7 threats CLOSED (5 accepted, 2 mitigated with code evidence). Zero blocking open threats.
