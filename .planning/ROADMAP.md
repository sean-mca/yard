# Roadmap: yard

## Overview

yard is a Rust workspace (4 crates, ~21k lines) providing a terragrunt-inspired CLI for data-engineering deploys plus a nascent Atlantis-like server. v1.0 hardened the CLI core (S3 pagination + four-module decomposition). v1.1 hardened yard-server (testable trait layer, structured errors, WebSocket updates, Slack drift alerts, cross-account credentials). v1.2 delivered a workspace tech-debt audit and drove clippy to zero across the workspace. v1.3 delivered the `--target` correctness sweep — `yard apply --target` and `yard plan --target` both behave correctly across the full job/DAG matrix with regression coverage. v1.3.1 fixed the `_yard_fill_nulls` over-matching bug and extended the recursive codegen coercion to cover the wider class of Parquet-unwritable types. v1.4 shipped the DAG codegen Glue kwargs, the Iceberg-aware codegen insert (Phase 18.1), and `yard list targets --json`; the Linux release workflow + composite Action work (Phases 16-18) was deferred to backlog. v1.5 Hardening + Extensibility (typed configs, storage/provider DRY, yard-server security) shipped 2026-04-28 — paying down structural tech debt across all four crates and closing the security gaps that blocked yard-server from leaving localhost. v1.6 Event-Driven DAGs shipped 2026-04-30 — adds a top-level `trigger:` block to `dag.yaml` covering S3 file drops, Airflow Datasets, SQS messages, and external API triggers, with first-class payload plumbing into task `op_kwargs`, DAG-level `publishes:` rendering via synthetic terminal task, cross-DAG broken-link soft-warning, per-DAG version-banner header, and the four-doc v1.6 user-facing surface (CONFIGURATION/AIRFLOW updates + new MIGRATION-v1.6.md). v1.7 Documentation Overhaul shipped 2026-05-02 — restructured `docs/` into a Diataxis-lite tree, refreshed reference layer, added copy-paste job templates, refreshed root README, CI doc-validation guard. v1.8 Code Quality + Bug Fixes shipped 2026-05-11 — workspace-wide audit and critical/high bug fixes across all 4 crates. v1.9 yard-server Redesign shipped 2026-05-18 — ground-up rebuild of yard-server as an Atlantis-like server with chatOps, environment-aware dashboard, drift detection, Slack alerting, and trait-based auth. v1.10 yard-server Production Readiness (in progress) — E2E integration tests, health endpoints, graceful shutdown, and Docker container image to take the v1.9 rebuild from dev to deployable. v1.11 Airflow 3.0 Asset Support — version-aware codegen so yard emits `Asset` (AF3) or `Dataset` (AF2) depending on a new `airflow.version` config field, with per-environment control via the config cascade. v1.12 Code Audit — rules compliance audit of yard-cli, yard-core, and yard-structs against the 179 Rust coding standards in `rules/`, catching drift since the Phase 48 baseline (especially v1.11 additions: version.rs, config cascade, trigger alias). v1.14 PII Detection & Masking — declarative `mask_pii` support in `job.yaml` that generates `EntityDetector.detect()` code blocks in Glue codegen with REDACT action and `"****"` mask text. v1.15 Distribution — cross-platform release workflow + Homebrew tap formula. v1.16 Rules Compliance Audit shipped 2026-08-18 — full codebase audit of yard-cli, yard-core, and yard-structs against the 179 Rust coding standards, catching drift since the v1.12 audit (June 2026). v2.0 Plugin Architecture transforms yard from a monolithic binary with compiled-in providers into an orchestrator that delegates provider-specific work to out-of-process plugin binaries over JSON-over-stdio, enabling community-authored providers and slimming the core binary.

## Milestones

- ✅ **v1.0 yard CLI Hardening** — Phases 1-5 (shipped 2026-04-18)
- ✅ **v1.1 yard-server Polish & Test Coverage** — Phases 6-9 (shipped 2026-04-22)
- ✅ **v1.2 Clippy Cleanliness + Audit** — Phases 10-11 (shipped 2026-04-23; scope reduced — Phases 12-14 abandoned)
- ✅ **v1.3 `--target` Correctness Sweep** — Phases 12-13 (shipped 2026-04-23)
- ✅ **v1.3.1 Codegen Null-Handling Fix** — Phase 14 + post-phase PRs #63/#64/#65 (shipped 2026-04-23)
- ✅ **v1.4 Distribution + DAG Fixes (partial)** — Phases 15, 18.1, 19 shipped (2026-04-24); distribution Phases 16-18 deferred to backlog
- ✅ **v1.5 Hardening + Extensibility** — Phases 20-27 (shipped 2026-04-28)
- ✅ **v1.6 Event-Driven DAGs** — Phases 28-32 (shipped 2026-04-30)
- ✅ **v1.7 Documentation Overhaul** — Phases 33-36 (shipped 2026-05-02)
- ✅ **v1.8 Code Quality + Bug Fixes** — Phases 37-38 (shipped 2026-05-11)
- ✅ **v1.9 yard-server Redesign** — Phases 39-45 (shipped 2026-05-18)
- **v1.10 yard-server Production Readiness** — Phases 46-54 (in progress; Phase 51 Docker postponed)
- ✅ **v1.11 Airflow 3.0 Asset Support** — Phases 55-57 (shipped 2026-06-15)
- ✅ **v1.12 Code Audit** — Phases 58-59 (shipped 2026-06-17)
- 🚧 **v1.13 State Scoping + Distributed State** — Phases 52-53 (in progress)
- ✅ **v1.14 PII Detection & Masking** — Phases 60-62 (shipped 2026-06-25)
- ✅ **v1.15 Distribution** — Phase 63 (shipped 2026-07-19)
- ✅ **v1.16 Rules Compliance Audit** — Phases 64-65 (shipped 2026-08-18)
- 🚧 **v2.0 Plugin Architecture** — Phases 66-70

## Phases

**Phase Numbering:**

- Integer phases: Planned milestone work
- Decimal phases (e.g., 18.1): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

<details>
<summary>v1.0 yard CLI Hardening (Phases 1-5) -- SHIPPED 2026-04-18</summary>

Archive: [v1.0-ROADMAP.md](milestones/v1.0-ROADMAP.md)

</details>

<details>
<summary>v1.1 yard-server Polish & Test Coverage (Phases 6-9) -- SHIPPED 2026-04-22</summary>

Archive: [v1.1-ROADMAP.md](milestones/v1.1-ROADMAP.md)

</details>

<details>
<summary>v1.2 Clippy Cleanliness + Audit (Phases 10-11) -- SHIPPED 2026-04-23</summary>

Archive: [v1.2-ROADMAP.md](milestones/v1.2-ROADMAP.md) | [v1.2-REQUIREMENTS.md](milestones/v1.2-REQUIREMENTS.md)

Phases 12-14 of the original v1.2 plan were abandoned (see `v1.2-ROADMAP.md` closure note). DUP-001, DUP-002, and all PERF-* rows remain as backlog in `10-AUDIT.md`.

</details>

<details>
<summary>v1.3 `--target` Correctness Sweep (Phases 12-13) -- SHIPPED 2026-04-23</summary>

Archive: [v1.3-ROADMAP.md](milestones/v1.3-ROADMAP.md) (pending milestone-close archive)

Phase 12 fixed the `apply --target` manifest-mutilation bug; Phase 13 audited `plan --target`, promoted `plan` to a yard-core library fn with `PlanResult`, slimmed the CLI to a thin wrapper, and added the matching integration matrix.

</details>

<details>
<summary>v1.3.1 Codegen Null-Handling Fix (Phase 14 + PRs #63/#64/#65) -- SHIPPED 2026-04-23</summary>

Archive: [v1.3.1-ROADMAP.md](milestones/v1.3.1-ROADMAP.md) | [v1.3.1-REQUIREMENTS.md](milestones/v1.3.1-REQUIREMENTS.md)

Phase 14 narrowed the `_yard_fill_nulls` void detection from substring match to exact `isinstance(dt, NullType)`, added `_yard_coerce_struct_voids` for FILL-06 nested-void handling, and locked in a 5-test regression matrix. Post-phase PRs extended coverage to the full class of Parquet-unwritable types after real Spark runs surfaced the remaining failure modes.

</details>

<details>
<summary>v1.4 Distribution + DAG Fixes -- partial (Phases 15, 18.1, 19) SHIPPED 2026-04-24</summary>

Phase 15 added Glue `iam_role_name` + `script_location` kwargs to emitted DAGs. Phase 18.1 (URGENT insertion) made codegen Iceberg-aware so existing-table writes conform to the live Iceberg schema instead of emitting `_yard_empty` placeholders. Phase 19 shipped `yard list targets [--json]` (released as v1.3.4) so CI/CD consumers can build per-target OIDC matrices.

Distribution Phases 16-18 (Linux release workflow, composite GitHub Action, install docs) deferred to backlog 2026-04-26 to keep v1.5 focused on architectural hardening; the manual Docker build recipe in CLAUDE.md still works for ad-hoc releases.

</details>

<details>
<summary>v1.5 Hardening + Extensibility (Phases 20-27) -- SHIPPED 2026-04-28</summary>

Paid down structural tech debt across all four crates so adding the next provider, storage backend, or server feature is mechanical rather than archaeological — and closed the security gaps that blocked yard-server from leaving localhost.

- [x] **Phase 20: Quick wins — resolve.rs unwrap audit + apply.rs duplication** (5/5 plans, completed 2026-04-26)
- [x] **Phase 21: Typed configs foundation — JobType enum + AWS credential struct + deny_unknown_fields** (3/3 plans, completed 2026-04-26)
- [x] **Phase 22: yard-structs cleanup — retire vestigial validation.rs + non-generic Diff** (2/2 plans, completed 2026-04-26)
- [x] **Phase 23: StorageBackend trait — Local/S3 match collapse** (1/1 plan, completed 2026-04-26)
- [x] **Phase 24: Provider extensibility — config-extraction helper + supported-types SSoT** (2/2 plans, completed 2026-04-27)
- [x] **Phase 25: yard-server security — auth middleware + Slack webhook secret store** (5/5 plans, completed 2026-04-27)
- [x] **Phase 26: yard-server internals — polling-loop supervision + typed PlanStatus / canonical state_hash** (2/2 plans, completed 2026-04-28)
- [x] **Phase 27: yard-server end-to-end integration test — webhook → plan → PR comment** (1/1 plan, completed 2026-04-28)

Archive: [v1.5-ROADMAP.md](milestones/v1.5-ROADMAP.md) | [v1.5-REQUIREMENTS.md](milestones/v1.5-REQUIREMENTS.md) | PR [#75](https://github.com/sean-mca/yard/pull/75)

</details>

<details>
<summary>v1.6 Event-Driven DAGs (Phases 28-32) -- SHIPPED 2026-04-30</summary>

Adds event-driven trigger support to yard's Airflow DAG codegen — S3 file drops, Airflow Datasets, SQS messages, and external API triggers — preserving the existing schedule-only path byte-identical. Hard rename of legacy `triggered_by:` and `produces:` field names with no back-compat aliases. Strict bottom-up build order across five phases.

- [x] **Phase 28: Typed Trigger model + bundled `calculate_diff` ordering fix** (3/3 plans, completed 2026-04-28) — typed `Trigger` enum + 5 source variants + hard rename of `triggered_by`/`produces`; deterministic calculate_diff + HASH-01/02 invariants
- [x] **Phase 29: Trigger validation rules** (1/1 plan, completed 2026-04-29) — `validate_dag_full` + four-rule checks (TRIG-04..06) + symmetric plan/apply rollup wiring (TRIG-07)
- [x] **Phase 30: Per-source codegen** (4/4 sub-plans, completed 2026-04-29) — Datasets / S3 / SQS / API render branches + heterogeneous-all `_yard_join` + `max_active_runs=1` event-driven default + TRIG-08/CONC-02 validation
- [x] **Phase 31: Canonical op_kwargs payload threading** (3/3 plans, completed 2026-04-29) — per-source typed fields (`s3_key`, `sqs_body`, `dataset_uri`, etc.) wired into Glue/Bash task bodies via shared helper; resolves `dag_run.conf is None` pitfall
- [x] **Phase 32: `publishes:` rendering + cross-DAG warning + docs** (4/4 plans, completed 2026-04-30) — synthetic `_yard_publish` EmptyOperator + cross-DAG broken-link soft-warning + VERSION_BANNER per-DAG header + four user-facing docs (CONFIGURATION + AIRFLOW + new MIGRATION-v1.6.md)

Cross-cutting requirements PRES-01..05 (clippy clean, test suite green, no `Cargo.toml` edits, no new `unsafe`/`unwrap` in prod, schedule-only DAGs render byte-identical) enforced at every phase close.

Mid-milestone fixes shipped alongside: PR #79 (diff-time render without persisted Glue script URI), `691a950` (aws_conn_id cascade through `yard.yaml -> account -> region -> job/dag` with per-field merge), `3134288` (codegen null-coercion fix). Released as **v0.7.3**.

Archive: [v1.6-ROADMAP.md](milestones/v1.6-ROADMAP.md) | [v1.6-REQUIREMENTS.md](milestones/v1.6-REQUIREMENTS.md) | PRs [#76](https://github.com/sean-mca/yard/pull/76), [#77](https://github.com/sean-mca/yard/pull/77), [#78](https://github.com/sean-mca/yard/pull/78), [#80](https://github.com/sean-mca/yard/pull/80)

</details>

<details>
<summary>v1.7 Documentation Overhaul (Phases 33-36) -- SHIPPED 2026-05-02</summary>

Restructured `docs/` into a Diataxis-lite tree (tutorials / how-to / reference / explanation / examples / server / contributing), audited the 11 existing docs against current code, added copy-paste job-template directories, refreshed the root `README.md`, and added a CI guard that runs `yard validate` against every example. Single PR / single branch (`gsd/v1.7-docs`). Milestone PR #81.

- [x] **Phase 33: Audit + restructure scaffolding** — AUDIT-01 + RESTR-01..04 (completed 2026-05-01)
- [x] **Phase 34: Reference layer + provider docs + CODEGEN/AIRFLOW splits** — REF-01..04, AUDIT-03/04 (completed 2026-05-01)
- [x] **Phase 35: How-to recipes + examples + CI guard** — HOW-01..04, EX-01..03, CI-01 (completed 2026-05-02)
- [x] **Phase 36: Stale fixes + top-level + close** — AUDIT-02, TOP-01, TOP-02 (completed 2026-05-02)

</details>

<details>
<summary>v1.8 Code Quality + Bug Fixes (Phases 37-38) -- SHIPPED 2026-05-11</summary>

Workspace-wide audit and remediation across all 4 crates (yard-cli, yard-core, yard-structs, yard-server). Phase 37 audit drove Phase 38 critical/high bug fixes: DAG lifecycle unwrap_or_default masking diffs, lock cleanup RAII guard, hard-coded us-east-1 fallback, EMR silent degradation, drift cache corruption, CI operator precedence, _yard_default_struct missing types, WR-01..05 GitHub comment cluster. Phases 37-38, PRs through #87 merged.

- [x] **Phase 37: Workspace Quality Audit** — AUDIT-01, AUDIT-02 (5/5 plans, completed 2026-05-10)
- [x] **Phase 38: Critical + HIGH Bug Fixes** — FIX-01, FIX-02, FIX-03 (4/4 plans, completed 2026-05-11)

</details>

<details>
<summary>v1.9 yard-server Redesign (Phases 39-45) -- SHIPPED 2026-05-18</summary>

Ground-up rebuild of yard-server as an Atlantis-like server for yard repos with a Dioxus dashboard — chatOps, environment-aware UI, drift detection, and Slack alerting. The existing ~10k LOC yard-server was replaced. Decentralized state (per-env S3), credentials from yard.yaml cascade, trait-based auth (NoopAuth + OAuth2 SSO).

Cross-cutting requirements: `cargo clippy -D warnings` clean, `cargo test --workspace` green, zero `Cargo.toml` edits without explicit approval, no new `unsafe`, no new `.unwrap()`/`.expect()` in production code.

- [x] **Phase 39: Foundation** - DynamoDB schema redesign, dependency upgrades, CI for native + WASM targets (completed 2026-05-12)
- [x] **Phase 40: Environment Discovery + Credentials** - Auto-discover environments from repo structure, resolve config hierarchy, per-env STS assume-role (completed 2026-05-13)
- [x] **Phase 41: ChatOps Plan** - Auto-plan on PR, re-trigger via comment, HMAC verification, stale-plan detection, truncation with dashboard link (completed 2026-05-18)
- [x] **Phase 42: ChatOps Apply + Locking** - Apply via PR comment, per-target apply, DynamoDB locking, auto-release on merge/close, force-unlock (completed 2026-05-13)
- [x] **Phase 43: Drift Detection + Alerting** - Configurable polling, health checks, circuit breaker, Slack notifications with diff summary (completed 2026-05-13)
- [x] **Phase 44: Dashboard** - Environment-aware UI with drill-down, drift diffs, search, real-time updates, skeleton loading, sidebar, theming (completed 2026-05-14)
- [x] **Phase 45: Auth + Settings** - AuthProvider trait, NoopAuth, OAuth2 SSO (Entra + Google), session management, settings page (completed 2026-05-15)

</details>

**Out-of-band patches (NOT tracked under v1.9):**

- PR #82 -- iceberg existing-table `_yard_fill_nulls` fix (merged to `main`)
- PR #83 -- JDBC RDS IAM auth via `boto3 generate_db_auth_token` (open)

### v1.10 yard-server Production Readiness (In Progress)

Take the v1.9 yard-server rebuild from "works in dev" to deployable and trustworthy in a real environment. E2E integration tests validate the chatOps flow, health endpoints enable container orchestration, a rules compliance pass hardens the core crates, DMS provider extends coverage, graceful shutdown prevents lock orphaning and connection drops, and a multi-stage Docker image produces a deployable artifact.

Cross-cutting requirements: `cargo clippy -D warnings` clean, `cargo test --workspace` green, zero `Cargo.toml` edits without explicit approval, no new `unsafe`, no new `.unwrap()`/`.expect()` in production code. All code must adhere to the 179 rules defined in `rules/`.

- [x] **Phase 46: E2E Integration Tests** - Full webhook-to-comment chatOps flow coverage using tower::ServiceExt::oneshot + existing test doubles (completed 2026-05-19)
- [x] **Phase 47: Health Endpoints** - Liveness and readiness probes for container orchestration and ALB routing (completed 2026-05-19)
- [x] **Phase 48: Rules Compliance Rewrite** - Audit and refactor yard-core, yard-cli, and yard-structs against all 179 Rust coding standards in rules/ (completed 2026-05-24)
- [x] **Phase 49: DMS Provider** - Add AWS Database Migration Service support as a new provider following trait-based architecture (completed 2026-05-24)
- [x] **Phase 50: Graceful Shutdown** - SIGTERM handling, background task cancellation, WebSocket drain, lock release (completed 2026-05-24)
- [x] **Phase 50.1: Iceberg Schema-Conform Rewrite** *(INSERTED — EMERGENCY, 2026-05-29; completed 2026-05-29, shipped v1.9.1)* - Replace void-only dual-arm null coercion with single-pass `df.to()` schema-conform
- [ ] **Phase 51: Docker Container Image** *(POSTPONED 2026-05-29 — deprioritized in favor of state/perf work)* - Multi-stage Dockerfile with cargo-chef + dx bundle producing a minimal runtime image
- [ ] **Phase 54: Codegen + Validation Performance** - Profile-first, then optimize hot paths in codegen and validation

### v1.13 State Scoping + Distributed State (Not Started)

Decouple `yard validate` and `--target` ops from full AWS credentials, then co-locate state with each job's AWS account for distributed state management.

- [x] **Phase 52: State Scoping + Offline Validation** - `yard validate` runs with no AWS creds; `--target` ops access only the targeted job's state (completed 2026-07-20)
  **Plans:** 3 plans
  Plans:

  - [x] 52-01-PLAN.md — yard-core foundation: ResolvedManifest, resolve_manifest(), load_job_state(), show_dag_with_fallback()
  - [x] 52-02-PLAN.md — CLI consumer switch: validate, list targets, show use resolve_manifest()
  - [x] 52-03-PLAN.md — Target-scoped state wiring: plan/apply use load_job_state with --target, destroy/force_unlock use resolve_manifest
- [ ] **Phase 53: Account-Distributed State** - State co-located with each job's AWS account; per-target/account-aware storage-backend resolution

<details>
<summary>v1.11 Airflow 3.0 Asset Support (Phases 55-57) -- SHIPPED 2026-06-15</summary>

Version-aware codegen so yard emits `Asset` (AF3) or `Dataset` (AF2) depending on a new `airflow.version` config field, with per-environment control via the config cascade. Internal Rust types unchanged -- the version switch controls only emitted Python string literals and import paths.

Archive: [v1.11-ROADMAP.md](milestones/v1.11-ROADMAP.md) | [v1.11-REQUIREMENTS.md](milestones/v1.11-REQUIREMENTS.md)

- [x] **Phase 55: Type Foundation + Config Cascade** - `AirflowMajorVersion` enum, `airflow.version` field, config cascade, `"asset"` trigger alias (completed 2026-06-13)
- [x] **Phase 56: Version-Aware Codegen + Tests** - 15 emission sites version-gated, 22 new V3 tests (completed 2026-06-15)
- [x] **Phase 57: Documentation** - Migration guide, configuration reference, airflow-dag reference AF3 updates (completed 2026-06-15)

</details>

<details>
<summary>v1.12 Code Audit (Phases 58-59) -- SHIPPED 2026-06-17</summary>

Archive: [v1.12-ROADMAP.md](milestones/v1.12-ROADMAP.md) | [v1.12-REQUIREMENTS.md](milestones/v1.12-REQUIREMENTS.md)

- [x] Phase 58: yard-structs + yard-cli Audit (2/2 plans) — completed 2026-06-16
- [x] Phase 59: yard-core Audit (6/6 plans) — completed 2026-06-17

</details>

<details>
<summary>v1.14 PII Detection & Masking (Phases 60-62) -- SHIPPED 2026-06-25</summary>

Declarative `mask_pii` support in `job.yaml` that generates `EntityDetector.detect()` code blocks in Glue codegen with REDACT action and `"****"` mask text. Uses Glue-native `awsglueml.transforms.EntityDetector` — zero new Rust or Python dependencies. 16 requirements across CFG/VAL/GEN/TEST/DOC categories, all complete.

Archive: [v1.14-ROADMAP.md](milestones/v1.14-ROADMAP.md) | [v1.14-REQUIREMENTS.md](milestones/v1.14-REQUIREMENTS.md)

- [x] **Phase 60: Config & Validation Foundation** (2/2 plans) — `mask_pii` field on `JobDefinition`, entity type validation, EMR rejection rule (completed 2026-06-24)
- [x] **Phase 61: Codegen & Tests** (2/2 plans) — `codegen/pii.rs` module, DynamicFrame conversion, EntityDetector emission, import management, full test coverage (completed 2026-06-24)
- [x] **Phase 62: Documentation** (1/1 plan) — `docs/reference/configuration.md` updated with `mask_pii` field docs, entity type examples, Glue 3.0+ note (completed 2026-06-25)

</details>

<details>
<summary>v1.15 Distribution (Phase 63) -- SHIPPED 2026-07-19</summary>

Tag-triggered GitHub Actions release workflow that cross-compiles yard-cli for 4 targets (x86_64-apple-darwin, aarch64-apple-darwin, x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu) and uploads binaries as GitHub Release assets; plus a Homebrew tap formula for macOS installation. Retires deferred v1.4 Phases 16-18.

- [x] **Phase 63: Cross-Platform Releases + Homebrew** — release workflow + Homebrew tap formula (completed 2026-07-19)

Plans:

- [x] 63-01-PLAN.md — Release workflow (validate-version, build matrix, release, homebrew update)
- [x] 63-02-PLAN.md — Cross-platform action.yml rewrite
- [x] 63-03-PLAN.md — External setup (tap repo, PAT, secret)

</details>

<details>
<summary>v1.16 Rules Compliance Audit (Phases 64-65) -- SHIPPED 2026-08-18</summary>

Full codebase audit of yard-cli, yard-core, and yard-structs against the 179 Rust coding standards in `rules/`, catching drift since the v1.12 audit (June 2026). yard-server excluded per precedent (v1.12). Each phase audits its crate subset against ALL 14 rule categories.

- [x] **Phase 64: yard-structs + yard-cli Audit & Fix** (2/2 plans, completed 2026-08-18)
- [x] **Phase 65: yard-core Audit & Fix** (3/3 plans, completed 2026-08-18)

</details>

### v2.0 Plugin Architecture (Phases 66-70)

Transform yard from a monolithic binary with compiled-in providers into an orchestrator that delegates provider-specific work (validate, codegen, deploy, destroy, verify, schema) to out-of-process plugin binaries communicating over JSON-over-stdio. Providers (Glue, EMR) move to separate repos; this milestone builds the plugin infrastructure and slims the core.

Cross-cutting requirements: `cargo clippy -D warnings` clean, `cargo test --workspace` green, zero `Cargo.toml` edits without explicit approval, no new `unsafe`, no new `.unwrap()`/`.expect()` in production code. All code must adhere to the 179 rules defined in `rules/`.

- [x] **Phase 66: Plugin Protocol + Host** - JSON-over-stdio protocol types, process spawner, PluginProvider adapter (completed 2026-08-31)
- [x] **Phase 67: Plugin SDK** - `yard-plugin-sdk` workspace crate with PluginServer::run() and PluginHandler trait (completed 2026-09-01)
- [ ] **Phase 68: Provider-Scoped Config Cascade** - Provider-scoped config sections at all hierarchy levels, schema-driven validation
- [x] **Phase 69: Plugin Distribution** - `yard init` downloads, checksum-verifies, and caches plugin binaries (completed 2026-09-01)
- [ ] **Phase 70: Core Slimming + Documentation** - Remove compiled-in providers, migration guide, plugin author guide

## Phase Details

### Phase 66: Plugin Protocol + Host

**Goal**: yard-core can spawn plugin binaries as child processes and exchange typed JSON-over-stdio messages for all provider operations
**Depends on**: Nothing (first v2.0 phase)
**Requirements**: PROTO-01, PROTO-02, PROTO-03, PROTO-04, HOST-01, HOST-02, HOST-03, HOST-04
**Success Criteria** (what must be TRUE):

  1. A test plugin binary spawned by yard-core correctly handles all 6 operations (validate, codegen, deploy, destroy, verify, schema) over line-delimited JSON
  2. The plugin sends a version/capabilities handshake on startup; yard-core rejects incompatible versions and the plugin terminates when stdin closes (EOF)
  3. Progress lines emitted by a plugin during long-running operations are received by yard-core without blocking the response flow
  4. A plugin that exceeds the configured timeout is killed and the user sees an actionable error message
  5. PluginProvider implements the existing Provider trait so orchestrate.rs dispatches to plugins with zero changes to the orchestration flow

**Plans:** 3/3 plans complete
Plans:
**Wave 1**

- [x] 66-01-PLAN.md — Protocol types in yard-structs + Provider trait expansion (3->6 methods)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 66-02-PLAN.md — Plugin host module (spawner + PluginProvider adapter)

**Wave 3** *(blocked on Wave 2 completion)*

- [x] 66-03-PLAN.md — Test plugin binary + integration tests

### Phase 67: Plugin SDK

**Goal**: Plugin authors can build provider plugins using a Rust SDK that handles all protocol mechanics
**Depends on**: Phase 66
**Requirements**: SDK-01, SDK-02, SDK-03
**Success Criteria** (what must be TRUE):

  1. A minimal plugin binary using `PluginServer::run()` and implementing `PluginHandler` compiles and responds correctly to host requests for all 6 operations
  2. Plugin author code cannot accidentally write to stdout -- the SDK captures stdout for protocol framing and provides a separate logging channel via stderr
  3. SDK re-exports Resource and ResourceStatus types from yard-structs so plugin authors do not need a direct yard-structs dependency

**Plans:** 2/2 plans complete
Plans:
**Wave 1**

- [x] 67-01-PLAN.md — SDK crate foundation (handler trait, stdout capture, server run loop, re-exports)

**Wave 2** *(blocked on Wave 1 completion)*

- [x] 67-02-PLAN.md — SDK integration verification (SDK-based test plugin + host interop tests)

### Phase 68: Provider-Scoped Config Cascade

**Goal**: Config cascade resolves provider-specific sections per job type and validates config fields dynamically via the plugin's schema operation
**Depends on**: Phase 67
**Requirements**: CASC-01, CASC-02, CASC-03
**Success Criteria** (what must be TRUE):

  1. A `providers.<type>:` section at any hierarchy level (yard.yaml, account.yaml, region.yaml, job.yaml) merges into jobs of that type with correct four-level precedence
  2. Common fields (`aws:`, `state:`) cascade to all job types regardless of whether a provider-scoped section exists
  3. A plugin's `schema` response determines which config fields are valid for that provider, replacing the hardcoded ALLOWED_* lists in validation

**Plans:** 0/2 plans executed
Plans:
**Wave 1**

- [ ] 68-01-PLAN.md — Config cascade syntax enforcement + SchemaResponse extension (CASC-01, CASC-02)

**Wave 2** *(blocked on Wave 1 completion)*

- [ ] 68-02-PLAN.md — Schema-driven validation pipeline (CASC-03)

### Phase 69: Plugin Distribution

**Goal**: Users can declare provider plugins in yard.yaml and `yard init` downloads, verifies, and caches the correct platform-specific binaries
**Depends on**: Phase 66
**Requirements**: DIST-01, DIST-02, DIST-03, DIST-04
**Success Criteria** (what must be TRUE):

  1. Running `yard init` reads provider declarations from yard.yaml and downloads the correct platform-specific binary from the configured GitHub release URL
  2. Downloaded binaries are verified via SHA-256 checksum before being placed in the cache; a checksum mismatch produces an error and does not cache the binary
  3. Plugin binaries are cached at `~/.yard/plugins/<name>-<version>-<os>-<arch>` and reused across projects without re-downloading
  4. A version mismatch between the version pinned in yard.yaml and the cached binary produces a clear error at startup

**Plans**: 2/2 plans complete

- [x] 69-01-PLAN.md
- [x] 69-02-PLAN.md

### Phase 70: Core Slimming + Documentation

**Goal**: yard-core contains no compiled-in provider code, AWS SDK provider dependencies are removed, and v2.0 migration and plugin authoring documentation is published
**Depends on**: Phase 68, Phase 69
**Requirements**: SLIM-01, SLIM-02, SLIM-03, DOC-01, DOC-02
**Success Criteria** (what must be TRUE):

  1. `JobType::Plugin(String)` variant enables dynamic provider resolution so adding a new provider requires no changes to the yard-core enum
  2. aws-sdk-glue and aws-sdk-emr are not in yard-core's dependency tree (`cargo tree -p yard-core` shows neither)
  3. A v1.x user can follow the migration guide to upgrade to v2.0 -- covering new config format, `yard init` requirement, and plugin binary setup
  4. A plugin author can follow the author guide to build a provider plugin from scratch using yard-plugin-sdk, test it locally, and release it

**Plans:** 3 plans
Plans:
**Wave 1** *(parallel)*

- [ ] 70-01-PLAN.md — JobType rewrite to Plugin(String), module deletion (~11k lines), dependency removal, provider dispatch rewrite
- [ ] 70-03-PLAN.md — v2.0 migration guide, plugin author guide, doc cleanup

**Wave 2** *(blocked on Wave 1 completion)*

- [ ] 70-02-PLAN.md — Rewire remaining yard-core modules and CLI commands for plugin-only architecture

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 46. E2E Integration Tests | v1.10 | 2/2 | Complete | 2026-05-19 |
| 47. Health Endpoints | v1.10 | 1/1 | Complete | 2026-05-19 |
| 48. Rules Compliance Rewrite | v1.10 | 10/10 | Complete | 2026-05-24 |
| 49. DMS Provider | v1.10 | 0/2 | Not started | - |
| 50. Graceful Shutdown | v1.10 | 2/2 | Complete | 2026-05-24 |
| 51. Docker Container Image | v1.10 | 0/? | Not started | - |
| 52. State Scoping + Offline Validation | v1.13 | 3/3 | Complete | 2026-07-20 |
| 53. Account-Distributed State | v1.13 | 0/? | Not started | - |
| 54. Codegen + Validation Performance | v1.10 | 0/? | Not started | - |
| 66. Plugin Protocol + Host | v2.0 | 3/3 | Complete    | 2026-08-31 |
| 67. Plugin SDK | v2.0 | 2/2 | Complete   | 2026-09-01 |
| 68. Provider-Scoped Config Cascade | v2.0 | 0/2 | Planned    |  |
| 69. Plugin Distribution | v2.0 | 2/2 | Complete    | 2026-09-01 |
| 70. Core Slimming + Documentation | v2.0 | 0/3 | Planned | - |

**Out-of-band patches (NOT tracked under v1.9 or v1.10):**

- PR #82 -- iceberg existing-table `_yard_fill_nulls` fix (merged to `main`)
- PR #83 -- JDBC RDS IAM auth via `boto3 generate_db_auth_token` (open)
