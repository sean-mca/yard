# Requirements: yard

**Defined:** 2026-08-31
**Core Value:** The CLI must remain correct and easy to reason about — every refactor must preserve existing behavior and pass the full test suite.

## v2.0 Requirements

Requirements for plugin architecture release. Each maps to roadmap phases.

### Plugin Protocol

- [x] **PROTO-01**: yard defines a JSON-over-stdio protocol with 6 typed operations (validate, codegen, deploy, destroy, verify, schema)
- [x] **PROTO-02**: Plugin sends a version/capabilities handshake line on startup before receiving requests
- [x] **PROTO-03**: Plugin can emit progress lines (`{"type":"progress"}`) during long-running operations
- [x] **PROTO-04**: Protocol uses line-delimited JSON framing (one JSON object per line)

### Plugin Host

- [x] **HOST-01**: yard-core spawns plugin binaries as child processes with piped stdin/stdout and inherited stderr
- [x] **HOST-02**: Plugin process terminates when stdin closes (EOF from core)
- [x] **HOST-03**: yard-core enforces a configurable timeout on plugin operations and kills unresponsive processes
- [x] **HOST-04**: PluginProvider adapter implements the Provider trait, transparent to orchestrate.rs

### Plugin SDK

- [x] **SDK-01**: `yard-plugin-sdk` workspace crate provides `PluginServer::run()` entry point and `PluginHandler` trait with 6 methods
- [x] **SDK-02**: SDK owns stdout for protocol framing; plugin author code cannot accidentally corrupt the protocol channel
- [x] **SDK-03**: SDK re-exports shared types (Resource, ResourceStatus) from yard-structs

### Config Cascade

- [ ] **CASC-01**: Config cascade supports provider-scoped sections (`providers.<type>:`) at yard.yaml, account.yaml, region.yaml, and job.yaml levels
- [ ] **CASC-02**: Only the matching provider's section merges into jobs of that type; common fields (`aws:`, `state:`) cascade universally
- [ ] **CASC-03**: Plugin's `schema` operation tells core what config fields the provider accepts, replacing hardcoded validation lists

### Distribution

- [ ] **DIST-01**: `yard init` reads provider declarations from `yard.yaml` and downloads platform-specific binaries from GitHub release URLs
- [ ] **DIST-02**: Downloaded binaries are verified via SHA-256 checksum before caching
- [ ] **DIST-03**: Binaries are cached at `~/.yard/plugins/<name>-<version>-<os>-<arch>`
- [ ] **DIST-04**: Provider version is pinned in `yard.yaml`; version mismatch is an error at startup

### Core Slimming

- [ ] **SLIM-01**: `JobType::Plugin(String)` variant enables dynamic provider resolution without modifying the enum for each new provider
- [ ] **SLIM-02**: Compiled-in Glue and EMR provider code is removed from yard-core
- [ ] **SLIM-03**: aws-sdk-glue and aws-sdk-emr dependencies are removed from yard-core's Cargo.toml

### Documentation

- [ ] **DOC-01**: Migration guide covers v1.x → v2.0 breaking changes (provider config format, `yard init` requirement, plugin binary setup)
- [ ] **DOC-02**: Plugin author guide documents the protocol, SDK usage, and how to build/release a provider plugin

## Future Requirements

Deferred to post-v2.0. Tracked but not in current roadmap.

### Databricks Provider

- **DBX-01**: Databricks Jobs API provider (deploy, destroy, verify_resources) via OAuth M2M
- **DBX-02**: Full PySpark codegen via Tera templates (`databricks.py.tera`)
- **DBX-03**: Unity Catalog integration (default catalog/schema per job)
- **DBX-04**: Cluster policy support (reference by ID in config)
- **DBX-05**: Init scripts attachment
- **DBX-06**: Airflow DAG integration via `DatabricksSubmitRunOperator`

### Plugin Ecosystem

- **ECO-01**: Cross-provider DAG orchestration (mixed provider tasks in one DAG)
- **ECO-02**: Plugin-provided Airflow task templates (plugin returns rendered operator snippet)
- **ECO-03**: Plugin registry / marketplace

## Out of Scope

| Feature | Reason |
|---------|--------|
| gRPC plugin protocol | yard's 6 operations are simple request/response; gRPC adds protobuf deps for no benefit |
| Dynamic library / .so plugins | Rust has no stable ABI; process isolation is safer |
| WASM plugins | wasmtime dep (~20MB), capability constraints (no fs/network without WASI) |
| Auto-update plugins | Breaks reproducibility; use `yard init --upgrade` instead |
| Long-lived plugin processes | yard operations are atomic; process-per-operation is simpler |
| Plugin hot-reload | No persistent process to reload |
| Databricks provider in this repo | Providers live in their own repos with independent release cycles |
| Glue/EMR plugin repos | Separate projects; v2.0 only removes them from core |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| PROTO-01 | Phase 66 | Verified |
| PROTO-02 | Phase 66 | Verified |
| PROTO-03 | Phase 66 | Verified |
| PROTO-04 | Phase 66 | Verified |
| HOST-01 | Phase 66 | Verified |
| HOST-02 | Phase 66 | Verified |
| HOST-03 | Phase 66 | Verified |
| HOST-04 | Phase 66 | Verified |
| SDK-01 | Phase 67 | Complete |
| SDK-02 | Phase 67 | Complete |
| SDK-03 | Phase 67 | Complete |
| CASC-01 | Phase 68 | Pending |
| CASC-02 | Phase 68 | Pending |
| CASC-03 | Phase 68 | Pending |
| DIST-01 | Phase 69 | Pending |
| DIST-02 | Phase 69 | Pending |
| DIST-03 | Phase 69 | Pending |
| DIST-04 | Phase 69 | Pending |
| SLIM-01 | Phase 70 | Pending |
| SLIM-02 | Phase 70 | Pending |
| SLIM-03 | Phase 70 | Pending |
| DOC-01 | Phase 70 | Pending |
| DOC-02 | Phase 70 | Pending |

**Coverage:**

- v2.0 requirements: 23 total
- Mapped to phases: 23
- Unmapped: 0

---
*Requirements defined: 2026-08-31*
*Last updated: 2026-08-31 after roadmap creation*
