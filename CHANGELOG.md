# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-09-02

### Breaking Changes

- **Plugin architecture**: Providers (Glue, EMR) are now external plugin binaries instead of compiled into yard. Install provider plugins separately (`yard-plugin-glue`, `yard-plugin-emr`).
- **Job config**: `plugin_version` and `plugin_source` fields are now required in `job.yaml` for all job types.
- **DAG generation**: Airflow DAG generation removed from core. A future Airflow plugin will provide this functionality.
- **CLI**: `yard show dag` command removed (DAG generation is now a plugin responsibility).
- **Dependencies**: `aws-sdk-glue`, `aws-sdk-emr`, and `tera` removed from the yard binary. Binary size is significantly smaller.

See [v2.0 Migration Guide](docs/reference/migrations/v2.0.md) for upgrade instructions.

### Added

- Plugin protocol (JSON-over-stdio) for out-of-process provider execution
- `yard-plugin-sdk` crate for building provider plugins in Rust
- Plugin auto-download from GitHub Releases with TOFU checksum verification (`yard.lock`)
- Provider-scoped config cascade (`provider:` block in context files)
- v2.0 migration guide (`docs/reference/migrations/v2.0.md`)
- Plugin author guide (`docs/how-to/build-a-plugin.md`)

### Changed

- `JobType` enum simplified to `Plugin(String)` — single variant, backward-compatible serde
- `calculate_diff` is now async (plugin codegen runs out-of-process)
- Provider dispatch routes exclusively through `PluginProvider`
- Plugin fields (`plugin_version`, `plugin_source`) persisted in deployment state for destroy operations

### Removed

- Compiled-in Glue provider (~600 lines)
- Compiled-in EMR provider (~372 lines)
- Codegen module (~3.1k lines)
- Airflow DAG module (~6.2k lines)
- DAG lifecycle module (~1.1k lines)
- PySpark Tera templates (`glue.py.tera`, `emr.py.tera`, `airflow_dag.py.tera`)
- Provider-specific validation (now driven by plugin `schema()` operation)
