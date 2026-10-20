# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- A top-level `plugins:` block in `yard.yaml`, keyed by job type, pins a plugin once with `version` and `source` instead of repeating `plugin_version` / `plugin_source` in every job file. The same block may appear in `account.yaml` and `region.yaml`; pins cascade `yard.yaml` -> `account.yaml` -> `region.yaml` -> job file field by field, and the job file wins.
- `yard_structs::PluginPin` and `ProjectManifest.plugins` expose the `yard.yaml` pins to embedders; the map is left out of serialized manifests when empty.

### Changed

- `plugin_version` / `plugin_source` under `providers.<type>` (in any layer, or a job's own `<type>:` block) is now an error that points at `plugins.<type>`. Previously they were silently passed to the plugin as config and the job ran unpinned.
- `plugins` is now a reserved key in `account.yaml` / `region.yaml`; entries with unknown keys are rejected.
- Per-job `plugin_version` / `plugin_source` are optional when a `plugins` entry covers the job's type.

## [2.1.2] - 2026-10-19

### Added

- `plugin_source` accepts local binaries (`file://path`, or a bare `./`, `../`, `/`, `~/` path); `plugin_version` is ignored for them and no checksum is recorded.
- `plugin_source` accepts git repositories (`git::<url>`, `git+<url>`, `git@...`, `ssh://`, or any URL ending in `.git`, with an optional `//subdir`). The repo is cloned at the tag, branch, or commit named by `plugin_version`, then built with cargo or used as a script.
- `plugin_source` accepts a bare GitHub repository URL; yard derives the release asset `<name>-<version>-<target triple>` for the running platform. Directory URLs (trailing `/`) and `${name}`/`${version}`/`${target}`/`${os}`/`${arch}`/`${platform}` templates are also supported.
- `yard validate` now calls each job's plugin: its `schema` operation (so plugin-declared source/sink types validate) and its `validate` operation. `--skip-plugins` restores structural-only checks.
- `yard_core::validate_jobs` and `yard_core::plugin_host_config_for` for callers embedding yard.

### Changed

- Plugins now receive the `providers.<type>` block from `yard.yaml` (deep-merged with the job's own `<type>:` block) nested under `<type>` in `job_config`, alongside the parsed job. Previously the provider block was dropped before reaching the plugin.
- Deploy, destroy, and verify no longer require a `providers.<type>` block in `yard.yaml`; they run whenever the job has a `plugin_source`.
- A configured plugin that fails codegen is now an error in `plan` and `apply` instead of silently hashing or deploying an empty script.
- `${name}`, `${version}`, and other single-segment placeholders in job files are no longer rejected by context interpolation.
- Release asset naming convention is now `<name>-<version>-<target triple>` (e.g. `yard-plugin-glue-0.1.0-aarch64-apple-darwin`), matching cargo cross-compile targets.
- `providers::get_provider_for_job` no longer takes a provider config argument; `destroy_job` / `destroy_all` no longer take the providers map; `Provider::schema` returns the full `SchemaResponse`.

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
- Docs for removed subsystems: `reference/codegen.md`, `reference/airflow-dag.md`,
  `how-to/schedule-a-dag.md`, `how-to/debug-codegen-output.md`,
  `explanation/why-codegen.md`, and the `multi-job-dag` example
