<!-- generated-by: gsd-doc-writer -->
# yard-core

The business-logic heart of [yard](../README.md) — all codegen, providers, state
management, storage, and validation live here. The `yard-cli` binary is a thin
wrapper over this crate, and `yard-server` consumes the same public API for
webhook-driven apply/plan and drift detection.

Part of the yard workspace. See the [root README](../README.md) and
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for the system-level picture.

## Installation

Internal workspace crate — not published to crates.io. Depend on it via a path
from elsewhere in the workspace:

```toml
[dependencies]
yard-core = { path = "../yard-core" }
```

## Usage

Top-level entry points are re-exported from the crate root (`src/lib.rs`):

```rust
use yard_core::{apply, calculate_diff, destroy_all, load_state};
use yard_core::{apply_dags, calculate_dag_diffs, load_dag_state};
use yard_core::parsing::parse_job_file;
```

A typical flow used by the CLI and server:

1. Parse `*.yaml` job files with `parsing::parse_job_file`.
2. Resolve hierarchical config inheritance with `resolve` + `config_merge`.
3. Validate jobs with `validation::validate_job_full`.
4. Compute diffs against state with `calculate_diff` / `calculate_dag_diffs`.
5. Apply or destroy via `orchestrate::apply` / `dag_lifecycle::apply_dags`.

## Module Layout (`src/`)

| Module | Role |
| --- | --- |
| `providers/` | `Provider` trait + concrete `glue`, `emr` implementations; shared `aws_config`, `S3ScriptOps` helpers |
| `codegen/` | PySpark script generation via Tera templates (`source`, `transform`, `sink` sub-modules) |
| `airflow_dag/` | Airflow DAG discovery, config resolution, and Python DAG file generation |
| `templates/` | Bundled Tera templates: `glue.py.tera`, `emr.py.tera`, `airflow_dag.py.tera` (compiled in via `include_str!`) |
| `parsing.rs` | yaml-rust2-based parsers for `*.yaml` job files and Airflow sections |
| `config_merge.rs` | Hierarchical config merge (terragrunt-style) between root, intermediate, and job-level config |
| `resolve.rs` | Walks the directory tree to resolve a job's effective config |
| `validation/` | Schema validation (`rules`) + generated PySpark syntax checking (`syntax`) |
| `storage.rs` | `LocalStorage` and `S3Storage` backends behind a `Storage` enum for state files |
| `orchestrate.rs` | Job-level lifecycle: `apply`, `destroy_all`, `destroy_job`, `force_unlock`, drift `verify_deployed_resources` |
| `dag_lifecycle.rs` | DAG-level lifecycle: `apply_dags`, `destroy_dag`, `destroy_all_dags`, `calculate_dag_diffs` |
| `diff.rs` | Compute `calculate_diff` between desired config and tracked state |
| `show.rs` | `show` / `show_dag` — human-readable output of resolved config |
| `utils.rs` | Shared helpers |

## The `Provider` Trait

Defined in [`src/providers/mod.rs`](src/providers/mod.rs). Each target service
(Glue, EMR, and future providers like Databricks) implements:

```rust
pub trait Provider: Send + Sync {
    fn deploy(&self, job_name: &str, artifact: &str, job_config: &Value)
        -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>>;

    fn destroy(&self, job_name: &str, resources: &[Resource])
        -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    fn verify_resources(&self, job_name: &str, resources: &[Resource])
        -> Pin<Box<dyn Future<Output = Result<Vec<ResourceStatus>>> + Send + '_>>;
}
```

Provider-level config (deploy roles, script buckets) is passed at construction;
job-level config (execution role, sources, sink) is passed per call. `deploy`
returns the `Resource`s that were created or updated so the state backend can
track them. `verify_resources` powers drift detection by checking that tracked
resources still exist in the target service.

### Adding a new provider

1. Create a new module under `src/providers/` (e.g. `databricks.rs`).
2. Implement `Provider` for your struct. Use `S3ScriptOps` from
   `providers/mod.rs` if you need to upload generated PySpark scripts to S3.
3. Wire the new `job_type` into `get_provider` in `src/providers/mod.rs`.
4. If a new codegen target is needed, add a Tera template under `src/templates/`
   and include it from `codegen/mod.rs` with `include_str!`.

## Dependencies

Relies on the sibling `yard-structs` crate for all shared data types
(`JobDefinition`, `Resource`, `ResourceStatus`, `DagState`, `JobState`,
`LockInfo`, `StateBackend`, `ValidationError`, `AirflowSection`). Keep shared
types in `yard-structs` rather than inside this crate so `yard-cli` and
`yard-server` can depend on them without pulling in all business logic.

External deps include `tokio`, `anyhow`, `tera`, `yaml-rust2`, `serde_json`,
`walkdir`, `chrono`, `blake3`, and the `aws-sdk-*` crates for Glue, EMR, and S3.

## Testing

From the workspace root:

```bash
cargo test -p yard-core
```

Integration tests live in [`tests/`](tests/) (`glue_integration.rs`,
`emr_integration.rs`). Every change must pass `cargo clippy -D warnings`.

## License

See [LICENSE](../LICENSE) at the workspace root.
