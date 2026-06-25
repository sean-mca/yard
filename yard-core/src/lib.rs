#![warn(missing_docs)]
//! Business logic crate for **yard** -- a terragrunt-inspired CLI for data
//! engineering teams.
//!
//! All domain logic lives here; the `yard-cli` crate is a thin wrapper that
//! parses arguments and displays output. Key modules:
//!
//! - [`orchestrate`] -- top-level `apply`, `plan`, `destroy`, `init` entry points
//! - [`resolve`] -- project discovery, YAML config cascade, variable resolution
//! - [`codegen`] -- PySpark script generation via Tera templates
//! - [`airflow_dag`] -- Airflow DAG generation, trigger rendering, connections
//! - [`storage`] -- per-job and per-DAG state persistence (local FS, S3)
//! - [`validation`] -- schema and semantic validation rules
//! - [`providers`] -- AWS Glue / EMR provider implementations
//! - [`diff`] -- manifest-vs-state diff computation
//! - [`dag_lifecycle`] -- DAG apply/destroy/diff lifecycle
//! - [`config_merge`] -- provider config deep-merge, task-only classification
//! - [`parsing`] -- YAML-to-typed-struct parsers for jobs, sources, sinks, airflow blocks
//! - [`mod@list_targets`] -- deployment target enumeration for CI matrix builders
//! - [`mod@show`] -- script/DAG preview without deploying
//! - [`utils`] -- hashing, variable resolution

pub mod airflow_dag;
pub mod codegen;
/// Provider config deep-merge and task-only classification.
pub mod config_merge;
/// DAG apply/destroy/diff lifecycle orchestration.
pub mod dag_lifecycle;
/// Manifest-vs-state diff computation.
pub mod diff;
pub mod list_targets;
/// Top-level `apply`, `plan`, `destroy`, `init` entry points.
pub mod orchestrate;
/// YAML-to-typed-struct parsers for jobs, sources, sinks, and airflow blocks.
pub mod parsing;
pub mod providers;
/// Project discovery, YAML config cascade, and variable resolution.
pub mod resolve;
/// Script/DAG preview without deploying.
pub mod show;
/// Per-job and per-DAG state persistence (local FS, S3).
pub mod storage;
/// Hashing, variable resolution, and misc utilities.
pub mod utils;
pub mod validation;

pub use config_merge::{build_provider_config, is_task_only, merge_provider_config};
pub use dag_lifecycle::{
    apply_dags, calculate_dag_diffs, destroy_all_dags, destroy_dag,
    load_dag_state, load_script_locations, DagApplyResult, DagDestroyResult,
};
pub use diff::calculate_diff;
pub use orchestrate::{
    apply, destroy_all, destroy_job, force_unlock, init_state_backend,
    load_state, plan, validate_target, verify_deployed_resources,
    ApplyResult, DestroyResult, PlanResult,
};
pub use parsing::{
    merge_airflow_sections, parse_airflow_job_block, parse_airflow_section,
    parse_body, parse_create_timestamp, parse_imports, parse_job_file,
    parse_mask_pii, parse_partition_by, parse_partition_timestamp_column,
    parse_sink, parse_sources, parse_transforms,
};
pub use show::{show, show_dag, show_dag_with_state};
pub use list_targets::{list_targets, TargetRow};
