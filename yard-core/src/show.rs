//! Show command implementation for previewing generated artifacts.
//!
//! Provides read-only generation of job scripts and DAG Python files without
//! deploying or modifying state. Used by `yard show job <name>` and
//! `yard show dag <name>` CLI commands.

use anyhow::{anyhow, Context, Result};
use yard_structs::{ProjectManifest, StateBackend};

use crate::airflow_dag;
use crate::dag_lifecycle;
use crate::storage;

/// Generate and return the Python content for a DAG without deploying.
///
/// Requires a storage handle so the renderer can read each Glue task's
/// persisted script URI (DAG-02). On an un-applied Glue task, surfaces
/// the D-07 "run 'yard apply'" error unfiltered -- that is the intended
/// contract per phase CONTEXT.md D-04.
///
/// # Errors
///
/// Returns an error if the DAG name is not found, if script locations
/// cannot be loaded, or if DAG generation fails.
pub async fn show_dag(
    manifest: &ProjectManifest,
    dags: &[crate::airflow_dag::ResolvedDag],
    dag_name: &str,
    storage: &storage::Storage,
) -> Result<String> {
    let dag = dags
        .iter()
        .find(|d| d.name == dag_name)
        .ok_or_else(|| anyhow!("DAG \"{dag_name}\" not found"))?;

    // Pre-load JobStates so the renderer can read each Glue task's
    // persisted script_location. Mirrors dag_lifecycle::apply_dags.
    let script_locations = dag_lifecycle::load_script_locations_from_storage(storage).await?;

    airflow_dag::generate_dag(manifest, dag, &script_locations)
        .with_context(|| format!("Failed to generate DAG \"{dag_name}\""))
}

/// CLI-friendly wrapper: open storage from a state backend, then call `show_dag`.
///
/// Keeps storage handling inside yard-core per CLAUDE.md "All logic in yard-core;
/// CLI just parses args and displays."
///
/// # Errors
///
/// Returns an error if the storage backend cannot be initialized or if
/// `show_dag` fails.
pub async fn show_dag_with_state(
    manifest: &ProjectManifest,
    dags: &[crate::airflow_dag::ResolvedDag],
    dag_name: &str,
    backend: &StateBackend,
) -> Result<String> {
    let storage = storage::get_storage(backend).await?;
    show_dag(manifest, dags, dag_name, &storage).await
}

/// Generate and return the script for a job without deploying or modifying state.
///
/// # Errors
///
/// Returns an error if the job name is not found in the manifest or if
/// script generation fails.
pub fn show(manifest: &ProjectManifest, job_name: &str) -> Result<String> {
    let job_def = manifest
        .jobs
        .get(job_name)
        .ok_or_else(|| anyhow!("Job \"{job_name}\" not found in manifest"))?;

    crate::codegen::generate_python_script(job_name, job_def)
        .with_context(|| format!("Failed to generate script for job \"{job_name}\""))
}
