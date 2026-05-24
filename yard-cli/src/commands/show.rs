//! Handler for the `yard show` subcommand.

use super::resolve_project;
use anyhow::Result;

/// Execute `yard show <name>`: print the generated script for a job or DAG.
///
/// Tries the `name` as a job first; if no matching job exists, falls back
/// to DAG lookup. The generated script is written to stdout.
///
/// # Errors
///
/// Returns an error if project resolution fails or if `name` matches
/// neither a job nor a DAG in the manifest.
pub async fn execute(name: String, directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    // Try as a job first
    if project.manifest.jobs.contains_key(&name) {
        let script = yard_core::show(&project.manifest, &name)?;
        print!("{script}");
        return Ok(());
    }

    // Try as a DAG
    let dags = yard_core::airflow_dag::collect_dags(&project.root_dir, &project.manifest)?;
    let script = yard_core::show_dag_with_state(
        &project.manifest,
        &dags,
        &name,
        &project.manifest.state,
    )
    .await?;
    print!("{script}");
    Ok(())
}
