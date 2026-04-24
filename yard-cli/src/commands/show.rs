use super::resolve_project;
use anyhow::Result;

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
