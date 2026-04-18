use anyhow::{anyhow, Context, Result};
use yard_structs::ProjectManifest;

/// Generate and return the Python content for a DAG without deploying.
pub fn show_dag(
    manifest: &ProjectManifest,
    dags: &[crate::airflow_dag::ResolvedDag],
    dag_name: &str,
) -> Result<String> {
    let dag = dags
        .iter()
        .find(|d| d.name == dag_name)
        .ok_or_else(|| anyhow!("DAG \"{dag_name}\" not found"))?;

    crate::airflow_dag::generate_dag(manifest, dag)
        .with_context(|| format!("Failed to generate DAG \"{dag_name}\""))
}

/// Generate and return the script for a job without deploying or modifying state.
pub fn show(manifest: &ProjectManifest, job_name: &str) -> Result<String> {
    let job_def = manifest
        .jobs
        .get(job_name)
        .ok_or_else(|| anyhow!("Job \"{job_name}\" not found in manifest"))?;

    crate::codegen::generate_python_script(job_name, job_def)
        .with_context(|| format!("Failed to generate script for job \"{job_name}\""))
}
