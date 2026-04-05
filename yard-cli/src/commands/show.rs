use super::resolve_project;
use anyhow::Result;

pub async fn execute(job_name: String, directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;
    let script = yard_core::show(&project.manifest, &job_name)?;
    print!("{script}");
    Ok(())
}
