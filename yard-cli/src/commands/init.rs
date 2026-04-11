use super::resolve_project;
use anyhow::Result;

pub async fn execute(directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;
    yard_core::init(&project.manifest).await?;
    Ok(())
}
