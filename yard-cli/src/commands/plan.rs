use super::resolve_project;
use crate::commands::display::print_plan_summary;
use anyhow::Result;
use std::io;

pub async fn execute(directory: Option<String>, target: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    let result = yard_core::plan(
        &project.manifest,
        &project.current_state,
        &project.root_dir,
        target.clone(),
    )
    .await?;

    if result.job_diffs.is_empty() && result.dag_diffs.is_empty() {
        println!("No changes. Infrastructure is up to date.");
        return Ok(());
    }

    print_plan_summary(
        &mut io::stdout().lock(),
        &project.manifest.project,
        target.as_deref(),
        &result.job_diffs,
        &result.dag_diffs,
    )?;

    Ok(())
}
