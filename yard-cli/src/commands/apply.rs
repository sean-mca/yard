use super::resolve_project;
use crate::commands::display::print_plan_summary;
use crate::utils::{color_create, color_delete, color_modify, confirm};
use anyhow::Result;
use std::io;

pub async fn execute(
    directory: Option<String>,
    dry_run: bool,
    auto_approve: bool,
    target: Option<String>,
) -> Result<()> {
    let project = resolve_project(directory).await?;

    let result = yard_core::plan(
        &project.manifest,
        &project.current_state,
        &project.root_dir,
        target.clone(),
    )
    .await?;

    if result.job_diffs.is_empty() && result.dag_diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(());
    }

    print_plan_summary(
        &mut io::stdout().lock(),
        &project.manifest.project,
        target.as_deref(),
        &result.job_diffs,
        &result.dag_diffs,
    )?;

    if dry_run {
        println!("\nDry run -- no changes applied.");
        return Ok(());
    }

    if !auto_approve {
        println!();
        if !confirm("Do you want to apply these changes? (y/n)")? {
            println!("Apply cancelled.");
            return Ok(());
        }
    }

    println!("\nApplying...");

    let result = yard_core::apply(
        &project.manifest,
        &project.current_state,
        &project.root_dir,
        dry_run,
        target,
    )
    .await?;

    for name in &result.created {
        println!("{}", color_create(&format!("  + Created: {}", name)));
    }
    for name in &result.modified {
        println!("{}", color_modify(&format!("  ~ Modified: {}", name)));
    }
    for name in &result.deleted {
        println!("{}", color_delete(&format!("  - Deleted: {}", name)));
    }
    for name in &result.dag_created {
        println!("{}", color_create(&format!("  + Created DAG: {}", name)));
    }
    for name in &result.dag_modified {
        println!("{}", color_modify(&format!("  ~ Modified DAG: {}", name)));
    }
    for name in &result.dag_deleted {
        println!("{}", color_delete(&format!("  - Deleted DAG: {}", name)));
    }

    if !result.dag_required_connections.is_empty() {
        println!("\nRequired Airflow connections (create in MWAA before the DAG runs):");
        for rc in &result.dag_required_connections {
            println!("  - {}  ->  {}", rc.conn_id, rc.role_arn);
        }
    }

    println!("\nState updated successfully.");

    Ok(())
}
