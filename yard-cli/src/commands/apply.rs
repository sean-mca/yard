use super::resolve_project;
use crate::utils::{bold, color_create, color_delete, color_modify, confirm};
use anyhow::Result;

pub async fn execute(
    directory: Option<String>,
    dry_run: bool,
    auto_approve: bool,
    target: Option<String>,
) -> Result<()> {
    let project = resolve_project(directory).await?;

    let mut diffs = yard_core::calculate_diff(&project.manifest, &project.current_state)?;

    if let Some(ref name) = target {
        diffs.retain(|d| &d.name == name);
    }

    // Also compute DAG diffs for the plan display
    let dags = yard_core::airflow_dag::collect_dags(&project.root_dir, &project.manifest)?;
    let dag_state = yard_core::load_dag_state(&project.manifest.state).await?;
    let mut dag_diffs = yard_core::calculate_dag_diffs(&project.manifest, &dags, &dag_state)?;

    if let Some(ref name) = target {
        dag_diffs.retain(|d| &d.name == name);
    }

    if diffs.is_empty() && dag_diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(());
    }

    // Show the plan
    println!(
        "{}",
        bold(&format!("--- Plan for {} ---", project.manifest.project))
    );
    if let Some(ref name) = target {
        println!("(targeting: {})\n", name);
    } else {
        println!();
    }

    for diff in &diffs {
        match &diff.diff_type {
            yard_structs::DiffType::Create => {
                println!(
                    "{}",
                    color_create(&format!("  + Create job [{}]", diff.name))
                );
            }
            yard_structs::DiffType::Modify { changes } => {
                println!(
                    "{}",
                    color_modify(&format!("  ~ Modify job [{}]", diff.name))
                );
                for (key, (old, new)) in changes {
                    println!("      {} : {} -> {}", key, old, new);
                }
            }
            yard_structs::DiffType::Delete => {
                println!(
                    "{}",
                    color_delete(&format!("  - Delete job [{}]", diff.name))
                );
            }
        }
    }

    for diff in &dag_diffs {
        match &diff.diff_type {
            yard_structs::DiffType::Create => {
                println!(
                    "{}",
                    color_create(&format!("  + Create DAG [{}]", diff.name))
                );
            }
            yard_structs::DiffType::Modify { changes } => {
                println!(
                    "{}",
                    color_modify(&format!("  ~ Modify DAG [{}]", diff.name))
                );
                for (key, (old, new)) in changes {
                    println!("      {} : {} -> {}", key, old, new);
                }
            }
            yard_structs::DiffType::Delete => {
                println!(
                    "{}",
                    color_delete(&format!("  - Delete DAG [{}]", diff.name))
                );
            }
        }
    }

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
