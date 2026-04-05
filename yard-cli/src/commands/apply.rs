use super::resolve_project;
use crate::utils::confirm;
use anyhow::Result;

pub async fn execute(directory: Option<String>, dry_run: bool, auto_approve: bool) -> Result<()> {
    let project = resolve_project(directory).await?;

    let diffs = yard_core::calculate_diff(&project.manifest, &project.current_state);
    if diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(());
    }

    // Show the plan
    println!("--- Plan for {} ---\n", project.manifest.project);
    for diff in &diffs {
        match &diff.diff_type {
            yard_structs::DiffType::Create => {
                println!("  + Create job [{}]", diff.name);
            }
            yard_structs::DiffType::Modify { changes } => {
                println!("  ~ Modify job [{}]", diff.name);
                for (key, (old, new)) in changes {
                    println!("      {} : {} -> {}", key, old, new);
                }
            }
            yard_structs::DiffType::Delete => {
                println!("  - Delete job [{}]", diff.name);
            }
        }
    }

    if dry_run {
        println!("\nDry run -- no changes applied.");
        return Ok(());
    }

    // Confirm
    if !auto_approve {
        println!();
        if !confirm("Do you want to apply these changes? (y/n)") {
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
    )
    .await?;

    for name in &result.created {
        println!("  + Created: {}", name);
    }
    for name in &result.modified {
        println!("  ~ Modified: {}", name);
    }
    for name in &result.deleted {
        println!("  - Deleted: {}", name);
    }

    println!("\nState updated successfully.");

    Ok(())
}
