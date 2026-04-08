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

    if diffs.is_empty() {
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

    if dry_run {
        println!("\nDry run -- no changes applied.");
        return Ok(());
    }

    if !auto_approve {
        println!();
        if !confirm("Do you want to apply these changes? (y/n)") {
            println!("Apply cancelled.");
            return Ok(());
        }
    }

    println!("\nApplying...");

    // When targeting a single job, filter the manifest
    let manifest = if let Some(ref name) = target {
        let mut filtered = project.manifest.clone();
        filtered.jobs.retain(|k, _| k == name);
        filtered
    } else {
        project.manifest.clone()
    };

    let result = yard_core::apply(
        &manifest,
        &project.current_state,
        &project.root_dir,
        dry_run,
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

    println!("\nState updated successfully.");

    Ok(())
}
