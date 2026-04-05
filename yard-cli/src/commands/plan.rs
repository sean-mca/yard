use super::resolve_project;
use crate::utils::{bold, color_create, color_delete, color_modify};
use anyhow::Result;

pub async fn execute(directory: Option<String>, target: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    let mut diffs = yard_core::calculate_diff(&project.manifest, &project.current_state);

    if let Some(ref name) = target {
        diffs.retain(|d| &d.name == name);
    }

    println!(
        "{}",
        bold(&format!("--- Plan for {} ---", project.manifest.project))
    );

    if let Some(ref name) = target {
        println!("(targeting: {})\n", name);
    } else {
        println!();
    }

    if diffs.is_empty() {
        println!("No changes. Infrastructure is up to date.");
        return Ok(());
    }

    for diff in diffs {
        match diff.diff_type {
            yard_structs::DiffType::Create => {
                println!(
                    "{}",
                    color_create(&format!("  + Create job [{}]", diff.name))
                );
            }
            yard_structs::DiffType::Modify { ref changes } => {
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

    Ok(())
}
