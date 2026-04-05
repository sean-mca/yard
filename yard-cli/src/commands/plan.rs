use super::resolve_project;
use anyhow::Result;
use yard_structs::YardAction;

pub async fn execute(directory: Option<String>) -> Result<Option<YardAction>> {
    let project = resolve_project(directory).await?;

    let diffs = yard_core::calculate_diff(&project.manifest, &project.current_state);

    println!("--- Plan for {} ---", project.manifest.project);

    if diffs.is_empty() {
        println!("No changes. Infrastructure is up to date.");
    }

    for diff in diffs {
        match diff.diff_type {
            yard_structs::DiffType::Create => {
                println!(
                    "+ Create job [{}] ({})",
                    diff.name,
                    diff.new_hash.as_ref().unwrap_or(&"???".to_string())
                );
            }
            yard_structs::DiffType::Modify { ref changes } => {
                println!("~ Modify job [{}]", diff.name);
                for (key, (old, new)) in changes {
                    println!("    {} : {} -> {}", key, old, new);
                }
            }
            yard_structs::DiffType::Delete => {
                println!("- Delete job [{}]", diff.name);
            }
            _ => {}
        }
    }

    Ok(Some(YardAction::Plan {
        manifest: project.manifest,
    }))
}
