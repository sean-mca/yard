use super::resolve_project;
use anyhow::Result;

pub async fn execute(directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    let diffs = yard_core::calculate_diff(&project.manifest, &project.current_state);
    if diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(());
    }

    println!("Applying changes for {}...", project.manifest.project);

    let result = yard_core::apply(&project.manifest, &project.current_state, &project.root_dir).await?;

    for name in &result.created {
        println!("  + Created: .yard/generated/{}.py", name);
    }
    for name in &result.modified {
        println!("  ~ Modified: .yard/generated/{}.py", name);
    }
    for name in &result.deleted {
        println!("  - Deleted: {}", name);
    }

    println!("\nState updated successfully.");

    Ok(())
}
