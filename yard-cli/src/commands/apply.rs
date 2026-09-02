//! Handler for the `yard apply` subcommand.

use super::resolve_project;
use crate::commands::display::print_plan_summary;
use crate::utils::{color_create, color_delete, color_modify, confirm};
use anyhow::Result;
use std::io;
use std::path::Path;

/// Execute `yard apply`: plan changes, prompt for confirmation, then deploy.
///
/// When `dry_run` is `true`, changes are planned and displayed but not
/// applied. When `auto_approve` is `true`, the confirmation prompt is
/// skipped. An optional `target` restricts the operation to a single job;
/// an optional `dir` scopes the operation to all jobs under a directory
/// subtree.
///
/// # Errors
///
/// Returns an error if project resolution, planning, user confirmation
/// I/O, or the apply operation itself fails.
pub async fn execute(
    directory: Option<String>,
    dry_run: bool,
    auto_approve: bool,
    target: Option<String>,
    dir: Option<String>,
) -> Result<()> {
    let project = resolve_project(directory).await?;

    let (manifest, dir_scope) = if let Some(ref dir_path) = dir {
        let filtered =
            yard_core::resolve::filter_manifest_by_dir(&project.manifest, Path::new(dir_path), &project.root_dir)?;
        (filtered.manifest, Some(filtered.display_path))
    } else {
        (project.manifest.clone(), None)
    };

    let result = yard_core::plan(
        &manifest,
        &project.current_state,
        &project.root_dir,
        target.clone(),
    )
    .await?;

    if result.job_diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(());
    }

    print_plan_summary(
        &mut io::stdout().lock(),
        &manifest.project,
        target.as_deref(),
        dir_scope.as_deref(),
        &result.job_diffs,
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
        &manifest,
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

    println!("\nState updated successfully.");

    Ok(())
}
