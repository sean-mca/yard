//! Handler for the `yard plan` subcommand.

use super::resolve_project;
use crate::commands::display::print_plan_summary;
use anyhow::Result;
use std::io;
use std::path::Path;

/// Execute `yard plan`: preview infrastructure changes without applying them.
///
/// Resolves the project, computes diffs between desired and current state,
/// and prints a human-readable plan summary. An optional `target`
/// restricts the diff to a single job; an optional `dir` scopes the diff
/// to all jobs under a directory subtree.
///
/// # Errors
///
/// Returns an error if project resolution, state loading, or diff
/// computation fails.
pub async fn execute(
    directory: Option<String>,
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

    if result.job_diffs.is_empty() && result.dag_diffs.is_empty() {
        println!("No changes. Infrastructure is up to date.");
        return Ok(());
    }

    print_plan_summary(
        &mut io::stdout().lock(),
        &manifest.project,
        target.as_deref(),
        dir_scope.as_deref(),
        &result.job_diffs,
        &result.dag_diffs,
    )?;

    Ok(())
}
