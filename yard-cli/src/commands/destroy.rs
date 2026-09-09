//! Handler for the `yard destroy` subcommand.

use super::resolve_project;
use crate::utils::{bold, color_delete, confirm};
use anyhow::Result;
use std::path::Path;

/// Execute `yard destroy`: tear down deployed resources and remove state.
///
/// Three mutually exclusive modes: `dir` scopes to all jobs under a
/// directory subtree, `job_name` destroys a single job, `None` destroys
/// all deployed resources. `dry_run` skips provider teardown;
/// `auto_approve` skips the confirmation prompt.
///
/// # Errors
///
/// Returns an error if project resolution, storage access, provider
/// teardown, or user-confirmation I/O fails.
pub async fn execute(
    job_name: Option<String>,
    directory: Option<String>,
    dry_run: bool,
    auto_approve: bool,
    dir: Option<String>,
) -> Result<()> {
    let project = resolve_project(directory).await?;

    if let Some(ref dir_path) = dir {
        let filtered = yard_core::resolve::filter_manifest_by_dir(
            &project.manifest,
            Path::new(dir_path),
            &project.root_dir,
        )?;

        println!(
            "{}",
            bold(&format!(
                "--- Destroy plan for {} ---",
                project.manifest.project
            ))
        );
        println!("(scoped to: {})\n", filtered.display_path);

        let mut names: Vec<&String> = filtered.manifest.jobs.keys().collect();
        names.sort();
        for name in &names {
            println!("{}", color_delete(&format!("  - Destroy job [{}]", name)));
        }

        if dry_run {
            println!("\nDry run -- no changes applied.");
            return Ok(());
        }

        if !auto_approve {
            println!();
            if !confirm("Do you want to destroy these resources? (y/n)")? {
                println!("Destroy cancelled.");
                return Ok(());
            }
        }

        println!("\nDestroying...");

        for name in &names {
            let destroyed = yard_core::destroy_job(
                &project.manifest.state,
                &project.manifest.providers,
                name,
                &project.root_dir,
                dry_run,
            )
            .await?;

            if destroyed {
                println!("{}", color_delete(&format!("  - Destroyed: {}", name)));
            } else {
                println!("No state found for \"{}\".", name);
            }
        }

        return Ok(());
    }

    match job_name {
        Some(name) => {
            println!("{}", bold("--- Destroy plan ---"));
            println!();
            println!("{}", color_delete(&format!("  - Destroy [{}]", name)));

            if dry_run {
                println!("\nDry run -- no changes applied.");
                return Ok(());
            }

            if !auto_approve {
                println!();
                if !confirm("Do you want to destroy this resource? (y/n)")? {
                    println!("Destroy cancelled.");
                    return Ok(());
                }
            }

            println!("\nDestroying...");

            let destroyed_job = yard_core::destroy_job(
                &project.manifest.state,
                &project.manifest.providers,
                &name,
                &project.root_dir,
                dry_run,
            )
            .await?;

            if destroyed_job {
                println!("{}", color_delete(&format!("  - Destroyed: {}", name)));
            } else {
                println!("No state found for \"{}\".", name);
            }
        }
        None => {
            let storage = yard_core::storage::get_storage(&project.manifest.state).await?;
            let job_names = storage.list_jobs().await?;

            if job_names.is_empty() {
                println!("No resources to destroy.");
                return Ok(());
            }

            println!(
                "{}",
                bold(&format!(
                    "--- Destroy plan for {} ---",
                    project.manifest.project
                ))
            );
            println!();
            for name in &job_names {
                println!("{}", color_delete(&format!("  - Destroy job [{}]", name)));
            }

            if dry_run {
                println!("\nDry run -- no changes applied.");
                return Ok(());
            }

            if !auto_approve {
                println!();
                if !confirm("Do you want to destroy all resources? (y/n)")? {
                    println!("Destroy cancelled.");
                    return Ok(());
                }
            }

            println!("\nDestroying...");

            let result = yard_core::destroy_all(
                &project.manifest.state,
                &project.manifest.providers,
                project.manifest.aws.as_ref(),
                &project.root_dir,
                dry_run,
            )
            .await?;

            for name in &result.destroyed {
                println!("{}", color_delete(&format!("  - Destroyed: {}", name)));
            }

            println!("\nAll resources destroyed.");
        }
    }

    Ok(())
}
