use super::resolve_project;
use crate::utils::{bold, color_delete, confirm};
use anyhow::Result;

pub async fn execute(
    job_name: Option<String>,
    directory: Option<String>,
    dry_run: bool,
    auto_approve: bool,
) -> Result<()> {
    let project = resolve_project(directory).await?;

    match job_name {
        Some(name) => {
            println!("{}", bold("--- Destroy plan ---"));
            println!();
            println!("{}", color_delete(&format!("  - Destroy job [{}]", name)));

            if dry_run {
                println!("\nDry run -- no changes applied.");
                return Ok(());
            }

            if !auto_approve {
                println!();
                if !confirm("Do you want to destroy this job? (y/n)")? {
                    println!("Destroy cancelled.");
                    return Ok(());
                }
            }

            println!("\nDestroying...");

            let destroyed = yard_core::destroy_job(
                &project.manifest.state,
                &project.manifest.providers,
                &name,
                &project.root_dir,
                dry_run,
            )
            .await?;

            if destroyed {
                println!("{}", color_delete(&format!("  - Destroyed: {}", name)));
            } else {
                println!("No state found for job \"{}\".", name);
            }
        }
        None => {
            let storage = yard_core::storage::get_storage(&project.manifest.state).await?;
            let job_names = storage.list_jobs().await?;

            if job_names.is_empty() {
                println!("No jobs to destroy.");
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
                if !confirm("Do you want to destroy all jobs? (y/n)")? {
                    println!("Destroy cancelled.");
                    return Ok(());
                }
            }

            println!("\nDestroying...");

            let result = yard_core::destroy_all(
                &project.manifest.state,
                &project.manifest.providers,
                &project.root_dir,
                dry_run,
            )
            .await?;

            for name in &result.destroyed {
                println!("{}", color_delete(&format!("  - Destroyed: {}", name)));
            }

            println!("\nAll jobs destroyed.");
        }
    }

    Ok(())
}
