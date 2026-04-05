use super::resolve_project;
use crate::utils::confirm;
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
            println!("--- Destroy plan ---\n");
            println!("  - Destroy job [{}]", name);

            if dry_run {
                println!("\nDry run -- no changes applied.");
                return Ok(());
            }

            if !auto_approve {
                println!();
                if !confirm("Do you want to destroy this job? (y/n)") {
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
                println!("  - Destroyed: {}", name);
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

            println!("--- Destroy plan for {} ---\n", project.manifest.project);
            for name in &job_names {
                println!("  - Destroy job [{}]", name);
            }

            if dry_run {
                println!("\nDry run -- no changes applied.");
                return Ok(());
            }

            if !auto_approve {
                println!();
                if !confirm("Do you want to destroy all jobs? (y/n)") {
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
                println!("  - Destroyed: {}", name);
            }

            println!("\nAll jobs destroyed.");
        }
    }

    Ok(())
}
