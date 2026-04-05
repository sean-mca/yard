use super::resolve_project;
use anyhow::Result;

pub async fn execute(job_name: Option<String>, directory: Option<String>, dry_run: bool) -> Result<()> {
    let project = resolve_project(directory).await?;

    match job_name {
        Some(name) => {
            if dry_run {
                println!("Would destroy job \"{}\" (dry run).", name);
            }

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
            if dry_run {
                println!(
                    "Would destroy all jobs in {} (dry run).",
                    project.manifest.project
                );
            } else {
                println!("Destroying all jobs in {}...", project.manifest.project);
            }

            let result = yard_core::destroy_all(
                &project.manifest.state,
                &project.manifest.providers,
                &project.root_dir,
                dry_run,
            )
            .await?;

            if result.destroyed.is_empty() {
                println!("No jobs to destroy.");
            } else {
                for name in &result.destroyed {
                    println!("  - Destroyed: {}", name);
                }
                println!("\nAll jobs destroyed.");
            }
        }
    }

    Ok(())
}
