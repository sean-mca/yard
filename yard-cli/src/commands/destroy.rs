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

            // Try as a job first, then as a DAG
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
                let destroyed_dag = yard_core::destroy_dag(
                    &project.manifest.state,
                    &project.manifest.providers,
                    project.manifest.aws.as_ref(),
                    &name,
                    &project.root_dir,
                    dry_run,
                )
                .await?;

                if destroyed_dag {
                    println!("{}", color_delete(&format!("  - Destroyed DAG: {}", name)));
                } else {
                    println!("No state found for \"{}\".", name);
                }
            }
        }
        None => {
            let storage = yard_core::storage::get_storage(&project.manifest.state).await?;
            let job_names = storage.list_jobs().await?;
            let dag_names = storage.list_dags().await?;

            if job_names.is_empty() && dag_names.is_empty() {
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
            for name in &dag_names {
                println!("{}", color_delete(&format!("  - Destroy DAG [{}]", name)));
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
            for name in &result.dags_destroyed {
                println!("{}", color_delete(&format!("  - Destroyed DAG: {}", name)));
            }

            println!("\nAll resources destroyed.");
        }
    }

    Ok(())
}
