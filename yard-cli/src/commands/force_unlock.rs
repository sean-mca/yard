use super::resolve_project;
use anyhow::Result;

pub async fn execute(job_name: String, directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    match yard_core::force_unlock(&project.manifest.state, &job_name).await? {
        Some(info) => {
            println!(
                "Removing lock on job \"{}\" (held by {} since {})",
                job_name, info.who, info.created_at
            );
            println!("Lock removed.");
        }
        None => {
            println!("Job \"{}\" is not locked.", job_name);
        }
    }

    Ok(())
}
