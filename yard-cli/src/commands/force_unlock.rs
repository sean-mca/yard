//! Handler for the `yard force-unlock` subcommand.

use super::resolve_project;
use anyhow::Result;

/// Execute `yard force-unlock <job_name>`: remove a stale lock from a job.
///
/// If the job is currently locked, prints lock details (holder, timestamp)
/// and removes the lock. If the job is not locked, prints a no-op message.
///
/// # Errors
///
/// Returns an error if project resolution or the storage unlock
/// operation fails.
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
