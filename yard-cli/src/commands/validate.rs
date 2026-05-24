//! Handler for the `yard validate` subcommand.

use super::resolve_project;
use anyhow::{Result, bail};

/// Execute `yard validate`: check all job configurations for errors.
///
/// Iterates over every job in the manifest, runs the full validation
/// suite, and prints `[PASS]` or `[FAIL]` per job. Exits with an error
/// if any job fails validation.
///
/// # Errors
///
/// Returns an error if project resolution fails or if one or more jobs
/// have validation errors.
pub async fn execute(directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    println!("Validating project: {}\n", project.manifest.project);

    let mut total_pass = 0;
    let mut total_fail = 0;

    let mut job_names: Vec<&String> = project.manifest.jobs.keys().collect();
    job_names.sort();

    for name in job_names {
        let job_def = &project.manifest.jobs[name];
        let errors = yard_core::validation::validate_job_full(name, job_def);

        if errors.is_empty() {
            println!("[PASS] {}.yaml", name);
            total_pass += 1;
        } else {
            println!("[FAIL] {}.yaml", name);
            for error in &errors {
                println!("  - {}", error);
            }
            total_fail += 1;
        }
    }

    println!(
        "\nValidation complete: {} passed, {} failed",
        total_pass, total_fail
    );

    if total_fail > 0 {
        bail!("Validation failed: {total_fail} job(s) had errors");
    }

    Ok(())
}
