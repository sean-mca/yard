//! Handler for the `yard validate` subcommand.

use super::resolve_project;
use anyhow::{Result, bail};
use std::path::Path;

/// Execute `yard validate`: check job configurations for errors.
///
/// When `target` is `Some`, validates only that single job. When `dir` is
/// `Some`, validates only jobs under that directory subtree. When both are
/// `None`, validates all jobs. Each job gets yard's structural checks plus
/// its plugin's schema and `validate` operation, unless `skip_plugins` is
/// set. Prints `[PASS]` or `[FAIL]` per job and exits with an error if any
/// job fails validation.
///
/// # Errors
///
/// Returns an error if project resolution fails, the target job is not
/// found, or one or more jobs have validation errors.
pub async fn execute(
    directory: Option<String>,
    target: Option<String>,
    dir: Option<String>,
    skip_plugins: bool,
) -> Result<()> {
    let project = resolve_project(directory).await?;

    let manifest = if let Some(ref dir_path) = dir {
        let filtered = yard_core::resolve::filter_manifest_by_dir(
            &project.manifest,
            Path::new(dir_path),
            &project.root_dir,
        )?;
        println!(
            "Validating project: {} (scoped to: {})\n",
            project.manifest.project, filtered.display_path
        );
        filtered.manifest
    } else if let Some(ref name) = target {
        if !project.manifest.jobs.contains_key(name) {
            bail!("target '{}' not found -- no job with that name", name);
        }
        println!(
            "Validating project: {} (targeting: {})\n",
            project.manifest.project, name
        );
        project.manifest.clone()
    } else {
        println!("Validating project: {}\n", project.manifest.project);
        project.manifest.clone()
    };

    let mut total_pass = 0;
    let mut total_fail = 0;

    let mut job_names: Vec<&str> = if let Some(ref name) = target {
        vec![name.as_str()]
    } else {
        manifest.jobs.keys().map(String::as_str).collect()
    };
    job_names.sort();

    let results =
        yard_core::validate_jobs(&manifest, &job_names, &project.root_dir, !skip_plugins).await?;

    for result in &results {
        if result.errors.is_empty() {
            println!("[PASS] {}.yaml", result.name);
            total_pass += 1;
        } else {
            println!("[FAIL] {}.yaml", result.name);
            for error in &result.errors {
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
