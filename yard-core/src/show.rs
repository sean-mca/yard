//! Show command implementation for previewing generated artifacts.
//!
//! Provides read-only generation of job scripts without deploying or
//! modifying state. Used by `yard show <name>` CLI command.

use anyhow::{anyhow, Context, Result};
use yard_structs::ProjectManifest;

use crate::plugin_host::PluginHostConfig;
use crate::providers;

/// Generate and return the script for a job without deploying or modifying state.
///
/// Uses plugin codegen (via [`providers::get_provider_for_job`]) to generate
/// the script content. Returns the generated script or an error if the job
/// is not found or plugin codegen fails.
///
/// # Errors
///
/// Returns an error if the job name is not found in the manifest, if the
/// plugin provider cannot be constructed, or if codegen fails.
pub async fn show(
    manifest: &ProjectManifest,
    job_name: &str,
    plugin_host_config: &PluginHostConfig,
) -> Result<String> {
    let job_def = manifest
        .jobs
        .get(job_name)
        .ok_or_else(|| anyhow!("Job \"{job_name}\" not found in manifest"))?;

    let provider = providers::get_provider_for_job(
        &job_def.job_type,
        &job_def.config,
        job_def.plugin_version.as_deref(),
        job_def.plugin_source.as_deref(),
        plugin_host_config,
    )
    .await
    .with_context(|| format!("Failed to get provider for job \"{job_name}\""))?;

    let script = provider
        .codegen(job_name, &job_def.config)
        .await
        .with_context(|| format!("Failed to generate script for job \"{job_name}\""))?;

    Ok(script.unwrap_or_default())
}
