//! Handler for the `yard show` subcommand.

use super::resolve_project;
use anyhow::Result;

/// Execute `yard show <name>`: print the generated script for a job.
///
/// Uses plugin codegen to generate the script. The generated script is
/// written to stdout.
///
/// # Errors
///
/// Returns an error if project resolution fails or if `name` does not match
/// a job in the manifest.
pub async fn execute(name: String, directory: Option<String>) -> Result<()> {
    let project = resolve_project(directory).await?;

    let plugin_host_config = yard_core::plugin_host::PluginHostConfig {
        plugins_dir: project.root_dir.join(".yard/plugins"),
        lock_file_path: Some(project.root_dir.join("yard.lock")),
        ..Default::default()
    };

    let script = yard_core::show(&project.manifest, &name, &plugin_host_config).await?;
    print!("{script}");
    Ok(())
}
