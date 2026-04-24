use super::resolve_project;
use anyhow::Result;

/// Emit deployment targets (jobs + DAGs) as a pretty-printed JSON array to
/// stdout. Consumed by CI matrix builders (v1.4 GitHub Action reference
/// workflows) that fan out `yard apply --target` with per-account OIDC roles.
///
/// The `_json` flag is accepted for forward-compatibility (D-06) — JSON is
/// the only output mode in v1.4; a future phase may add a human-readable
/// default and then `--json` would become meaningful.
pub async fn execute(directory: Option<String>, _json: bool) -> Result<()> {
    let project = resolve_project(directory).await?;
    let rows = yard_core::list_targets(&project.manifest, &project.root_dir)?;
    let out = serde_json::to_string_pretty(&rows)?;
    println!("{out}");
    Ok(())
}
