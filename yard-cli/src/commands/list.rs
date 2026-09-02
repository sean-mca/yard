//! Handler for the `yard list targets` subcommand.

use std::collections::HashSet;

use super::resolve_project;
use anyhow::{Context, Result};
use yard_structs::DiffType;

/// Emit deployment targets (jobs) that have pending changes as a
/// pretty-printed JSON array to stdout. Consumed by CI matrix builders
/// that fan out `yard apply --target` with per-account OIDC roles.
///
/// Only targets with a Create, Modify, or Delete diff are included --
/// unchanged targets are omitted to avoid overwhelming CI matrices.
/// Delete diffs (targets removed from the manifest but still in state) are
/// included so CI can fan out `yard apply --target` for the teardown.
///
/// # Errors
///
/// Returns an error if project resolution, diff computation, or JSON
/// serialisation encounters an error.
pub async fn execute(directory: Option<String>, _json: bool) -> Result<()> {
    let project = resolve_project(directory).await?;
    let rows = yard_core::list_targets(&project.manifest, &project.root_dir)?;

    let plan_result = yard_core::plan(
        &project.manifest,
        &project.current_state,
        &project.root_dir,
        None,
    )
    .await?;

    let changed: HashSet<&str> = plan_result
        .job_diffs
        .iter()
        .map(|d| d.name.as_str())
        .collect();

    let mut filtered: Vec<_> = rows
        .into_iter()
        .filter(|r| changed.contains(r.target.as_str()))
        .collect();

    for diff in &plan_result.job_diffs {
        if !matches!(diff.diff_type, DiffType::Delete) {
            continue;
        }
        let account = project
            .current_state
            .deployments
            .get(&diff.name)
            .and_then(|d| d.config.get("_aws"))
            .and_then(|aws| aws.get("assume_role"))
            .and_then(|r| r.as_str())
            .filter(|s| !s.is_empty())
            .map(yard_core::parse_account_from_role_arn)
            .transpose()
            .with_context(|| format!("target '{}'", diff.name))?;
        filtered.push(yard_core::TargetRow {
            target: diff.name.clone(),
            kind: "job",
            aws_account_id: account,
        });
    }

    filtered.sort_by(|a, b| a.target.cmp(&b.target));
    filtered.dedup_by(|a, b| a.target == b.target);

    let out = serde_json::to_string_pretty(&filtered)?;
    println!("{out}");
    Ok(())
}
