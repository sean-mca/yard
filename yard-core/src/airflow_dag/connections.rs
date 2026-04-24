use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::BTreeMap;
use yard_structs::{JobDefinition, ProjectManifest};

use super::helpers::sanitize_identifier;
use super::RequiredConnection;
use super::ResolvedDag;
use super::DEFAULT_AWS_CONN_ID;

/// Derive the Airflow connection id a task should use. Returns
/// `DEFAULT_AWS_CONN_ID` when the task's assume-role matches the project root
/// (same-account case, no per-task override needed) or when no assume-role is
/// set. Otherwise returns a deterministic id derived from the role ARN.
pub(super) fn resolve_task_aws_conn_id(job: &JobDefinition, manifest: &ProjectManifest) -> Result<String> {
    let task_role = job_assume_role(job);
    let root_role = assume_role_of(&manifest.aws);
    match (task_role, root_role) {
        (Some(task), Some(root)) if task == root => Ok(DEFAULT_AWS_CONN_ID.to_string()),
        (Some(task), _) => derive_aws_conn_id(task),
        (None, _) => Ok(DEFAULT_AWS_CONN_ID.to_string()),
    }
}

fn job_assume_role(job: &JobDefinition) -> Option<&str> {
    // `_aws` is the merged view (root + account.yaml + job-inline) produced by
    // `cascade_provider_defaults`; it's authoritative, no fallbacks needed.
    job.config.get("_aws").and_then(assume_role_of)
}

fn assume_role_of(v: &Value) -> Option<&str> {
    v.get("assume_role").and_then(|r| r.as_str()).filter(|s| !s.is_empty())
}

/// Extract the 12-digit AWS account id from an IAM role ARN. Validates the
/// full ARN shape (prefix, account length/digits, `role/` segment, non-empty
/// role name) and returns just the account substring on success.
///
/// Note: this surfaces the yard deploy-credential account (what yard assumes
/// into to upload scripts/DAGs), NOT the Glue execution role — that's
/// `config.role`, threaded into rendered DAGs as `iam_role_name` in Phase 15.
pub(crate) fn parse_account_from_role_arn(role_arn: &str) -> Result<String> {
    let rest = role_arn
        .strip_prefix("arn:aws:iam::")
        .ok_or_else(|| anyhow!("malformed role ARN '{role_arn}': expected 'arn:aws:iam::...'"))?;
    let (account, tail) = rest
        .split_once(':')
        .ok_or_else(|| anyhow!("malformed role ARN '{role_arn}': missing account/resource separator"))?;
    if account.len() != 12 || !account.chars().all(|c| c.is_ascii_digit()) {
        return Err(anyhow!(
            "malformed role ARN '{role_arn}': account id must be 12 digits"
        ));
    }
    let name = tail
        .strip_prefix("role/")
        .ok_or_else(|| anyhow!("malformed role ARN '{role_arn}': expected resource type 'role/'"))?;
    if name.is_empty() {
        return Err(anyhow!("malformed role ARN '{role_arn}': empty role name"));
    }
    Ok(account.to_string())
}

/// Parse a role ARN and produce a stable Airflow connection id of the form
/// `yard_<account>_<role_name_sanitized>`. Returns an error on malformed or
/// non-IAM-role ARNs so invalid config fails at plan/apply rather than at
/// DAG runtime.
pub fn derive_aws_conn_id(role_arn: &str) -> Result<String> {
    let account = parse_account_from_role_arn(role_arn)?;
    // parse_account_from_role_arn already validated the full ARN shape; these
    // strip_prefix calls cannot fail. Using expect() is safe and keeps
    // the error surface owned by parse_account_from_role_arn above.
    let name = role_arn
        .strip_prefix("arn:aws:iam::")
        .and_then(|rest| rest.split_once(':'))
        .and_then(|(_, tail)| tail.strip_prefix("role/"))
        .expect("ARN shape validated by parse_account_from_role_arn");
    let sanitized = sanitize_identifier(name);
    Ok(format!("yard_{account}_{sanitized}"))
}

/// Distinct Airflow connections the DAG's Glue tasks need, in deterministic
/// order. Empty when every task uses `aws_default`.
pub fn required_connections_for_dag(
    manifest: &ProjectManifest,
    dag: &ResolvedDag,
) -> Result<Vec<RequiredConnection>> {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for task_id in &dag.tasks {
        let Some(job) = manifest.jobs.get(task_id) else {
            continue;
        };
        if job.job_type != "glue" {
            continue;
        }
        let conn_id = resolve_task_aws_conn_id(job, manifest)?;
        if conn_id == DEFAULT_AWS_CONN_ID {
            continue;
        }
        if let Some(arn) = job_assume_role(job) {
            seen.entry(conn_id).or_insert_with(|| arn.to_string());
        }
    }
    Ok(seen
        .into_iter()
        .map(|(conn_id, role_arn)| RequiredConnection { conn_id, role_arn })
        .collect())
}
