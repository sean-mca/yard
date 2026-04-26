//! Enumerate deployment targets (jobs + DAGs) with their resolved AWS
//! deploy-credential account ids. Powers `yard list targets` (Phase 19) —
//! CI/CD matrix builders fan out `apply --target` with per-account OIDC
//! roles using the emitted `{target, kind, aws_account_id}` rows.
//!
//! Enumeration is manifest-driven (D-01): jobs come from `manifest.jobs`,
//! DAGs come from `airflow_dag::collect_dags`. State files are NOT
//! consulted — un-applied targets appear in the output.
//!
//! `aws_account_id` is the yard deploy-credential account (what yard
//! assumes into to upload scripts/DAGs), NOT the Glue execution role —
//! that's `config.role`, threaded into rendered DAGs as `iam_role_name`
//! in Phase 15.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::path::Path;
use yard_structs::{AwsCredentialConfig, ProjectManifest};

use crate::airflow_dag;
use crate::airflow_dag::parse_account_from_role_arn;
use crate::dag_lifecycle;

/// One deployment target (job or DAG) with its resolved AWS account id.
///
/// Schema is locked for v1.4 — adding fields is a future breaking change
/// for CI consumers. `aws_account_id` is `Some(12-digit-string)` when the
/// target has an `assume_role` ARN set; `None` when the target uses the
/// default credential chain (local dev, no cross-account role).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TargetRow {
    pub target: String,
    pub kind: &'static str,
    pub aws_account_id: Option<String>,
}

/// Enumerate every deployment target in the resolved project and return
/// their account ids, sorted alphabetically by target name (D-08).
///
/// Returns `Err` only when a target has an `assume_role` ARN that fails
/// shape validation (D-10); a missing/empty `assume_role` is NOT an error
/// (D-04) — it emits `aws_account_id: None`.
pub fn list_targets(
    manifest: &ProjectManifest,
    root_dir: &Path,
) -> Result<Vec<TargetRow>> {
    let mut rows: Vec<TargetRow> = Vec::new();

    // --- Jobs (D-02: read pre-merged _aws.assume_role) ---
    for (name, job) in &manifest.jobs {
        let account = assume_role_str(job.config.get("_aws"))
            .map(parse_account_from_role_arn)
            .transpose()
            .with_context(|| format!("target '{name}'"))?;
        rows.push(TargetRow {
            target: name.clone(),
            kind: "job",
            aws_account_id: account,
        });
    }

    // --- DAGs (D-02: call authoritative resolve_effective_dag_aws) ---
    let dags = airflow_dag::collect_dags(root_dir, manifest)?;
    for dag in &dags {
        let effective = dag_lifecycle::resolve_effective_dag_aws(manifest, dag);
        let account = typed_assume_role_str(effective.as_ref())
            .map(parse_account_from_role_arn)
            .transpose()
            .with_context(|| format!("target '{}'", dag.name))?;
        rows.push(TargetRow {
            target: dag.name.clone(),
            kind: "dag",
            aws_account_id: account,
        });
    }

    // Deterministic order across runs — CI matrix diffs stay stable (D-08).
    rows.sort_by(|a, b| a.target.cmp(&b.target));
    Ok(rows)
}

/// Extract a non-empty `assume_role` string from an aws block Value.
/// Used for the per-job `_aws` override which still lives inside
/// `JobDefinition.config: Value` (D-09 / D-14). Returns None when the input
/// is None, not a JSON object, has no `assume_role` key, has a non-string
/// `assume_role`, or has an empty-string `assume_role`. Matches
/// `airflow_dag::connections::value_assume_role`'s behavior (D-04).
fn assume_role_str(aws: Option<&Value>) -> Option<&str> {
    aws?.get("assume_role")
        .and_then(|r| r.as_str())
        .filter(|s| !s.is_empty())
}

/// Extract a non-empty `assume_role` string from a typed `AwsCredentialConfig`.
/// Used for the typed manifest-level cascade (D-04 / TYPE-02). Returns None
/// when the credentials are None or `assume_role` is unset / empty.
fn typed_assume_role_str(creds: Option<&AwsCredentialConfig>) -> Option<&str> {
    creds?.assume_role.as_deref().filter(|s| !s.is_empty())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use yard_structs::{JobDefinition, JobType, StateBackend};

    fn make_manifest(
        jobs: Vec<(&str, Value)>,
        root_aws: Option<AwsCredentialConfig>,
    ) -> ProjectManifest {
        let mut job_map: HashMap<String, JobDefinition> = HashMap::new();
        for (name, config) in jobs {
            job_map.insert(
                name.to_string(),
                JobDefinition {
                    job_type: JobType::Glue,
                    config,
                    ..Default::default()
                },
            );
        }
        ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: job_map,
            aws: root_aws,
        }
    }

    fn empty_root() -> std::path::PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir()
            .join(format!("yard_list_targets_test_{pid}_{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn empty_manifest_returns_empty_vec() {
        let manifest = make_manifest(vec![], None);
        let root = empty_root();
        let rows = list_targets(&manifest, &root).unwrap();
        assert!(rows.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn job_with_no_aws_emits_none() {
        let manifest = make_manifest(
            vec![("j1", json!({"type": "glue"}))],
            None,
        );
        let root = empty_root();
        let rows = list_targets(&manifest, &root).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].target, "j1");
        assert_eq!(rows[0].kind, "job");
        assert_eq!(rows[0].aws_account_id, None);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn job_with_assume_role_emits_account_id() {
        let manifest = make_manifest(
            vec![(
                "deployer_job",
                json!({
                    "type": "glue",
                    "_aws": { "assume_role": "arn:aws:iam::123456789012:role/Deployer" }
                }),
            )],
            None,
        );
        let root = empty_root();
        let rows = list_targets(&manifest, &root).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].aws_account_id.as_deref(), Some("123456789012"));
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn job_with_empty_assume_role_emits_none() {
        let manifest = make_manifest(
            vec![(
                "j1",
                json!({
                    "type": "glue",
                    "_aws": { "assume_role": "" }
                }),
            )],
            None,
        );
        let root = empty_root();
        let rows = list_targets(&manifest, &root).unwrap();
        assert_eq!(rows[0].aws_account_id, None);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn malformed_arn_is_wrapped_with_target_context() {
        let manifest = make_manifest(
            vec![(
                "bad_job",
                json!({
                    "type": "glue",
                    "_aws": { "assume_role": "not-an-arn" }
                }),
            )],
            None,
        );
        let root = empty_root();
        let err = list_targets(&manifest, &root).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("target 'bad_job'"),
            "expected 'target \\'bad_job\\'' in error, got: {msg}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn rows_are_sorted_alphabetically() {
        let manifest = make_manifest(
            vec![
                ("zeta", json!({})),
                ("alpha", json!({})),
                ("mid", json!({})),
            ],
            None,
        );
        let root = empty_root();
        let rows = list_targets(&manifest, &root).unwrap();
        let names: Vec<&str> = rows.iter().map(|r| r.target.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn kind_is_literal_job_or_dag() {
        let manifest = make_manifest(vec![("j1", json!({}))], None);
        let root = empty_root();
        let rows = list_targets(&manifest, &root).unwrap();
        assert_eq!(rows[0].kind, "job");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn serialized_shape_is_snake_case_with_null_for_none() {
        let row = TargetRow {
            target: "etl".to_string(),
            kind: "job",
            aws_account_id: None,
        };
        let v = serde_json::to_value(&row).unwrap();
        assert_eq!(v["target"], json!("etl"));
        assert_eq!(v["kind"], json!("job"));
        assert_eq!(v["aws_account_id"], Value::Null);
        let obj = v.as_object().unwrap();
        let mut keys: Vec<&String> = obj.keys().collect();
        keys.sort();
        assert_eq!(keys, vec!["aws_account_id", "kind", "target"]);
    }
}
