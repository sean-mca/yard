use anyhow::{Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use yard_structs::{DagDeployment, DagDiff, DagState, DiffType, ProjectManifest};

use crate::airflow_dag;
use crate::providers;
use crate::resolve;
use crate::storage;
use crate::utils;

use crate::config_merge::merge_provider_config;
use crate::parsing::parse_airflow_section;

/// Result of applying DAG changes.
#[derive(Debug)]
pub struct DagApplyResult {
    pub created: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
    pub required_connections: Vec<airflow_dag::RequiredConnection>,
}

/// Load the current DAG deployment state from the state backend.
pub async fn load_dag_state(
    backend: &yard_structs::StateBackend,
) -> Result<HashMap<String, DagDeployment>> {
    let storage = storage::get_storage(backend).await?;
    let dag_names = storage.list_dags().await?;
    let mut deployments = HashMap::new();
    for name in dag_names {
        if let Some(state) = storage.read_dag(&name).await? {
            deployments.insert(name, state.deployment);
        }
    }
    Ok(deployments)
}

/// Compute the diff between resolved DAGs and stored DAG state.
pub fn calculate_dag_diffs(
    manifest: &ProjectManifest,
    dags: &[airflow_dag::ResolvedDag],
    dag_deployments: &HashMap<String, DagDeployment>,
) -> Result<Vec<DagDiff>> {
    let mut diffs = Vec::new();

    for dag in dags {
        let content = airflow_dag::generate_dag(manifest, dag)?;
        let current_hash = utils::calculate_hash(&content);

        if let Some(existing) = dag_deployments.get(&dag.name) {
            if existing.content_hash != current_hash {
                let changes = compare_dag_config(existing, dag);
                diffs.push(DagDiff {
                    name: dag.name.clone(),
                    diff_type: DiffType::Modify { changes },
                    old_hash: Some(existing.content_hash.clone()),
                    new_hash: Some(current_hash),
                });
            }
        } else {
            diffs.push(DagDiff {
                name: dag.name.clone(),
                diff_type: DiffType::Create,
                old_hash: None,
                new_hash: Some(current_hash),
            });
        }
    }

    // DAGs in state but no longer in the filesystem
    for name in dag_deployments.keys() {
        if !dags.iter().any(|d| &d.name == name) {
            diffs.push(DagDiff {
                name: name.clone(),
                diff_type: DiffType::Delete,
                old_hash: dag_deployments.get(name).map(|d| d.content_hash.clone()),
                new_hash: None,
            });
        }
    }

    Ok(diffs)
}

/// Compare old DAG deployment config with new resolved DAG to produce change map.
fn compare_dag_config(
    old: &DagDeployment,
    new: &airflow_dag::ResolvedDag,
) -> HashMap<String, (String, String)> {
    let mut changes = HashMap::new();
    let new_config = serde_json::to_value(&new.config).unwrap_or_default();
    if let (Value::Object(old_obj), Value::Object(new_obj)) = (&old.config, &new_config) {
        for (k, v) in new_obj {
            let old_val = old_obj.get(k).unwrap_or(&Value::Null);
            if old_val != v {
                changes.insert(k.clone(), (old_val.to_string(), v.to_string()));
            }
        }
    }
    let old_tasks = old.tasks.join(",");
    let new_tasks = new.tasks.join(",");
    if old_tasks != new_tasks {
        changes.insert("tasks".to_string(), (old_tasks, new_tasks));
    }
    changes
}

/// Apply DAG changes: generate Python files, upload to S3, persist state.
pub async fn apply_dags(
    manifest: &ProjectManifest,
    dags: &[airflow_dag::ResolvedDag],
    root_dir: &Path,
    dry_run: bool,
    storage: &storage::Storage,
) -> Result<DagApplyResult> {
    // Load current DAG state
    let mut dag_deployments = HashMap::new();
    let dag_names = storage.list_dags().await?;
    for name in &dag_names {
        if let Some(state) = storage.read_dag(name).await? {
            dag_deployments.insert(name.clone(), state.deployment);
        }
    }

    let diffs = calculate_dag_diffs(manifest, dags, &dag_deployments)?;
    let mut result = DagApplyResult {
        created: Vec::new(),
        modified: Vec::new(),
        deleted: Vec::new(),
        required_connections: Vec::new(),
    };

    if diffs.is_empty() {
        return Ok(result);
    }

    let dag_gen_dir = root_dir.join(".yard/generated/dags");
    std::fs::create_dir_all(&dag_gen_dir)?;

    for diff in &diffs {
        match &diff.diff_type {
            DiffType::Create | DiffType::Modify { .. } => {
                let dag = dags
                    .iter()
                    .find(|d| d.name == diff.name)
                    .ok_or_else(|| anyhow!("DAG {} missing during apply", diff.name))?;

                let content = airflow_dag::generate_dag(manifest, dag)?;
                let content_hash = utils::calculate_hash(&content);

                // Write locally
                let dag_path = dag_gen_dir.join(format!("{}.py", diff.name));
                std::fs::write(&dag_path, &content)?;

                // Upload to S3 if dags_bucket is configured and not dry-run.
                // Returns (s3_uri, effective_aws) — persist aws on DagState so
                // the destroy path can re-auth without the DAG's source dir (D-05).
                let upload_result = if !dry_run {
                    upload_dag_to_s3(manifest, dag, &content).await?
                } else {
                    None
                };
                let (s3_uri, effective_aws) = match upload_result {
                    Some((uri, aws)) => (Some(uri), aws),
                    None => (None, Value::Null),
                };

                let status = if s3_uri.is_some() {
                    "deployed"
                } else {
                    "generated"
                };

                let dag_state = DagState {
                    dag_name: diff.name.clone(),
                    project: manifest.project.clone(),
                    deployment: DagDeployment {
                        content_hash,
                        config: serde_json::to_value(&dag.config).unwrap_or_default(),
                        tasks: dag.tasks.clone(),
                        status: status.to_string(),
                        applied_at: chrono::Utc::now().to_rfc3339(),
                        s3_uri,
                    },
                    aws: effective_aws,
                };

                storage.write_dag(&diff.name, &dag_state).await?;

                if matches!(&diff.diff_type, DiffType::Create) {
                    result.created.push(diff.name.clone());
                } else {
                    result.modified.push(diff.name.clone());
                }
            }
            DiffType::Delete => {
                // Delete S3 file if it was deployed
                if !dry_run
                    && let Some(existing) = dag_deployments.get(&diff.name)
                    && let Some(ref uri) = existing.s3_uri
                {
                    delete_dag_from_s3(manifest, &diff.name, uri).await?;
                }

                storage.delete_dag(&diff.name).await?;

                let dag_path = dag_gen_dir.join(format!("{}.py", diff.name));
                if dag_path.exists() {
                    let _ = std::fs::remove_file(dag_path);
                }

                result.deleted.push(diff.name.clone());
            }
        }
    }

    let mut conn_set: std::collections::BTreeMap<String, airflow_dag::RequiredConnection> =
        std::collections::BTreeMap::new();
    for diff in &diffs {
        if matches!(diff.diff_type, DiffType::Create | DiffType::Modify { .. })
            && let Some(dag) = dags.iter().find(|d| d.name == diff.name)
        {
            for rc in airflow_dag::required_connections_for_dag(manifest, dag)? {
                conn_set.entry(rc.conn_id.clone()).or_insert(rc);
            }
        }
    }
    result.required_connections = conn_set.into_values().collect();

    Ok(result)
}

/// Resolve the effective `aws:` block for a DAG's upload bucket.
///
/// Precedence (highest first):
///   1. `dag.config.aws` — the `AirflowSection.aws` field (D-05).
///   2. Root `aws:` shallow-merged with nearest `account.yaml` `aws:` via
///      `resolve_aws_for_dir` — today's cascade, preserved when the new
///      `AirflowSection.aws` is Null (D-02 strictly additive).
///
/// Per-job `_aws` is intentionally ignored; see the
/// `dag_upload_credentials_ignore_job_aws` test for the invariant.
///
/// Returns `Value::Null` if neither source produces a value; callers pass
/// `None` to `providers::aws_config` in that case (default chain).
fn resolve_effective_dag_aws(manifest: &ProjectManifest, dag: &airflow_dag::ResolvedDag) -> Value {
    if !dag.config.aws.is_null() {
        // Explicit config on the AirflowSection — use verbatim, no merge.
        return dag.config.aws.clone();
    }
    resolve_aws_for_dir(&manifest.aws, &dag.dir)
}

/// Upload a generated DAG file to S3 using the resolved dags_bucket/dags_prefix.
/// Returns `Ok(Some((s3_uri, effective_aws)))` on success — the `effective_aws`
/// must be persisted on `DagState.aws` so the destroy path can re-authenticate
/// to the same account without needing the DAG's source dir (D-05).
/// Returns `Ok(None)` if no `dags_bucket` is configured.
async fn upload_dag_to_s3(
    manifest: &ProjectManifest,
    dag: &airflow_dag::ResolvedDag,
    content: &str,
) -> Result<Option<(String, Value)>> {
    let Some(ref bucket) = dag.config.dags_bucket else {
        return Ok(None);
    };

    let region = extract_airflow_region(manifest)?;
    let prefix = dag.config.dags_prefix.as_deref().unwrap_or("dags/");

    let effective_aws = resolve_effective_dag_aws(manifest, dag);
    let aws_cfg_opt = if effective_aws.is_null() {
        None
    } else {
        Some(&effective_aws)
    };
    let s3_ops = providers::S3ScriptOps {
        s3_client: aws_sdk_s3::Client::new(&providers::aws_config(&region, aws_cfg_opt).await),
        script_bucket: bucket.clone(),
        script_prefix: prefix.to_string(),
    };

    let uri = s3_ops.upload_script(&dag.name, content).await?;
    Ok(Some((uri, effective_aws)))
}

/// Merge the root `aws:` block with the nearest-ancestor `account.yaml` `aws:`
/// override found at or above `dir`. Mirrors the cascade done at discovery
/// time so DAG uploads/deletes respect account-level overrides.
fn resolve_aws_for_dir(root_aws: &Value, dir: &Path) -> Value {
    let account_aws = resolve::find_and_parse_context(dir, "account.yaml", false)
        .ok()
        .and_then(|v| v.get("aws").cloned())
        .unwrap_or(Value::Null);
    merge_provider_config(root_aws, &account_aws)
}

/// Delete a DAG file from S3.
async fn delete_dag_from_s3(
    manifest: &ProjectManifest,
    dag_name: &str,
    _s3_uri: &str,
) -> Result<()> {
    // Parse bucket/prefix from the manifest's airflow provider config
    let airflow_config = manifest
        .providers
        .get("airflow")
        .ok_or_else(|| anyhow!("Cannot delete DAG from S3: no airflow provider config"))?;
    let section = parse_airflow_section(airflow_config);

    let Some(ref bucket) = section.dags_bucket else {
        return Ok(());
    };

    let region = extract_airflow_region(manifest)?;
    let prefix = section.dags_prefix.as_deref().unwrap_or("dags/");

    // Destroy path runs without the DAG's source dir (state-only), so
    // account.yaml overrides can't be re-resolved here — root `aws:` applies.
    let s3_ops = providers::S3ScriptOps {
        s3_client: aws_sdk_s3::Client::new(
            &providers::aws_config(&region, Some(&manifest.aws)).await,
        ),
        script_bucket: bucket.clone(),
        script_prefix: prefix.to_string(),
    };

    s3_ops.delete_script(dag_name).await
}

/// Extract the AWS region from the airflow provider config.
fn extract_airflow_region(manifest: &ProjectManifest) -> Result<String> {
    // Try airflow provider config first
    if let Some(airflow_config) = manifest.providers.get("airflow")
        && let Some(region) = airflow_config.get("region").and_then(|v| v.as_str())
    {
        return Ok(region.to_string());
    }
    // Fall back to state backend region
    match &manifest.state {
        yard_structs::StateBackend::S3 { region, .. } => Ok(region.clone()),
        _ => Err(anyhow!(
            "Cannot determine AWS region for DAG S3 upload. \
             Set `region` in providers.airflow or use an S3 state backend."
        )),
    }
}

/// Result of destroying DAGs.
pub struct DagDestroyResult {
    pub destroyed: Vec<String>,
}

/// Destroy a single DAG: delete S3 file, delete state, delete generated script.
pub async fn destroy_dag(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, Value>,
    aws: &Value,
    dag_name: &str,
    root_dir: &Path,
    dry_run: bool,
) -> Result<bool> {
    let storage = storage::get_storage(backend).await?;

    let dag_state = match storage.read_dag(dag_name).await? {
        Some(s) => s,
        None => return Ok(false),
    };

    let lock_key = format!("{}{dag_name}", storage::DAG_STATE_PREFIX);
    let lock = storage.lock(&lock_key).await?;

    let result: Result<()> = async {
        // Delete S3 file if deployed
        if !dry_run
            && dag_state.deployment.s3_uri.is_some()
            && let Some(airflow_config) = provider_configs.get("airflow")
        {
            let section = parse_airflow_section(airflow_config);
            if let Some(ref bucket) = section.dags_bucket {
                let region = airflow_config
                    .get("region")
                    .and_then(|v| v.as_str())
                    .unwrap_or("us-east-1");
                let prefix = section.dags_prefix.as_deref().unwrap_or("dags/");

                let s3_ops = providers::S3ScriptOps {
                    s3_client: aws_sdk_s3::Client::new(
                        &providers::aws_config(region, Some(aws)).await,
                    ),
                    script_bucket: bucket.clone(),
                    script_prefix: prefix.to_string(),
                };
                s3_ops.delete_script(dag_name).await?;
            }
        }

        storage.delete_dag(dag_name).await?;

        let dag_path = root_dir
            .join(".yard/generated/dags")
            .join(format!("{dag_name}.py"));
        if dag_path.exists() {
            let _ = std::fs::remove_file(dag_path);
        }

        Ok(())
    }
    .await;

    storage.unlock(&lock_key, &lock).await?;
    result?;

    Ok(true)
}

/// Destroy all DAGs that have state.
pub async fn destroy_all_dags(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, Value>,
    aws: &Value,
    root_dir: &Path,
    dry_run: bool,
) -> Result<DagDestroyResult> {
    let storage = storage::get_storage(backend).await?;
    let dag_names = storage.list_dags().await?;
    let mut result = DagDestroyResult {
        destroyed: Vec::new(),
    };

    for name in dag_names {
        if destroy_dag(backend, provider_configs, aws, &name, root_dir, dry_run).await? {
            result.destroyed.push(name);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::{
        parse_airflow_job_block, parse_body, parse_imports, parse_job_file, parse_sink,
        parse_sources, parse_transforms,
    };
    use serde_json::json;
    use yard_structs::JobDefinition;

    /// Locks the invariant flagged when scoping cross-account DAGs:
    /// DAG-upload credentials come strictly from root + nearest `account.yaml`.
    /// Per-job `_aws` must never be consulted for DAG artifact upload,
    /// otherwise a cross-account job would hijack the upload target from the
    /// MWAA home account. `resolve_aws_for_dir` takes no job context, which
    /// is the structural guarantee; this test also verifies the runtime
    /// cascade (root <- account.yaml) behaves as expected.
    #[test]
    fn dag_upload_credentials_ignore_job_aws() {
        let tmp =
            std::env::temp_dir().join(format!("yard_dag_upload_invariant_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let dag_dir = tmp.join("pipeline");
        std::fs::create_dir_all(&dag_dir).unwrap();
        std::fs::write(
            tmp.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::111111111111:role/AccountA\n",
        )
        .unwrap();

        let root_aws = json!({"assume_role": "arn:aws:iam::999999999999:role/Root"});
        let resolved = resolve_aws_for_dir(&root_aws, &dag_dir);
        // account.yaml wins, proving the cascade uses account context only.
        assert_eq!(
            resolved
                .get("assume_role")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "arn:aws:iam::111111111111:role/AccountA"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    fn make_job(job_type: &str, config: serde_json::Value) -> JobDefinition {
        let imports = parse_imports(&config);
        let body = parse_body(&config);
        let job_file = parse_job_file(&config);
        let sources = parse_sources(&config);
        let sink = parse_sink(&config);
        let transforms = parse_transforms(&config);
        let airflow = parse_airflow_job_block(&config);

        // Inject a default role for glue jobs so tests pass validation
        let config = if job_type == "glue" && config.get("role").is_none() {
            let mut obj = config;
            obj.as_object_mut()
                .expect("config must be a JSON object")
                .insert(
                    "role".to_string(),
                    serde_json::Value::String(
                        "arn:aws:iam::123456789:role/TestGlueRole".to_string(),
                    ),
                );
            obj
        } else {
            config
        };

        JobDefinition {
            job_type: job_type.to_string(),
            imports,
            body,
            job_file,
            sources,
            sink,
            transforms,
            airflow,
            partition_by: Vec::new(),
            partition_timestamp_column: None,
            create_timestamp: false,
            config,
            dir: std::path::PathBuf::new(),
            base_name: String::new(),
        }
    }

    fn make_resolved_dag(name: &str, tasks: Vec<&str>) -> airflow_dag::ResolvedDag {
        use std::collections::BTreeMap;
        airflow_dag::ResolvedDag {
            name: name.to_string(),
            dir: std::path::PathBuf::from("/tmp/fake"),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                ..Default::default()
            },
            tasks: tasks.iter().map(|s| s.to_string()).collect(),
            depends_on: BTreeMap::new(),
        }
    }

    fn make_dag_deployment(content_hash: &str, tasks: Vec<&str>) -> DagDeployment {
        DagDeployment {
            content_hash: content_hash.to_string(),
            config: json!({"schedule": "@daily"}),
            tasks: tasks.iter().map(|s| s.to_string()).collect(),
            status: "generated".to_string(),
            applied_at: "2025-01-01T00:00:00Z".to_string(),
            s3_uri: None,
        }
    }

    #[test]
    fn dag_diff_detects_create() {
        let dag = make_resolved_dag("test_dag", vec!["task_a"]);
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([(
                "task_a".to_string(),
                make_job("bash", json!({"type": "bash", "command": "echo hi"})),
            )]),
            aws: serde_json::Value::Null,
        };

        let dag_deployments = HashMap::new();
        let diffs = calculate_dag_diffs(&manifest, &[dag], &dag_deployments).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Create));
        assert_eq!(diffs[0].name, "test_dag");
    }

    #[test]
    fn dag_diff_detects_delete() {
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: serde_json::Value::Null,
        };

        let dag_deployments = HashMap::from([(
            "old_dag".to_string(),
            make_dag_deployment("oldhash", vec!["task_a"]),
        )]);

        let diffs = calculate_dag_diffs(&manifest, &[], &dag_deployments).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Delete));
        assert_eq!(diffs[0].name, "old_dag");
    }

    #[test]
    fn dag_diff_detects_no_change() {
        let dag = make_resolved_dag("test_dag", vec!["task_a"]);
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([(
                "task_a".to_string(),
                make_job("bash", json!({"type": "bash", "command": "echo hi"})),
            )]),
            aws: serde_json::Value::Null,
        };

        // Generate the actual hash that would be produced
        let content = airflow_dag::generate_dag(&manifest, &dag).unwrap();
        let hash = crate::utils::calculate_hash(&content);

        let dag_deployments = HashMap::from([(
            "test_dag".to_string(),
            make_dag_deployment(&hash, vec!["task_a"]),
        )]);

        let diffs = calculate_dag_diffs(&manifest, &[dag], &dag_deployments).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn dag_diff_detects_modify() {
        let dag = make_resolved_dag("test_dag", vec!["task_a"]);
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([(
                "task_a".to_string(),
                make_job("bash", json!({"type": "bash", "command": "echo hi"})),
            )]),
            aws: serde_json::Value::Null,
        };

        // Use a stale hash
        let dag_deployments = HashMap::from([(
            "test_dag".to_string(),
            make_dag_deployment("stale_hash", vec!["task_a"]),
        )]);

        let diffs = calculate_dag_diffs(&manifest, &[dag], &dag_deployments).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    // --- Phase 9 Plan 03 Task 1: apply-path credential resolution ---

    #[test]
    fn resolve_effective_dag_aws_prefers_dag_config_aws() {
        let tmp =
            std::env::temp_dir().join(format!("yard_dag_aws_priority_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        // account.yaml that WOULD apply via the cascade if dag.config.aws were Null.
        std::fs::write(
            tmp.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::111111111111:role/Account\n",
        )
        .unwrap();

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: json!({"assume_role": "arn:aws:iam::999999999999:role/Root"}),
        };
        let dag = airflow_dag::ResolvedDag {
            name: "test_dag".to_string(),
            dir: tmp.clone(),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                aws: json!({"assume_role": "arn:aws:iam::222222222222:role/DagExplicit"}),
                ..Default::default()
            },
            tasks: Vec::new(),
            depends_on: std::collections::BTreeMap::new(),
        };

        let effective = resolve_effective_dag_aws(&manifest, &dag);
        // dag.config.aws wins OUTRIGHT — no merge with account.yaml.
        assert_eq!(
            effective.get("assume_role").and_then(|v| v.as_str()),
            Some("arn:aws:iam::222222222222:role/DagExplicit")
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_effective_dag_aws_falls_back_to_cascade() {
        let tmp =
            std::env::temp_dir().join(format!("yard_dag_aws_fallback_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        std::fs::write(
            tmp.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::111111111111:role/Account\n",
        )
        .unwrap();

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: json!({"assume_role": "arn:aws:iam::999999999999:role/Root"}),
        };
        let dag = airflow_dag::ResolvedDag {
            name: "test_dag".to_string(),
            dir: tmp.clone(),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                aws: Value::Null, // unset; cascade should apply
                ..Default::default()
            },
            tasks: Vec::new(),
            depends_on: std::collections::BTreeMap::new(),
        };

        let effective = resolve_effective_dag_aws(&manifest, &dag);
        // account.yaml wins per the existing cascade (nearest ancestor).
        assert_eq!(
            effective.get("assume_role").and_then(|v| v.as_str()),
            Some("arn:aws:iam::111111111111:role/Account")
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn resolve_effective_dag_aws_all_null_returns_null() {
        let tmp = std::env::temp_dir().join(format!("yard_dag_aws_null_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: Value::Null,
        };
        let dag = airflow_dag::ResolvedDag {
            name: "test_dag".to_string(),
            dir: tmp.clone(),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                aws: Value::Null,
                ..Default::default()
            },
            tasks: Vec::new(),
            depends_on: std::collections::BTreeMap::new(),
        };

        let effective = resolve_effective_dag_aws(&manifest, &dag);
        assert!(
            effective.is_null(),
            "absent everywhere → Null → caller passes None to aws_config (D-02)"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
