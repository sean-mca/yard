use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use yard_structs::{
    AwsCredentialConfig, DagDeployment, DagDiff, DagState, DiffType, JobState, ProjectManifest,
};

use crate::airflow_dag;
use crate::providers;
use crate::resolve;
use crate::storage;
use crate::utils;

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

/// Load the current persisted script URIs for every job in state, given an
/// already-open storage handle. Returns `job_name -> s3_uri` filtered to jobs
/// with a persisted `s3_object` resource. Single source of truth for the
/// in-core call sites (`apply_dags`, `plan`, `show_dag`) that must pre-compute
/// `script_locations` for `calculate_dag_diffs` / `generate_dag`.
pub(crate) async fn load_script_locations_from_storage(
    storage: &storage::Storage,
) -> Result<HashMap<String, String>> {
    let mut job_states: HashMap<String, JobState> = HashMap::new();
    let job_names = storage.list_jobs().await?;
    for name in &job_names {
        if let Some(state) = storage.read_job(name).await? {
            job_states.insert(name.clone(), state);
        }
    }
    Ok(airflow_dag::script_locations_from_state(&job_states))
}

/// CLI-facing wrapper around `load_script_locations_from_storage`: opens
/// storage from a state backend and returns the same `job_name -> s3_uri`
/// projection. Kept as the public entry point so CLI callers don't need to
/// know about `storage::Storage` directly (per CLAUDE.md "all logic in
/// yard-core").
pub async fn load_script_locations(
    backend: &yard_structs::StateBackend,
) -> Result<HashMap<String, String>> {
    let storage = storage::get_storage(backend).await?;
    load_script_locations_from_storage(&storage).await
}

/// Compute the diff between resolved DAGs and stored DAG state.
pub fn calculate_dag_diffs(
    manifest: &ProjectManifest,
    dags: &[airflow_dag::ResolvedDag],
    dag_deployments: &HashMap<String, DagDeployment>,
    script_locations: &HashMap<String, String>,
) -> Result<Vec<DagDiff>> {
    let mut diffs = Vec::new();

    for dag in dags {
        let content = airflow_dag::generate_dag(manifest, dag, script_locations)?;
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
///
/// Phase 28 / D-16: returns `BTreeMap<String, (String, String)>` matching
/// `DiffType::Modify::changes`'s new BTreeMap field type — sibling of
/// `compare_json` in `diff.rs`. BTreeMap iterates in key-sorted order
/// natively, so downstream consumers (display.rs, github/router.rs,
/// api/drift.rs) iterate without per-site sort logic.
fn compare_dag_config(
    old: &DagDeployment,
    new: &airflow_dag::ResolvedDag,
) -> BTreeMap<String, (String, String)> {
    let mut changes = BTreeMap::new();
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
    // Load full DagState (not just .deployment) so the Delete branch below
    // can re-authenticate using the persisted `aws:` per D-05.
    let mut dag_states: HashMap<String, DagState> = HashMap::new();
    let dag_names = storage.list_dags().await?;
    for name in &dag_names {
        if let Some(state) = storage.read_dag(name).await? {
            dag_states.insert(name.clone(), state);
        }
    }
    // `calculate_dag_diffs` still wants a deployment-only map — build it from
    // the full states without re-reading storage.
    let dag_deployments: HashMap<String, DagDeployment> = dag_states
        .iter()
        .map(|(k, v)| (k.clone(), v.deployment.clone()))
        .collect();

    // Pre-load all JobStates so DAG render (which now reads each Glue task's
    // persisted script_location from state per DAG-02) does not re-traverse
    // storage per render. Mirrors the dag_states bulk-load above.
    let script_locations = load_script_locations_from_storage(storage).await?;

    let diffs = calculate_dag_diffs(manifest, dags, &dag_deployments, &script_locations)?;
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

                let content = airflow_dag::generate_dag(manifest, dag, &script_locations)?;
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
                    None => (None, None),
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
                // Delete S3 file if it was deployed. Re-auth uses the
                // persisted `DagState.aws` when present (D-05); otherwise
                // fall back to `manifest.aws` for pre-Phase-9 state files.
                if !dry_run
                    && let Some(existing_state) = dag_states.get(&diff.name)
                    && let Some(ref uri) = existing_state.deployment.s3_uri
                {
                    let destroy_aws = resolve_destroy_dag_aws(
                        existing_state.aws.as_ref(),
                        manifest.aws.as_ref(),
                    );
                    delete_dag_from_s3(manifest, destroy_aws, &diff.name, uri).await?;
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
///      `AirflowSection.aws` is `None` (D-02 strictly additive).
///
/// Per-job `_aws` is intentionally ignored; see the
/// `dag_upload_credentials_ignore_job_aws` test for the invariant.
///
/// Returns `None` if neither source produces a value; callers pass
/// `None` to `providers::aws_config` in that case (default chain).
pub(crate) fn resolve_effective_dag_aws(
    manifest: &ProjectManifest,
    dag: &airflow_dag::ResolvedDag,
) -> Option<AwsCredentialConfig> {
    if let Some(ref creds) = dag.config.aws {
        // Explicit config on the AirflowSection — use verbatim, no merge.
        return Some(creds.clone());
    }
    resolve_aws_for_dir(manifest.aws.as_ref(), &dag.dir)
}

/// Resolve the `aws:` block to use at destroy time.
///
/// Preference:
///   1. `dag_state_aws` — persisted at apply time by `apply_dags` (D-05).
///   2. `fallback` — today's behavior for state files written before Phase 9,
///      where `DagState.aws` is `None`. Callers should pass the project-root
///      `manifest.aws` (or whatever they already had) as this fallback to
///      preserve existing semantics.
fn resolve_destroy_dag_aws<'a>(
    dag_state_aws: Option<&'a AwsCredentialConfig>,
    fallback: Option<&'a AwsCredentialConfig>,
) -> Option<&'a AwsCredentialConfig> {
    dag_state_aws.or(fallback)
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
) -> Result<Option<(String, Option<AwsCredentialConfig>)>> {
    let Some(ref bucket) = dag.config.dags_bucket else {
        return Ok(None);
    };

    let region = extract_airflow_region(manifest)?;
    let prefix = dag.config.dags_prefix.as_deref().unwrap_or("dags/");

    let effective_aws = resolve_effective_dag_aws(manifest, dag);
    // The providers::aws_config boundary stays Value-typed (D-14); convert
    // at the call site via serde_json::to_value.
    let effective_value: Option<Value> = effective_aws
        .as_ref()
        .map(serde_json::to_value)
        .transpose()
        .context("Failed to serialize effective DAG AWS credentials")?;
    let aws_cfg_opt = effective_value.as_ref();
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
///
/// Account.yaml's `aws:` block is parsed into `AwsCredentialConfig` via
/// best-effort `serde_json::from_value(..).ok()`: malformed `aws:` blocks at
/// the account level fall through silently, preserving today's permissive
/// behavior. (Strict typo gating for the user yard.yaml `aws:` block lives
/// in plan 21-03 via the `validate_unknown_keys` helper.)
fn resolve_aws_for_dir(
    root_aws: Option<&AwsCredentialConfig>,
    dir: &Path,
) -> Option<AwsCredentialConfig> {
    let account_aws: Option<AwsCredentialConfig> =
        resolve::find_and_parse_context(dir, "account.yaml", false)
            .ok()
            .and_then(|v| v.get("aws").cloned())
            .and_then(|v| serde_json::from_value(v).ok());

    match (root_aws, account_aws) {
        (None, None) => None,
        (Some(r), None) => Some(r.clone()),
        (None, Some(a)) => Some(a),
        (Some(r), Some(a)) => Some(AwsCredentialConfig::merge(r, &a)),
    }
}

/// Delete a DAG file from S3. Uses the caller-supplied `dag_state_aws`
/// (typically `DagState.aws` persisted at apply time — see D-05). The
/// previous signature took only `&ProjectManifest` and read `manifest.aws`,
/// which authenticated destroy against the project root even when the DAG
/// had been uploaded to a different account.
async fn delete_dag_from_s3(
    manifest: &ProjectManifest,
    dag_state_aws: Option<&AwsCredentialConfig>,
    dag_name: &str,
    _s3_uri: &str,
) -> Result<()> {
    // Parse bucket/prefix from the manifest's airflow provider config
    let airflow_config = manifest
        .providers
        .get("airflow")
        .ok_or_else(|| anyhow!("Cannot delete DAG from S3: no airflow provider config"))?;
    let section = parse_airflow_section(airflow_config, "providers.airflow")?;

    let Some(ref bucket) = section.dags_bucket else {
        return Ok(());
    };

    let region = extract_airflow_region(manifest)?;
    let prefix = section.dags_prefix.as_deref().unwrap_or("dags/");

    // Destroy re-authenticates using `DagState.aws` persisted at apply time
    // (D-05). Pre-Phase-9 state files have `aws: None`; callers pass the
    // project-root `manifest.aws` as a fallback so today's behavior is
    // preserved for legacy state. The providers::aws_config boundary stays
    // Value-typed (D-14); convert here via serde_json::to_value.
    let dag_state_value: Option<Value> = dag_state_aws
        .map(serde_json::to_value)
        .transpose()
        .context("Failed to serialize destroy AWS credentials")?;
    let aws_cfg_opt = dag_state_value.as_ref();
    let s3_ops = providers::S3ScriptOps {
        s3_client: aws_sdk_s3::Client::new(&providers::aws_config(&region, aws_cfg_opt).await),
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
    aws: Option<&AwsCredentialConfig>,
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
        // Delete S3 file if deployed. Re-auth prefers the persisted
        // `DagState.aws` (D-05); the caller-supplied `aws` is the fallback
        // for pre-Phase-9 state files where `DagState.aws` is `Null`.
        if !dry_run
            && dag_state.deployment.s3_uri.is_some()
            && let Some(airflow_config) = provider_configs.get("airflow")
        {
            let section = parse_airflow_section(airflow_config, "providers.airflow")?;
            if let Some(ref bucket) = section.dags_bucket {
                let region = airflow_config
                    .get("region")
                    .and_then(|v| v.as_str())
                    .unwrap_or("us-east-1");
                let prefix = section.dags_prefix.as_deref().unwrap_or("dags/");

                let destroy_aws = resolve_destroy_dag_aws(dag_state.aws.as_ref(), aws);
                // The providers::aws_config boundary stays Value-typed (D-14);
                // convert at the call site via serde_json::to_value.
                let destroy_value: Option<Value> = destroy_aws
                    .map(serde_json::to_value)
                    .transpose()
                    .context("Failed to serialize destroy AWS credentials")?;
                let aws_cfg_opt = destroy_value.as_ref();
                let s3_ops = providers::S3ScriptOps {
                    s3_client: aws_sdk_s3::Client::new(
                        &providers::aws_config(region, aws_cfg_opt).await,
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
    aws: Option<&AwsCredentialConfig>,
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::parsing::{
        parse_airflow_job_block, parse_body, parse_imports, parse_job_file, parse_sink,
        parse_sources, parse_transforms,
    };
    use serde_json::json;
    use yard_structs::{AwsCredentialConfig, JobDefinition, JobType};

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

        let root_aws = AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::999999999999:role/Root".to_string()),
            ..Default::default()
        };
        let resolved = resolve_aws_for_dir(Some(&root_aws), &dag_dir);
        // account.yaml wins, proving the cascade uses account context only.
        assert_eq!(
            resolved
                .as_ref()
                .and_then(|c| c.assume_role.as_deref())
                .unwrap_or(""),
            "arn:aws:iam::111111111111:role/AccountA"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    fn make_job(job_type: JobType, config: serde_json::Value) -> JobDefinition {
        let imports = parse_imports(&config);
        let body = parse_body(&config);
        let job_file = parse_job_file(&config);
        let sources = parse_sources(&config, "test").expect("test fixture must parse");
        let sink = parse_sink(&config, "test").expect("test fixture must parse");
        let transforms = parse_transforms(&config, "test").expect("test fixture must parse");
        let airflow = parse_airflow_job_block(&config, "test").expect("test fixture must parse");

        // Inject a default role for glue jobs so tests pass validation
        let config = if job_type == JobType::Glue && config.get("role").is_none() {
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
            job_type,
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
                make_job(JobType::Bash, json!({"type": "bash", "command": "echo hi"})),
            )]),
            aws: None,
        };

        let dag_deployments = HashMap::new();
        let diffs =
            calculate_dag_diffs(&manifest, &[dag], &dag_deployments, &HashMap::new()).unwrap();
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
            aws: None,
        };

        let dag_deployments = HashMap::from([(
            "old_dag".to_string(),
            make_dag_deployment("oldhash", vec!["task_a"]),
        )]);

        let diffs =
            calculate_dag_diffs(&manifest, &[], &dag_deployments, &HashMap::new()).unwrap();
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
                make_job(JobType::Bash, json!({"type": "bash", "command": "echo hi"})),
            )]),
            aws: None,
        };

        // Generate the actual hash that would be produced
        let content = airflow_dag::generate_dag(&manifest, &dag, &HashMap::new()).unwrap();
        let hash = crate::utils::calculate_hash(&content);

        let dag_deployments = HashMap::from([(
            "test_dag".to_string(),
            make_dag_deployment(&hash, vec!["task_a"]),
        )]);

        let diffs =
            calculate_dag_diffs(&manifest, &[dag], &dag_deployments, &HashMap::new()).unwrap();
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
                make_job(JobType::Bash, json!({"type": "bash", "command": "echo hi"})),
            )]),
            aws: None,
        };

        // Use a stale hash
        let dag_deployments = HashMap::from([(
            "test_dag".to_string(),
            make_dag_deployment("stale_hash", vec!["task_a"]),
        )]);

        let diffs =
            calculate_dag_diffs(&manifest, &[dag], &dag_deployments, &HashMap::new()).unwrap();
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
            aws: Some(AwsCredentialConfig {
                assume_role: Some("arn:aws:iam::999999999999:role/Root".to_string()),
                ..Default::default()
            }),
        };
        let dag = airflow_dag::ResolvedDag {
            name: "test_dag".to_string(),
            dir: tmp.clone(),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                aws: Some(AwsCredentialConfig {
                    assume_role: Some(
                        "arn:aws:iam::222222222222:role/DagExplicit".to_string(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            },
            tasks: Vec::new(),
            depends_on: std::collections::BTreeMap::new(),
        };

        let effective = resolve_effective_dag_aws(&manifest, &dag);
        // dag.config.aws wins OUTRIGHT — no merge with account.yaml.
        assert_eq!(
            effective.as_ref().and_then(|c| c.assume_role.as_deref()),
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
            aws: Some(AwsCredentialConfig {
                assume_role: Some("arn:aws:iam::999999999999:role/Root".to_string()),
                ..Default::default()
            }),
        };
        let dag = airflow_dag::ResolvedDag {
            name: "test_dag".to_string(),
            dir: tmp.clone(),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                aws: None, // unset; cascade should apply
                ..Default::default()
            },
            tasks: Vec::new(),
            depends_on: std::collections::BTreeMap::new(),
        };

        let effective = resolve_effective_dag_aws(&manifest, &dag);
        // account.yaml wins per the existing cascade (nearest ancestor).
        assert_eq!(
            effective.as_ref().and_then(|c| c.assume_role.as_deref()),
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
            aws: None,
        };
        let dag = airflow_dag::ResolvedDag {
            name: "test_dag".to_string(),
            dir: tmp.clone(),
            config: yard_structs::AirflowSection {
                schedule: Some("@daily".to_string()),
                aws: None,
                ..Default::default()
            },
            tasks: Vec::new(),
            depends_on: std::collections::BTreeMap::new(),
        };

        let effective = resolve_effective_dag_aws(&manifest, &dag);
        assert!(
            effective.is_none(),
            "absent everywhere → None → caller passes None to aws_config (D-02)"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // --- Phase 9 Plan 03 Task 2: destroy-path credential resolution ---

    #[test]
    fn resolve_destroy_dag_aws_prefers_state() {
        // When DagState.aws is populated, it wins over the caller-supplied
        // fallback (today's manifest.aws) — this closes D-05's destroy gap.
        let state_aws = AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::111111111111:role/FromState".to_string()),
            ..Default::default()
        };
        let fallback = AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::999999999999:role/Fallback".to_string()),
            ..Default::default()
        };
        let picked = resolve_destroy_dag_aws(Some(&state_aws), Some(&fallback));
        assert_eq!(
            picked.and_then(|c| c.assume_role.as_deref()),
            Some("arn:aws:iam::111111111111:role/FromState")
        );
    }

    #[test]
    fn resolve_destroy_dag_aws_falls_back_when_state_null() {
        // Legacy pre-Phase-9 state files have DagState.aws == None. In that
        // case, fall back to the caller-supplied aws (typically manifest.aws)
        // to preserve today's behavior byte-for-byte.
        let fallback = AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::999999999999:role/Fallback".to_string()),
            ..Default::default()
        };
        let picked = resolve_destroy_dag_aws(None, Some(&fallback));
        assert_eq!(
            picked.and_then(|c| c.assume_role.as_deref()),
            Some("arn:aws:iam::999999999999:role/Fallback")
        );
    }

    #[test]
    fn resolve_destroy_dag_aws_both_null_returns_null() {
        let picked = resolve_destroy_dag_aws(None, None);
        assert!(
            picked.is_none(),
            "both None → None → caller passes None to aws_config → default chain (D-02)"
        );
    }
}
