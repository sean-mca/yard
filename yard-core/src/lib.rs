pub mod airflow_dag;
pub mod codegen;
pub mod providers;
pub mod resolve;
pub mod storage;
pub mod utils;
pub mod validation;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use yard_structs::{
    AirflowJobBlock, AirflowSection, DagDeployment, DagDiff, DagState, Deployment, DiffType,
    Import, JobDiff, JobState, ProjectManifest, ProjectState, ResourceStatus, Sink, Source,
    Transform,
};

/// Job types that are Airflow tasks only — they don't have a Spark artifact to
/// generate and no provider to deploy through. Used in validation, codegen,
/// and apply to short-circuit the Spark path. **Single source of truth** —
/// callers must use this helper instead of hard-coding the list.
pub fn is_task_only(job_type: &str) -> bool {
    matches!(job_type, "bash")
}

/// Build the `Value` passed to `get_provider`: provider defaults shallow-
/// merged with the job's `<job_type>:` block, plus the per-job `_aws` block
/// (resolved at discovery time) injected alongside.
pub fn build_provider_config(
    provider_defaults: &Value,
    full_config: &Value,
    job_type: &str,
) -> Value {
    let job_overrides = full_config.get(job_type).cloned().unwrap_or(Value::Null);
    let mut merged = merge_provider_config(provider_defaults, &job_overrides);
    if let Some(aws) = full_config.get("_aws")
        && let Some(obj) = merged.as_object_mut()
    {
        obj.insert("_aws".to_string(), aws.clone());
    }
    merged
}

/// Merge provider-level defaults with job-level overrides.
/// Provider config from yard.yaml is the base, job-level block wins on conflicts.
pub fn merge_provider_config(provider_defaults: &Value, job_overrides: &Value) -> Value {
    match (provider_defaults, job_overrides) {
        (Value::Object(base), Value::Object(overrides)) => {
            let mut merged = base.clone();
            for (key, val) in overrides {
                merged.insert(key.clone(), val.clone());
            }
            Value::Object(merged)
        }
        // If job overrides exist but aren't an object, just use defaults
        (_, Value::Null) => provider_defaults.clone(),
        // If defaults aren't an object, just use overrides
        _ => job_overrides.clone(),
    }
}

/// Load the current project state by reading all per-job state files.
/// Errors (permissions, network, corrupt files) are propagated — only
/// genuinely missing state is treated as "no deployments yet."
pub async fn load_state(
    backend: &yard_structs::StateBackend,
    project: &str,
) -> Result<ProjectState> {
    let storage = storage::get_storage(backend)
        .await
        .context("Failed to initialize state backend")?;

    let job_names = storage
        .list_jobs()
        .await
        .context("Failed to list jobs from state backend. Check permissions and connectivity.")?;

    let mut deployments = HashMap::new();
    for name in job_names {
        let job_state = storage.read_job(&name).await.with_context(|| {
            format!("Failed to read state for job \"{name}\". The state file may be corrupt.")
        })?;

        if let Some(state) = job_state {
            deployments.insert(name, state.deployment);
        }
    }

    Ok(ProjectState {
        project: project.to_string(),
        last_updated: chrono::Utc::now().to_rfc3339(),
        deployments,
    })
}

/// Verify that deployed resources still exist in their target services.
/// For each deployed job with resources and a known provider, instantiates the
/// provider and checks each resource. Returns a map of job_name → resource statuses.
/// Jobs without a matching provider config are silently skipped.
pub async fn verify_deployed_resources(
    manifest: &ProjectManifest,
    state: &ProjectState,
) -> Result<HashMap<String, Vec<ResourceStatus>>> {
    let mut results: HashMap<String, Vec<ResourceStatus>> = HashMap::new();

    for (job_name, deployment) in &state.deployments {
        if deployment.resources.is_empty() {
            continue;
        }

        // Determine the job type from the deployment config
        let job_type = match deployment.config.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => continue,
        };

        // Need provider config from the manifest to instantiate the provider
        let provider_defaults = match manifest.providers.get(job_type) {
            Some(config) => config,
            None => continue,
        };

        let merged_config = build_provider_config(provider_defaults, &deployment.config, job_type);
        let provider = providers::get_provider(job_type, &merged_config).await?;
        let statuses = provider
            .verify_resources(job_name, &deployment.resources)
            .await?;

        results.insert(job_name.clone(), statuses);
    }

    Ok(results)
}

/// Compute the diff between the manifest and the current state.
/// Used by both plan (read-only) and apply (before executing changes).
pub fn calculate_diff(manifest: &ProjectManifest, state: &ProjectState) -> Result<Vec<JobDiff>> {
    let mut diffs = Vec::new();

    for (name, job_def) in &manifest.jobs {
        let script_content = crate::codegen::generate_python_script(name, job_def)
            .with_context(|| format!("Failed to generate script for job \"{name}\""))?;

        // Hash both the script and the full job config so config-only changes
        // (e.g. worker_type, timeout) are detected even if the script is unchanged
        let config_str = serde_json::to_string(&job_def.config)
            .with_context(|| format!("Failed to serialize config for job \"{name}\""))?;
        let combined = format!("{script_content}\n{config_str}");
        let current_proposed_hash = crate::utils::calculate_hash(&combined);

        if let Some(existing) = state.deployments.get(name) {
            if existing.config_hash != current_proposed_hash {
                let changes = compare_json(&existing.config, &job_def.config);
                diffs.push(JobDiff {
                    name: name.clone(),
                    diff_type: DiffType::Modify { changes },
                    old_hash: Some(existing.config_hash.clone()),
                    new_hash: Some(current_proposed_hash),
                });
            }
        } else {
            diffs.push(JobDiff {
                name: name.clone(),
                diff_type: DiffType::Create,
                old_hash: None,
                new_hash: Some(current_proposed_hash),
            });
        }
    }

    for (name, existing_state) in &state.deployments {
        if !manifest.jobs.contains_key(name.as_str()) {
            diffs.push(JobDiff {
                name: name.clone(),
                diff_type: DiffType::Delete,
                old_hash: Some(existing_state.config_hash.clone()),
                new_hash: None,
            });
        }
    }

    Ok(diffs)
}

/// Result of applying changes.
#[derive(Debug)]
pub struct ApplyResult {
    pub created: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
    pub dag_created: Vec<String>,
    pub dag_modified: Vec<String>,
    pub dag_deleted: Vec<String>,
}

/// Apply changes: generate scripts, deploy via provider, update state.
/// `root_dir` is where `.yard/generated/` lives.
/// All affected jobs are locked upfront before diffing to prevent race conditions.
/// State is re-read under lock to ensure the diff is computed against fresh data.
/// All jobs are validated before any changes are made.
pub async fn apply(
    manifest: &ProjectManifest,
    current_state: &ProjectState,
    root_dir: &Path,
    dry_run: bool,
) -> Result<ApplyResult> {
    // Validate all jobs up front (schema + syntax) — abort before making any changes
    let mut all_errors: Vec<(String, Vec<yard_structs::ValidationError>)> = Vec::new();
    for (name, job_def) in &manifest.jobs {
        let errors = validation::validate_job_full(name, job_def);
        if !errors.is_empty() {
            all_errors.push((name.clone(), errors));
        }
    }
    if !all_errors.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, errors) in &all_errors {
            for e in errors {
                msg.push_str(&format!("  [{}] {}: {}\n", name, e.field, e.message));
            }
        }
        return Err(anyhow!("{msg}"));
    }

    // Validate orphan airflow blocks (airflow: on jobs outside any DAG dir)
    let pre_dags = airflow_dag::collect_dags(root_dir, manifest)?;
    let orphans = airflow_dag::validate_orphan_airflow_blocks(manifest, &pre_dags);
    if !orphans.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, err) in &orphans {
            msg.push_str(&format!("  [{name}] {err}\n"));
        }
        return Err(anyhow!("{msg}"));
    }

    let storage = storage::get_storage(&manifest.state).await?;

    // Preliminary diff to identify which jobs need locking
    let preliminary_diffs = calculate_diff(manifest, current_state)?;

    // Lock ALL affected jobs upfront — prevents concurrent applies from
    // modifying state between diff calculation and execution
    let job_names: Vec<String> = preliminary_diffs.iter().map(|d| d.name.clone()).collect();
    let locks = if !job_names.is_empty() {
        storage.lock_jobs(&job_names).await?
    } else {
        Vec::new()
    };

    // All work happens inside this block so we always unlock on exit
    let apply_result = async {
        // Re-read fresh state under lock — the passed-in current_state may be stale
        let mut fresh_deployments = HashMap::new();
        for name in &job_names {
            if let Some(job_state) = storage.read_job(name).await? {
                fresh_deployments.insert(name.clone(), job_state.deployment);
            }
        }
        let fresh_state = ProjectState {
            project: current_state.project.clone(),
            last_updated: chrono::Utc::now().to_rfc3339(),
            deployments: fresh_deployments,
        };

        // Authoritative diff against fresh state
        let diffs = calculate_diff(manifest, &fresh_state)?;

        let mut result = ApplyResult {
            created: Vec::new(),
            modified: Vec::new(),
            deleted: Vec::new(),
            dag_created: Vec::new(),
            dag_modified: Vec::new(),
            dag_deleted: Vec::new(),
        };

        for diff in &diffs {
            match &diff.diff_type {
                DiffType::Create | DiffType::Modify { .. } => {
                    let job_def = manifest
                        .jobs
                        .get(&diff.name)
                        .context("Job definition missing during apply")?;

                    let script_content = codegen::generate_python_script(&diff.name, job_def)
                        .context("Failed to generate Python script")?;
                    let config_str = serde_json::to_string(&job_def.config).with_context(|| {
                        format!("Failed to serialize config for job \"{}\"", diff.name)
                    })?;
                    let combined = format!("{script_content}\n{config_str}");
                    let script_hash = utils::calculate_hash(&combined);

                    // Write generated script locally
                    let gen_dir = root_dir.join(".yard/generated");
                    std::fs::create_dir_all(&gen_dir)?;
                    let script_path = gen_dir.join(format!("{}.py", diff.name));
                    std::fs::write(&script_path, &script_content)?;

                    // Deploy via provider if configured (skip in dry-run mode).
                    // Task-only job types (bash, ...) have no Provider impl and
                    // skip straight to state bookkeeping.
                    let resources = if dry_run || is_task_only(&job_def.job_type) {
                        Vec::new()
                    } else if let Some(provider_defaults) =
                        manifest.providers.get(&job_def.job_type)
                    {
                        let merged_config = build_provider_config(
                            provider_defaults,
                            &job_def.config,
                            &job_def.job_type,
                        );
                        let provider =
                            providers::get_provider(&job_def.job_type, &merged_config).await?;
                        provider
                            .deploy(&diff.name, &script_content, &job_def.config)
                            .await?
                    } else {
                        Vec::new()
                    };

                    let status = if resources.is_empty() {
                        "generated"
                    } else {
                        "deployed"
                    };

                    let deployment = Deployment {
                        config_hash: script_hash,
                        config: job_def.config.clone(),
                        status: status.to_string(),
                        applied_at: chrono::Utc::now().to_rfc3339(),
                        resources,
                        env: None,
                    };

                    storage
                        .write_job(
                            &diff.name,
                            &JobState {
                                job_name: diff.name.clone(),
                                project: manifest.project.clone(),
                                deployment,
                            },
                        )
                        .await?;

                    if matches!(&diff.diff_type, DiffType::Create) {
                        result.created.push(diff.name.clone());
                    } else {
                        result.modified.push(diff.name.clone());
                    }
                }
                DiffType::Delete => {
                    // Destroy via provider if configured and resources exist (skip in dry-run)
                    if !dry_run
                        && let Some(existing) = fresh_state.deployments.get(&diff.name)
                        && !existing.resources.is_empty()
                    {
                        let job_type = existing
                            .config
                            .get("type")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                anyhow!("Job '{}' state is missing a 'type' field", diff.name)
                            })?;

                        if let Some(provider_defaults) = manifest.providers.get(job_type) {
                            let merged_config =
                                build_provider_config(provider_defaults, &existing.config, job_type);
                            let provider =
                                providers::get_provider(job_type, &merged_config).await?;
                            provider.destroy(&diff.name, &existing.resources).await?;
                        }
                    }

                    storage.delete_job(&diff.name).await?;

                    let script_path = root_dir
                        .join(".yard/generated")
                        .join(format!("{}.py", diff.name));
                    if script_path.exists() {
                        let _ = std::fs::remove_file(script_path);
                    }

                    result.deleted.push(diff.name.clone());
                }
            }
        }

        // --- DAG phase: generate, diff, deploy DAG files ---
        let dags = airflow_dag::collect_dags(root_dir, manifest)?;
        if !dags.is_empty() {
            let dag_result =
                apply_dags(manifest, &dags, root_dir, dry_run, &storage).await?;
            result.dag_created = dag_result.created;
            result.dag_modified = dag_result.modified;
            result.dag_deleted = dag_result.deleted;
        } else {
            // No DAG dirs in the project — clean up any orphaned DAG state
            let dag_names = storage.list_dags().await?;
            for dag_name in dag_names {
                storage.delete_dag(&dag_name).await?;
                let dag_path = root_dir
                    .join(".yard/generated/dags")
                    .join(format!("{dag_name}.py"));
                if dag_path.exists() {
                    let _ = std::fs::remove_file(dag_path);
                }
                result.dag_deleted.push(dag_name);
            }
        }

        Ok(result)
    }
    .await;

    // Always unlock all jobs, even on error
    storage.unlock_jobs(&locks).await;

    apply_result
}

/// Prepare the state backend for use. Local: creates the state directory.
/// S3: runs a `head_bucket` to validate credentials and bucket existence.
pub async fn init_state_backend(
    backend: &yard_structs::StateBackend,
    aws_cfg: Option<&Value>,
) -> Result<()> {
    match backend {
        yard_structs::StateBackend::Local { path } => {
            tokio::fs::create_dir_all(path)
                .await
                .with_context(|| format!("Failed to create state dir {}", path.display()))?;
            println!("Initialized state at {}", path.display());
        }
        yard_structs::StateBackend::S3 { bucket, region, .. } => {
            let config = providers::aws_config(region, aws_cfg).await;
            let client = aws_sdk_s3::Client::new(&config);
            client
                .head_bucket()
                .bucket(bucket)
                .send()
                .await
                .with_context(|| format!("Failed to reach S3 bucket {bucket} in {region}"))?;
            println!("Verified S3 state bucket {bucket} ({region})");
        }
    }
    Ok(())
}

/// Force-unlock a job. Returns the LockInfo of the previous holder, or None if not locked.
pub async fn force_unlock(
    backend: &yard_structs::StateBackend,
    job_name: &str,
) -> Result<Option<yard_structs::LockInfo>> {
    let storage = storage::get_storage(backend).await?;
    let existing = storage.get_lock(job_name).await?;
    if existing.is_some() {
        storage.force_unlock(job_name).await?;
    }
    Ok(existing)
}

/// Result of destroying jobs and DAGs.
pub struct DestroyResult {
    pub destroyed: Vec<String>,
    pub dags_destroyed: Vec<String>,
}

/// Destroy a single job: tear down provider resources, delete state, delete generated script.
pub async fn destroy_job(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, Value>,
    job_name: &str,
    root_dir: &Path,
    dry_run: bool,
) -> Result<bool> {
    let storage = storage::get_storage(backend).await?;

    let job_state = match storage.read_job(job_name).await? {
        Some(s) => s,
        None => return Ok(false),
    };

    let lock = storage.lock(job_name).await?;

    let result: Result<()> = async {
        // Destroy provider resources if they exist
        if !dry_run && !job_state.deployment.resources.is_empty() {
            let job_type = job_state
                .deployment
                .config
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Job '{}' state is missing a 'type' field", job_name))?;

            if let Some(provider_defaults) = provider_configs.get(job_type) {
                let merged_config =
                    build_provider_config(provider_defaults, &job_state.deployment.config, job_type);
                let provider = providers::get_provider(job_type, &merged_config).await?;
                provider
                    .destroy(job_name, &job_state.deployment.resources)
                    .await?;
            }
        }

        // Delete state file
        storage.delete_job(job_name).await?;

        // Delete generated script
        let script_path = root_dir
            .join(".yard/generated")
            .join(format!("{job_name}.py"));
        if script_path.exists() {
            let _ = std::fs::remove_file(script_path);
        }

        Ok(())
    }
    .await;

    storage.unlock(job_name, &lock).await?;
    result?;

    Ok(true)
}

/// Destroy all jobs and DAGs that have state.
pub async fn destroy_all(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, Value>,
    aws: &Value,
    root_dir: &Path,
    dry_run: bool,
) -> Result<DestroyResult> {
    let storage = storage::get_storage(backend).await?;
    let job_names = storage.list_jobs().await?;
    let mut result = DestroyResult {
        destroyed: Vec::new(),
        dags_destroyed: Vec::new(),
    };

    for name in job_names {
        if destroy_job(backend, provider_configs, &name, root_dir, dry_run).await? {
            result.destroyed.push(name);
        }
    }

    // Also destroy all DAGs
    let dag_result = destroy_all_dags(backend, provider_configs, aws, root_dir, dry_run).await?;
    result.dags_destroyed = dag_result.destroyed;

    Ok(result)
}

// --- DAG lifecycle ---

/// Result of applying DAG changes.
#[derive(Debug)]
pub struct DagApplyResult {
    pub created: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
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
async fn apply_dags(
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

                // Upload to S3 if dags_bucket is configured and not dry-run
                let s3_uri = if !dry_run {
                    upload_dag_to_s3(manifest, dag, &content).await?
                } else {
                    None
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

    Ok(result)
}

/// Upload a generated DAG file to S3 using the resolved dags_bucket/dags_prefix.
/// Returns `Ok(Some(s3_uri))` on success, `Ok(None)` if no dags_bucket is configured.
async fn upload_dag_to_s3(
    manifest: &ProjectManifest,
    dag: &airflow_dag::ResolvedDag,
    content: &str,
) -> Result<Option<String>> {
    let Some(ref bucket) = dag.config.dags_bucket else {
        return Ok(None);
    };

    let region = extract_airflow_region(manifest)?;
    let prefix = dag
        .config
        .dags_prefix
        .as_deref()
        .unwrap_or("dags/");

    let aws_cfg = resolve_aws_for_dir(&manifest.aws, &dag.dir);
    let s3_ops = providers::S3ScriptOps {
        s3_client: aws_sdk_s3::Client::new(
            &providers::aws_config(&region, Some(&aws_cfg)).await,
        ),
        script_bucket: bucket.clone(),
        script_prefix: prefix.to_string(),
    };

    let uri = s3_ops.upload_script(&dag.name, content).await?;
    Ok(Some(uri))
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

/// Generate and return the Python content for a DAG without deploying.
pub fn show_dag(
    manifest: &ProjectManifest,
    dags: &[airflow_dag::ResolvedDag],
    dag_name: &str,
) -> Result<String> {
    let dag = dags
        .iter()
        .find(|d| d.name == dag_name)
        .ok_or_else(|| anyhow!("DAG \"{dag_name}\" not found"))?;

    airflow_dag::generate_dag(manifest, dag)
        .with_context(|| format!("Failed to generate DAG \"{dag_name}\""))
}

/// Generate and return the script for a job without deploying or modifying state.
pub fn show(manifest: &ProjectManifest, job_name: &str) -> Result<String> {
    let job_def = manifest
        .jobs
        .get(job_name)
        .ok_or_else(|| anyhow!("Job \"{job_name}\" not found in manifest"))?;

    codegen::generate_python_script(job_name, job_def)
        .with_context(|| format!("Failed to generate script for job \"{job_name}\""))
}

/// Extract optional body override from a job config.
pub fn parse_body(config: &Value) -> Option<String> {
    config
        .get("body")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Extract optional job_file path from a job config.
pub fn parse_job_file(config: &Value) -> Option<String> {
    config
        .get("job_file")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Parse an `airflow:` section body into an [`AirflowSection`]. The same
/// parser is used at every layer of the inheritance chain (yard.yaml
/// `providers.airflow`, `account.yaml` / `region.yaml` / `dag.yaml` airflow
/// keys, and the `overrides` nested under a job's `airflow:` block).
///
/// `value` is the object directly under the `airflow:` key (or the object
/// passed as `providers.airflow`). Unknown fields are ignored — forward
/// compatibility with future PR additions.
pub fn parse_airflow_section(value: &Value) -> AirflowSection {
    let retries = value
        .get("retries")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    AirflowSection {
        schedule: value
            .get("schedule")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        owner: value
            .get("owner")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        retries,
        dags_bucket: value
            .get("dags_bucket")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        dags_prefix: value
            .get("dags_prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    }
}

/// Parse the per-job `airflow:` block, if present. Returns `None` if the job
/// has no `airflow:` key. The block mixes task-level fields (`depends_on`)
/// with [`AirflowSection`] overrides (schedule, retries, etc.).
pub fn parse_airflow_job_block(config: &Value) -> Option<AirflowJobBlock> {
    let block = config.get("airflow")?;
    Some(AirflowJobBlock {
        depends_on: str_array_field(block, "depends_on"),
        overrides: parse_airflow_section(block),
    })
}

/// Shallow-merge two [`AirflowSection`]s: each `Some` field in `overlay`
/// overrides the corresponding field in `base`. Unset fields in `overlay`
/// leave `base` unchanged. Used to compose the inheritance chain
/// `yard.yaml → account → region → dag → job`.
pub fn merge_airflow_sections(base: &AirflowSection, overlay: &AirflowSection) -> AirflowSection {
    AirflowSection {
        schedule: overlay.schedule.clone().or_else(|| base.schedule.clone()),
        owner: overlay.owner.clone().or_else(|| base.owner.clone()),
        retries: overlay.retries.or(base.retries),
        dags_bucket: overlay
            .dags_bucket
            .clone()
            .or_else(|| base.dags_bucket.clone()),
        dags_prefix: overlay
            .dags_prefix
            .clone()
            .or_else(|| base.dags_prefix.clone()),
    }
}

/// Extract imports from a job config's "imports" array.
pub fn parse_partition_by(config: &Value) -> Vec<String> {
    config
        .get("partition_by")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_partition_timestamp_column(config: &Value) -> Option<String> {
    config
        .get("partition_timestamp_column")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub fn parse_create_timestamp(config: &Value) -> bool {
    config
        .get("create_timestamp")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

pub fn parse_imports(config: &Value) -> Vec<Import> {
    let mut imports = Vec::new();
    if let Some(arr) = config.get("imports").and_then(|v| v.as_array()) {
        for item in arr {
            let name = match item.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let from = item
                .get("from")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            imports.push(Import { name, from });
        }
    }
    imports
}

/// Helper to extract an optional string field from JSON.
fn str_field(obj: &Value, key: &str) -> Option<String> {
    obj.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// Helper to extract a string array field from JSON.
fn str_array_field(obj: &Value, key: &str) -> Vec<String> {
    obj.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Helper to extract a string->string map field from JSON.
/// Helper to extract an order_by field: array of {column: string, desc: bool} objects.
fn order_by_field(obj: &Value, key: &str) -> Vec<yard_structs::OrderBySpec> {
    obj.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let column = item.get("column").and_then(|v| v.as_str())?.to_string();
                    let desc = item.get("desc").and_then(|v| v.as_bool()).unwrap_or(false);
                    Some(yard_structs::OrderBySpec { column, desc })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn str_map_field(obj: &Value, key: &str) -> HashMap<String, String> {
    obj.get(key)
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_single_source(src: &Value, default_name: &str) -> Option<Source> {
    let headers = src
        .get("headers")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let options = src
        .get("options")
        .and_then(|v| v.as_object())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    Some(Source {
        name: str_field(src, "name").unwrap_or_else(|| default_name.to_string()),
        source_type: src.get("type")?.as_str()?.to_string(),
        format: str_field(src, "format"),
        path: str_field(src, "path"),
        connection_url: str_field(src, "connection_url"),
        table: str_field(src, "table"),
        database: str_field(src, "database"),
        secret_id: str_field(src, "secret_id"),
        engine: str_field(src, "engine"),
        connection_type: str_field(src, "connection_type"),
        topic: str_field(src, "topic"),
        url: str_field(src, "url"),
        headers,
        options,
    })
}

/// Extract sources from a job config. Supports both `source:` (single) and `sources:` (list).
pub fn parse_sources(config: &Value) -> Vec<Source> {
    // Try `sources:` (list) first
    if let Some(arr) = config.get("sources").and_then(|v| v.as_array()) {
        return arr
            .iter()
            .enumerate()
            .filter_map(|(i, item)| parse_single_source(item, &format!("source_{i}")))
            .collect();
    }
    // Fall back to `source:` (single)
    if let Some(src) = config.get("source")
        && let Some(parsed) = parse_single_source(src, "source")
    {
        return vec![parsed];
    }
    vec![]
}

/// Extract sink configuration from a job config.
pub fn parse_sink(config: &Value) -> Option<Sink> {
    let snk = config.get("sink")?;
    Some(Sink {
        source: str_field(snk, "source"),
        sink_type: snk.get("type")?.as_str()?.to_string(),
        format: str_field(snk, "format"),
        path: str_field(snk, "path"),
        connection_url: str_field(snk, "connection_url"),
        table: str_field(snk, "table"),
        database: str_field(snk, "database"),
        secret_id: str_field(snk, "secret_id"),
        mode: str_field(snk, "mode"),
        partition_by: str_array_field(snk, "partition_by"),
        fill_nulls: snk.get("fill_nulls").and_then(|v| v.as_bool()),
    })
}

/// Extract transforms from a job config.
pub fn parse_transforms(config: &Value) -> Vec<Transform> {
    let mut transforms = Vec::new();
    let Some(arr) = config.get("transforms").and_then(|v| v.as_array()) else {
        return transforms;
    };

    for item in arr {
        let Some(transform_type) = item.get("type").and_then(|v| v.as_str()) else {
            continue;
        };

        transforms.push(Transform {
            transform_type: transform_type.to_string(),
            source: str_field(item, "source"),
            output: str_field(item, "output"),
            condition: str_field(item, "condition"),
            query: str_field(item, "query"),
            columns: str_array_field(item, "columns"),
            mapping: str_map_field(item, "mapping"),
            name: str_field(item, "name"),
            expression: str_field(item, "expression"),
            left: str_field(item, "left"),
            right: str_field(item, "right"),
            on: str_field(item, "on"),
            how: str_field(item, "how"),
            group_by: str_array_field(item, "group_by"),
            aggs: str_map_field(item, "aggs"),
            partition_by: str_array_field(item, "partition_by"),
            order_by: order_by_field(item, "order_by"),
        });
    }

    transforms
}

fn compare_json(old: &Value, new: &Value) -> HashMap<String, (String, String)> {
    let mut changes = HashMap::new();
    if let (Value::Object(old_obj), Value::Object(new_obj)) = (old, new) {
        for (k, v) in new_obj {
            let old_val = old_obj.get(k).unwrap_or(&Value::Null);
            if old_val != v {
                changes.insert(k.clone(), (old_val.to_string(), v.to_string()));
            }
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use yard_structs::{JobDefinition, StateBackend};

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
        }
    }

    /// Compute the combined hash the same way calculate_diff does
    fn job_hash(name: &str, job: &JobDefinition) -> String {
        let script = crate::codegen::generate_python_script(name, job).unwrap();
        let config_str = serde_json::to_string(&job.config).unwrap_or_default();
        let combined = format!("{script}\n{config_str}");
        crate::utils::calculate_hash(&combined)
    }

    fn make_deployment(config_hash: &str, config: serde_json::Value) -> Deployment {
        Deployment {
            env: None,
            config_hash: config_hash.to_string(),
            config,
            status: "generated".to_string(),
            applied_at: "2025-01-01T00:00:00Z".to_string(),
            resources: Vec::new(),
        }
    }

    fn empty_state() -> ProjectState {
        ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::new(),
        }
    }

    #[test]
    fn diff_detects_create() {
        let job = make_job("glue", json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("new_job".to_string(), job)]),
            aws: serde_json::Value::Null,
        };

        let diffs = calculate_diff(&manifest, &empty_state()).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Create));
        assert_eq!(diffs[0].name, "new_job");
    }

    #[test]
    fn diff_detects_delete() {
        let config = json!({"type": "glue"});
        let hash = crate::utils::calculate_hash("some old script");
        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([("old_job".to_string(), make_deployment(&hash, config))]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: serde_json::Value::Null,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Delete));
        assert_eq!(diffs[0].name, "old_job");
    }

    #[test]
    fn diff_detects_no_change() {
        let job = make_job("glue", json!({"type": "glue", "script_name": "stable"}));
        let hash = job_hash("stable", &job);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "stable".to_string(),
                make_deployment(&hash, job.config.clone()),
            )]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("stable".to_string(), job)]),
            aws: serde_json::Value::Null,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn diff_detects_config_only_change() {
        // Same script, different config (e.g. worker_type changed)
        let old_config = json!({"type": "glue", "glue": {"worker_type": "G.1X"}});
        let new_config = json!({"type": "glue", "glue": {"worker_type": "G.2X"}});

        let old_job = make_job("glue", old_config.clone());
        let hash = job_hash("my_job", &old_job);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "my_job".to_string(),
                make_deployment(&hash, old_config),
            )]),
        };

        let new_job = make_job("glue", new_config);
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("my_job".to_string(), new_job)]),
            aws: serde_json::Value::Null,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[test]
    fn diff_detects_modify() {
        let old_config = json!({"type": "glue", "script_name": "v1"});
        let new_job = make_job("glue", json!({"type": "glue", "script_name": "v2"}));

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "my_job".to_string(),
                make_deployment("stale_hash", old_config),
            )]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("my_job".to_string(), new_job)]),
            aws: serde_json::Value::Null,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[test]
    fn diff_mixed_create_modify_delete() {
        let keep_job = make_job("glue", json!({"type": "glue", "script_name": "keep"}));
        let keep_hash = job_hash("keep", &keep_job);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([
                (
                    "keep".to_string(),
                    make_deployment(&keep_hash, keep_job.config.clone()),
                ),
                (
                    "to_delete".to_string(),
                    make_deployment("old", json!({"type": "glue"})),
                ),
                (
                    "to_modify".to_string(),
                    make_deployment("outdated", json!({"type": "glue", "v": "1"})),
                ),
            ]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([
                ("keep".to_string(), keep_job),
                (
                    "to_modify".to_string(),
                    make_job("glue", json!({"type": "glue", "v": "2"})),
                ),
                (
                    "new_job".to_string(),
                    make_job("glue", json!({"type": "glue"})),
                ),
            ]),
            aws: serde_json::Value::Null,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 3);

        let names: Vec<&str> = diffs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"to_delete"));
        assert!(names.contains(&"to_modify"));
        assert!(names.contains(&"new_job"));
    }

    #[tokio::test]
    async fn apply_creates_scripts_and_updates_state() {
        let dir = std::env::temp_dir().join(format!("yard_apply_{}", std::process::id()));
        let state_dir = dir.join(".yard/state");

        let job = make_job("glue", json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: state_dir.clone(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("new_job".to_string(), job)]),
            aws: serde_json::Value::Null,
        };

        let result = apply(&manifest, &empty_state(), &dir, true).await.unwrap();

        assert_eq!(result.created, vec!["new_job"]);
        assert!(result.modified.is_empty());
        assert!(result.deleted.is_empty());

        // Verify script was written
        let script_path = dir.join(".yard/generated/new_job.py");
        assert!(script_path.exists());

        // Verify per-job state was persisted
        let job_state_path = state_dir.join("new_job.json");
        assert!(job_state_path.exists());
        let job_state: yard_structs::JobState =
            serde_json::from_str(&std::fs::read_to_string(&job_state_path).unwrap()).unwrap();
        assert_eq!(job_state.job_name, "new_job");
        assert_eq!(job_state.deployment.status, "generated");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn destroy_job_removes_state_and_script() {
        let dir = std::env::temp_dir().join(format!("yard_destroy_{}", std::process::id()));
        let state_dir = dir.join(".yard/state");

        let job = make_job("glue", json!({"type": "glue", "script_name": "doomed"}));
        let backend = StateBackend::Local {
            path: state_dir.clone(),
        };
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: backend.clone(),
            providers: HashMap::new(),
            jobs: HashMap::from([("doomed".to_string(), job)]),
            aws: serde_json::Value::Null,
        };

        // Apply first to create state + script
        apply(&manifest, &empty_state(), &dir, true).await.unwrap();
        assert!(state_dir.join("doomed.json").exists());
        assert!(dir.join(".yard/generated/doomed.py").exists());

        // Destroy it
        let destroyed = destroy_job(&backend, &HashMap::new(), "doomed", &dir, true)
            .await
            .unwrap();
        assert!(destroyed);

        // State and script should be gone
        assert!(!state_dir.join("doomed.json").exists());
        assert!(!dir.join(".yard/generated/doomed.py").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn destroy_job_nonexistent_returns_false() {
        let dir = std::env::temp_dir().join(format!("yard_destroy_ne_{}", std::process::id()));
        let backend = StateBackend::Local {
            path: dir.join(".yard/state"),
        };

        let destroyed = destroy_job(&backend, &HashMap::new(), "nope", &dir, true)
            .await
            .unwrap();
        assert!(!destroyed);
    }

    #[tokio::test]
    async fn destroy_all_removes_everything() {
        let dir = std::env::temp_dir().join(format!("yard_destroy_all_{}", std::process::id()));
        let state_dir = dir.join(".yard/state");

        let backend = StateBackend::Local {
            path: state_dir.clone(),
        };
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: backend.clone(),
            providers: HashMap::new(),
            jobs: HashMap::from([
                (
                    "job_a".to_string(),
                    make_job("glue", json!({"type": "glue", "script_name": "a"})),
                ),
                (
                    "job_b".to_string(),
                    make_job("glue", json!({"type": "glue", "script_name": "b"})),
                ),
            ]),
            aws: serde_json::Value::Null,
        };

        // Apply both
        apply(&manifest, &empty_state(), &dir, true).await.unwrap();
        assert!(state_dir.join("job_a.json").exists());
        assert!(state_dir.join("job_b.json").exists());

        // Destroy all
        let result = destroy_all(&backend, &HashMap::new(), &Value::Null, &dir, true)
            .await
            .unwrap();
        let mut destroyed = result.destroyed.clone();
        destroyed.sort();
        assert_eq!(destroyed, vec!["job_a", "job_b"]);

        // Everything should be gone
        assert!(!state_dir.join("job_a.json").exists());
        assert!(!state_dir.join("job_b.json").exists());
        assert!(!dir.join(".yard/generated/job_a.py").exists());
        assert!(!dir.join(".yard/generated/job_b.py").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn apply_rejects_invalid_jobs() {
        let dir = std::env::temp_dir().join(format!("yard_invalid_{}", std::process::id()));
        let state_dir = dir.join(".yard/state");

        // Job with unsupported type — should fail validation
        let bad_job = make_job("spark_streaming", json!({"type": "spark_streaming"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: state_dir.clone(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("bad_job".to_string(), bad_job)]),
            aws: serde_json::Value::Null,
        };

        let result = apply(&manifest, &empty_state(), &dir, true).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Validation failed")
        );

        // No state or scripts should have been created
        assert!(!state_dir.exists());
        assert!(!dir.join(".yard/generated").exists());
    }

    #[test]
    fn merge_provider_config_job_overrides_defaults() {
        let defaults = json!({
            "script_bucket": "my-bucket",
            "worker_type": "G.1X",
            "number_of_workers": 2,
            "glue_version": "4.0"
        });
        let overrides = json!({
            "worker_type": "G.2X",
            "number_of_workers": 10,
            "timeout": 180
        });

        let merged = merge_provider_config(&defaults, &overrides);

        // Overrides win
        assert_eq!(merged["worker_type"], "G.2X");
        assert_eq!(merged["number_of_workers"], 10);
        assert_eq!(merged["timeout"], 180);
        // Defaults preserved
        assert_eq!(merged["script_bucket"], "my-bucket");
        assert_eq!(merged["glue_version"], "4.0");
    }

    #[test]
    fn merge_provider_config_no_overrides() {
        let defaults = json!({"worker_type": "G.1X", "number_of_workers": 2});
        let merged = merge_provider_config(&defaults, &Value::Null);
        assert_eq!(merged, defaults);
    }

    // --- is_task_only ---

    #[test]
    fn is_task_only_recognizes_bash() {
        assert!(is_task_only("bash"));
    }

    #[test]
    fn is_task_only_rejects_spark_types() {
        assert!(!is_task_only("glue"));
        assert!(!is_task_only("emr"));
        assert!(!is_task_only("unknown"));
    }

    // --- parse_airflow_section ---

    #[test]
    fn parse_airflow_section_all_fields() {
        let v = json!({
            "schedule": "@daily",
            "owner": "data-team",
            "retries": 3,
            "dags_bucket": "my-dags",
            "dags_prefix": "airflow/"
        });
        let s = parse_airflow_section(&v);
        assert_eq!(s.schedule.as_deref(), Some("@daily"));
        assert_eq!(s.owner.as_deref(), Some("data-team"));
        assert_eq!(s.retries, Some(3));
        assert_eq!(s.dags_bucket.as_deref(), Some("my-dags"));
        assert_eq!(s.dags_prefix.as_deref(), Some("airflow/"));
    }

    #[test]
    fn parse_airflow_section_empty_has_no_fields() {
        let s = parse_airflow_section(&json!({}));
        assert_eq!(s, AirflowSection::default());
    }

    #[test]
    fn parse_airflow_section_ignores_unknown_fields() {
        let s = parse_airflow_section(&json!({"schedule": "@hourly", "future_field": true}));
        assert_eq!(s.schedule.as_deref(), Some("@hourly"));
    }

    // --- parse_airflow_job_block ---

    #[test]
    fn parse_airflow_job_block_absent_returns_none() {
        let config = json!({"type": "glue"});
        assert!(parse_airflow_job_block(&config).is_none());
    }

    #[test]
    fn parse_airflow_job_block_with_depends_on_and_overrides() {
        let config = json!({
            "type": "glue",
            "airflow": {
                "depends_on": ["customers", "products"],
                "schedule": "@hourly",
                "retries": 5
            }
        });
        let block = parse_airflow_job_block(&config).expect("expected block");
        assert_eq!(block.depends_on, vec!["customers", "products"]);
        assert_eq!(block.overrides.schedule.as_deref(), Some("@hourly"));
        assert_eq!(block.overrides.retries, Some(5));
    }

    #[test]
    fn parse_airflow_job_block_depends_on_only() {
        let config = json!({"type": "glue", "airflow": {"depends_on": ["a"]}});
        let block = parse_airflow_job_block(&config).expect("expected block");
        assert_eq!(block.depends_on, vec!["a"]);
        assert_eq!(block.overrides, AirflowSection::default());
    }

    // --- merge_airflow_sections ---

    #[test]
    fn merge_airflow_overlay_wins() {
        let base = AirflowSection {
            schedule: Some("@daily".to_string()),
            owner: Some("base-owner".to_string()),
            retries: Some(2),
            ..Default::default()
        };
        let overlay = AirflowSection {
            schedule: Some("@hourly".to_string()),
            retries: None,
            ..Default::default()
        };
        let merged = merge_airflow_sections(&base, &overlay);
        assert_eq!(merged.schedule.as_deref(), Some("@hourly")); // overlay wins
        assert_eq!(merged.owner.as_deref(), Some("base-owner")); // fallback to base
        assert_eq!(merged.retries, Some(2)); // overlay None -> fallback
    }

    #[test]
    fn merge_airflow_empty_overlay_is_identity() {
        let base = AirflowSection {
            schedule: Some("@daily".to_string()),
            retries: Some(1),
            ..Default::default()
        };
        let merged = merge_airflow_sections(&base, &AirflowSection::default());
        assert_eq!(merged, base);
    }

    #[test]
    fn merge_airflow_chain_three_levels() {
        // Simulates yard.yaml -> region.yaml -> per-job override
        let project = AirflowSection {
            schedule: Some("@daily".to_string()),
            owner: Some("data".to_string()),
            retries: Some(1),
            dags_bucket: Some("proj-bucket".to_string()),
            ..Default::default()
        };
        let region = AirflowSection {
            retries: Some(3), // region overrides retries
            ..Default::default()
        };
        let job = AirflowSection {
            schedule: Some("0 */6 * * *".to_string()), // job overrides schedule
            ..Default::default()
        };
        let after_region = merge_airflow_sections(&project, &region);
        let final_cfg = merge_airflow_sections(&after_region, &job);
        assert_eq!(final_cfg.schedule.as_deref(), Some("0 */6 * * *"));
        assert_eq!(final_cfg.owner.as_deref(), Some("data"));
        assert_eq!(final_cfg.retries, Some(3));
        assert_eq!(final_cfg.dags_bucket.as_deref(), Some("proj-bucket"));
    }

    // --- DAG diff tests ---

    fn make_resolved_dag(name: &str, tasks: Vec<&str>) -> airflow_dag::ResolvedDag {
        use std::collections::BTreeMap;
        airflow_dag::ResolvedDag {
            name: name.to_string(),
            dir: std::path::PathBuf::from("/tmp/fake"),
            config: AirflowSection {
                schedule: Some("@daily".to_string()),
                ..Default::default()
            },
            tasks: tasks.iter().map(|s| s.to_string()).collect(),
            depends_on: BTreeMap::new(),
        }
    }

    fn make_dag_deployment(
        content_hash: &str,
        tasks: Vec<&str>,
    ) -> DagDeployment {
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
        // Need a manifest with the task so generate_dag works
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
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
            state: StateBackend::Local {
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
            state: StateBackend::Local {
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
            state: StateBackend::Local {
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
}
