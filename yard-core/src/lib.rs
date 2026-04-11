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
    Deployment, DiffType, Import, JobDiff, JobState, ProjectManifest, ProjectState,
    ResourceStatus, Sink, Source, Transform,
};

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

        // Merge provider defaults with job-level overrides (same logic as apply)
        let job_overrides = deployment
            .config
            .get(job_type)
            .unwrap_or(&Value::Null)
            .clone();
        let merged_config = merge_provider_config(provider_defaults, &job_overrides);

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

    let storage = storage::get_storage(&manifest.state).await?;

    // Preliminary diff to identify which jobs need locking
    let preliminary_diffs = calculate_diff(manifest, current_state)?;
    if preliminary_diffs.is_empty() {
        return Ok(ApplyResult {
            created: Vec::new(),
            modified: Vec::new(),
            deleted: Vec::new(),
        });
    }

    // Lock ALL affected jobs upfront — prevents concurrent applies from
    // modifying state between diff calculation and execution
    let job_names: Vec<String> = preliminary_diffs.iter().map(|d| d.name.clone()).collect();
    let locks = storage.lock_jobs(&job_names).await?;

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
                    let config_str = serde_json::to_string(&job_def.config)
                        .with_context(|| format!("Failed to serialize config for job \"{}\"", diff.name))?;
                    let combined = format!("{script_content}\n{config_str}");
                    let script_hash = utils::calculate_hash(&combined);

                    // Write generated script locally
                    let gen_dir = root_dir.join(".yard/generated");
                    std::fs::create_dir_all(&gen_dir)?;
                    let script_path = gen_dir.join(format!("{}.py", diff.name));
                    std::fs::write(&script_path, &script_content)?;

                    // Deploy via provider if configured (skip in dry-run mode)
                    let resources = if !dry_run {
                        if let Some(provider_defaults) = manifest.providers.get(&job_def.job_type) {
                            let job_overrides = job_def
                                .config
                                .get(&job_def.job_type)
                                .unwrap_or(&Value::Null)
                                .clone();
                            let merged_config =
                                merge_provider_config(provider_defaults, &job_overrides);

                            let provider =
                                providers::get_provider(&job_def.job_type, &merged_config).await?;
                            provider
                                .deploy(&diff.name, &script_content, &job_def.config)
                                .await?
                        } else {
                            Vec::new()
                        }
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
                            .ok_or_else(|| anyhow!("Job '{}' state is missing a 'type' field", diff.name))?;

                        if let Some(provider_defaults) = manifest.providers.get(job_type) {
                            let job_overrides = existing
                                .config
                                .get(job_type)
                                .unwrap_or(&Value::Null)
                                .clone();
                            let merged_config =
                                merge_provider_config(provider_defaults, &job_overrides);
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

        Ok(result)
    }
    .await;

    // Always unlock all jobs, even on error
    storage.unlock_jobs(&locks).await;

    apply_result
}

/// Initialize per-job state files. Skips jobs that already have state.
pub async fn init(manifest: &ProjectManifest) -> Result<()> {
    let storage = storage::get_storage(&manifest.state).await?;

    for (name, job_def) in &manifest.jobs {
        // Skip if state already exists for this job
        if storage.read_job(name).await?.is_some() {
            println!("State for job \"{name}\" already exists. Skipping.");
            continue;
        }

        let script_content = codegen::generate_python_script(name, job_def)
            .with_context(|| format!("Failed to generate script for job \"{name}\""))?;
        let config_str = serde_json::to_string(&job_def.config)
            .with_context(|| format!("Failed to serialize config for job \"{name}\""))?;
        let combined = format!("{script_content}\n{config_str}");
        let script_hash = utils::calculate_hash(&combined);

        let job_state = JobState {
            job_name: name.clone(),
            project: manifest.project.clone(),
            deployment: Deployment {
                env: Some("default".to_string()),
                config_hash: script_hash,
                config: job_def.config.clone(),
                status: "initialized".to_string(),
                applied_at: chrono::Utc::now().to_rfc3339(),
                resources: Vec::new(),
            },
        };

        storage.write_job(name, &job_state).await?;
        println!("Initialized state for job \"{name}\".");
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

/// Result of destroying jobs.
pub struct DestroyResult {
    pub destroyed: Vec<String>,
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
                let job_overrides = job_state
                    .deployment
                    .config
                    .get(job_type)
                    .unwrap_or(&Value::Null)
                    .clone();
                let merged_config = merge_provider_config(provider_defaults, &job_overrides);
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

/// Destroy all jobs that have state.
pub async fn destroy_all(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, Value>,
    root_dir: &Path,
    dry_run: bool,
) -> Result<DestroyResult> {
    let storage = storage::get_storage(backend).await?;
    let job_names = storage.list_jobs().await?;
    let mut result = DestroyResult {
        destroyed: Vec::new(),
    };

    for name in job_names {
        if destroy_job(backend, provider_configs, &name, root_dir, dry_run).await? {
            result.destroyed.push(name);
        }
    }

    Ok(result)
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

/// Extract imports from a job config's "imports" array.
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
    Some(Source {
        name: str_field(src, "name").unwrap_or_else(|| default_name.to_string()),
        source_type: src.get("type")?.as_str()?.to_string(),
        format: str_field(src, "format"),
        path: str_field(src, "path"),
        connection_url: str_field(src, "connection_url"),
        table: str_field(src, "table"),
        database: str_field(src, "database"),
        secret_id: str_field(src, "secret_id"),
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
            config,
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
        };

        // Apply both
        apply(&manifest, &empty_state(), &dir, true).await.unwrap();
        assert!(state_dir.join("job_a.json").exists());
        assert!(state_dir.join("job_b.json").exists());

        // Destroy all
        let result = destroy_all(&backend, &HashMap::new(), &dir, true)
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
}
