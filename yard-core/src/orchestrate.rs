use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use yard_structs::{
    Deployment, DiffType, JobState, ProjectManifest, ProjectState, ResourceStatus,
};

use crate::airflow_dag;
use crate::codegen;
use crate::providers;
use crate::storage;
use crate::utils;
use crate::validation;

use crate::config_merge::{build_provider_config, is_task_only};
use crate::diff::calculate_diff;
use crate::dag_lifecycle::{apply_dags, destroy_all_dags};

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

/// Result of applying changes.
#[derive(Debug)]
pub struct ApplyResult {
    pub created: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
    pub dag_created: Vec<String>,
    pub dag_modified: Vec<String>,
    pub dag_deleted: Vec<String>,
    /// Distinct cross-account Airflow connections required by created/modified
    /// DAGs. Operators must configure these in MWAA before the DAG runs.
    pub dag_required_connections: Vec<airflow_dag::RequiredConnection>,
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
    target: Option<String>,
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
            dag_required_connections: Vec::new(),
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
            result.dag_required_connections = dag_result.required_connections;
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

/// Validate the state backend is reachable. Local: creates the state directory
/// if it doesn't exist. S3: runs `head_bucket` to validate credentials and
/// bucket existence.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::{
        parse_airflow_job_block, parse_body, parse_imports, parse_job_file, parse_sink,
        parse_sources, parse_transforms,
    };
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
            base_name: String::new(),
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
                path: ".yard/state".into(),
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
                path: ".yard/state".into(),
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
                path: ".yard/state".into(),
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
                path: ".yard/state".into(),
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

        let result = apply(&manifest, &empty_state(), &dir, true, None).await.unwrap();

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
        apply(&manifest, &empty_state(), &dir, true, None).await.unwrap();
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
        apply(&manifest, &empty_state(), &dir, true, None).await.unwrap();
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

        let result = apply(&manifest, &empty_state(), &dir, true, None).await;
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
}
