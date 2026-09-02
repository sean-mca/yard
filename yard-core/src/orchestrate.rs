//! Top-level orchestration for apply, plan, and destroy commands.
//!
//! This module composes the data-flow pipeline (resolve, diff, validation)
//! with provider deployment and state persistence to implement the three
//! core CLI operations:
//!
//! - [`apply`] -- validate, diff, deploy changed jobs, persist state
//! - [`plan`] -- read-only diff preview (same validation, no side effects)
//! - [`destroy_job`] / [`destroy_all`] -- tear down resources, delete state
//!
//! All state mutations are protected by per-job locking via [`LockGuard`].
//! Locks are acquired upfront and released on exit (success or error), with
//! a TTL backstop for crash recovery.

use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;
use yard_structs::{
    Deployment, DeploymentStatus, DiffType, JobDiff, JobName, JobState, JobType,
    ProjectManifest, ProjectState, ResourceStatus,
};

use crate::providers;
use crate::storage;
use crate::storage::LockGuard;
use crate::utils;
use crate::validation;

use crate::config_merge::build_provider_config;
use crate::diff::calculate_diff;
use crate::plugin_host::PluginHostConfig;

/// Load the current project state by reading all per-job state files.
///
/// Errors (permissions, network, corrupt files) are propagated -- only
/// genuinely missing state is treated as "no deployments yet."
///
/// # Errors
///
/// Returns an error if the state backend cannot be initialized, if listing
/// jobs fails (permissions, connectivity), or if a state file is corrupt.
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
///
/// For each deployed job with resources and a known provider, instantiates the
/// provider and checks each resource. Returns a map of job_name to resource statuses.
/// Jobs without a matching provider config are silently skipped.
///
/// # Errors
///
/// Returns an error if a provider cannot be instantiated or if resource
/// verification fails for a job that has a valid provider config.
pub async fn verify_deployed_resources(
    manifest: &ProjectManifest,
    state: &ProjectState,
    plugin_host_config: &PluginHostConfig,
) -> Result<HashMap<String, Vec<ResourceStatus>>> {
    let mut results: HashMap<String, Vec<ResourceStatus>> = HashMap::new();

    for (job_name, deployment) in &state.deployments {
        if deployment.resources.is_empty() {
            continue;
        }

        // Determine the job type from the deployment config.
        let job_type: JobType = match deployment
            .config
            .get("type")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
        {
            Some(t) => t,
            None => continue,
        };

        // Need provider config from the manifest to instantiate the provider
        let provider_key = job_type.to_string();
        let provider_defaults = match manifest.providers.get(&provider_key) {
            Some(config) => config,
            None => continue,
        };

        let merged_config =
            build_provider_config(provider_defaults, &deployment.config, &provider_key);

        // Look up job_def for plugin_version/plugin_source
        let (plugin_version, plugin_source) = manifest
            .jobs
            .get(job_name)
            .map(|j| (j.plugin_version.as_deref(), j.plugin_source.as_deref()))
            .unwrap_or((None, None));

        let provider = providers::get_provider_for_job(
            &job_type,
            &merged_config,
            plugin_version,
            plugin_source,
            plugin_host_config,
        )
        .await?;
        let statuses = provider
            .verify_resources(job_name, &deployment.resources)
            .await?;

        results.insert(job_name.clone(), statuses);
    }

    Ok(results)
}

/// Result of applying changes to jobs.
///
/// Each field lists the names of jobs that were created, modified,
/// or deleted during the apply operation.
#[derive(Debug)]
pub struct ApplyResult {
    /// Job names that were newly deployed.
    pub created: Vec<String>,
    /// Job names whose configuration changed and were redeployed.
    pub modified: Vec<String>,
    /// Job names that were removed from the manifest and destroyed.
    pub deleted: Vec<String>,
}

/// Result of planning changes -- the already-filtered diff set.
#[derive(Debug)]
pub struct PlanResult {
    /// Per-job diffs (create, modify, or delete).
    pub job_diffs: Vec<JobDiff>,
}

/// Validate that `target` (if Some) matches a job in `manifest.jobs` by name.
/// Returns `Ok(())` when target is `None`.
///
/// Shared by `apply` and `plan` to guarantee an identical user-visible error
/// contract across both commands.
///
/// # Errors
///
/// Returns an error if the target name does not match any known job.
pub fn validate_target(
    manifest: &ProjectManifest,
    target: Option<&str>,
) -> Result<()> {
    if let Some(name) = target
        && !manifest.jobs.contains_key(name)
    {
        return Err(anyhow!(
            "target '{name}' not found -- no job with that name"
        ));
    }
    Ok(())
}

/// Apply changes: deploy via provider, update state.
///
/// `root_dir` is where `.yard/generated/` lives.
/// All affected jobs are locked upfront before diffing to prevent race conditions.
/// State is re-read under lock to ensure the diff is computed against fresh data.
/// All jobs are validated before any changes are made.
///
/// # Errors
///
/// Returns an error if validation fails, if locks cannot be acquired, if
/// provider deployment fails, or if state persistence fails.
pub async fn apply(
    manifest: &ProjectManifest,
    current_state: &ProjectState,
    root_dir: &Path,
    dry_run: bool,
    target: Option<String>,
) -> Result<ApplyResult> {
    // Construct plugin host config for plugin-aware provider dispatch.
    let plugin_host_config = PluginHostConfig {
        plugins_dir: root_dir.join(".yard/plugins"),
        lock_file_path: Some(root_dir.join("yard.lock")),
        ..Default::default()
    };

    // Validate all jobs up front (schema) -- abort before making any changes
    let mut all_errors: Vec<(String, Vec<yard_structs::ValidationError>)> = Vec::with_capacity(manifest.jobs.len());
    for (name, job_def) in &manifest.jobs {
        let errors = validation::validate_job_full_with_schema(name, job_def, None);
        if !errors.is_empty() {
            all_errors.push((name.clone(), errors));
        }
    }
    if !all_errors.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, errors) in &all_errors {
            for e in errors {
                let _ = writeln!(msg, "  [{}] {}: {}", name, e.field, e.message);
            }
        }
        return Err(anyhow!("{msg}"));
    }

    // Target validation: shared helper, identical contract for apply + plan.
    validate_target(manifest, target.as_deref())?;

    let storage = storage::get_storage(&manifest.state).await
        .context("failed to initialize storage for apply")?;

    // Preliminary diff to identify which jobs need locking
    let mut preliminary_diffs = calculate_diff(manifest, current_state, &plugin_host_config).await
        .context("failed to compute preliminary diff for apply")?;
    if let Some(ref name) = target {
        preliminary_diffs.retain(|d| &d.name == name);
    }

    // Lock ALL affected jobs upfront -- prevents concurrent applies from
    // modifying state between diff calculation and execution
    let job_names: Vec<String> = preliminary_diffs.iter().map(|d| d.name.clone()).collect();
    let locks = if !job_names.is_empty() {
        storage.lock_jobs(&job_names).await?
    } else {
        Vec::new()
    };
    let lock_guard = LockGuard::new(&storage, locks);

    // All work happens inside this block so we always unlock on exit
    let apply_result = async {
        // Re-read fresh state under lock -- the passed-in current_state may be stale.
        // Only read state for the locked jobs (the preliminary-diff set).
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

        // Scope manifest to locked jobs only so calculate_diff does not
        // classify unlocked jobs (missing from fresh_state) as Creates.
        let mut scoped_manifest = manifest.clone();
        scoped_manifest.jobs.retain(|name, _| job_names.contains(name));

        // Authoritative diff against fresh state
        let mut diffs = calculate_diff(&scoped_manifest, &fresh_state, &plugin_host_config).await?;
        if let Some(ref name) = target {
            diffs.retain(|d| &d.name == name);
        }

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

                    // Generate script via plugin codegen
                    let script_content = match providers::get_provider_for_job(
                        &job_def.job_type,
                        &job_def.config,
                        job_def.plugin_version.as_deref(),
                        job_def.plugin_source.as_deref(),
                        &plugin_host_config,
                    )
                    .await
                    {
                        Ok(provider) => provider
                            .codegen(&diff.name, &job_def.config)
                            .await
                            .unwrap_or(None)
                            .unwrap_or_default(),
                        Err(e) => {
                            eprintln!(
                                "Warning: plugin codegen failed for job \"{}\", deploying empty script: {e}",
                                diff.name
                            );
                            String::new()
                        }
                    };

                    let config_str = serde_json::to_string(&job_def.config).with_context(|| {
                        format!("Failed to serialize config for job \"{}\"", diff.name)
                    })?;
                    let trigger_str = match job_def
                        .airflow
                        .as_ref()
                        .and_then(|a| a.overrides.trigger.as_ref())
                    {
                        Some(t) => serde_json::to_string(t).with_context(|| {
                            format!("Failed to serialize trigger for job \"{}\"", diff.name)
                        })?,
                        None => String::new(),
                    };
                    let combined = format!("{script_content}\n{config_str}\n{trigger_str}");
                    let script_hash = utils::calculate_hash(&combined);

                    // Write generated script locally
                    let gen_dir = root_dir.join(".yard/generated");
                    tokio::fs::create_dir_all(&gen_dir)
                        .await
                        .context("failed to create .yard/generated directory")?;
                    let script_path = gen_dir.join(format!("{}.py", diff.name));
                    tokio::fs::write(&script_path, &script_content)
                        .await
                        .with_context(|| format!("failed to write generated script for job \"{}\"", diff.name))?;

                    // Deploy via provider if configured (skip in dry-run mode).
                    let resources = if dry_run {
                        Vec::new()
                    } else {
                        let provider_key = job_def.job_type.to_string();
                        if let Some(provider_defaults) =
                            manifest.providers.get(&provider_key)
                        {
                            let merged_config = build_provider_config(
                                provider_defaults,
                                &job_def.config,
                                &provider_key,
                            );
                            let provider = providers::get_provider_for_job(
                                &job_def.job_type,
                                &merged_config,
                                job_def.plugin_version.as_deref(),
                                job_def.plugin_source.as_deref(),
                                &plugin_host_config,
                            )
                            .await?;
                            provider
                                .deploy(&diff.name, &script_content, &job_def.config)
                                .await?
                        } else {
                            Vec::new()
                        }
                    };

                    let status = if resources.is_empty() {
                        DeploymentStatus::Generated
                    } else {
                        DeploymentStatus::Deployed
                    };

                    let deployment = Deployment {
                        config_hash: script_hash,
                        config: job_def.config.clone(),
                        status,
                        applied_at: chrono::Utc::now().to_rfc3339(),
                        resources,
                        env: None,
                        plugin_version: job_def.plugin_version.clone(),
                        plugin_source: job_def.plugin_source.clone(),
                    };

                    storage
                        .write_job(
                            &diff.name,
                            &JobState {
                                job_name: JobName::new(diff.name.clone()),
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
                        let job_type: JobType = existing
                            .config
                            .get("type")
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                anyhow!("Job '{}' state is missing a 'type' field", diff.name)
                            })?
                            .parse()
                            .with_context(|| {
                                format!("invalid job type in state for job '{}'", diff.name)
                            })?;

                        let provider_key = job_type.to_string();
                        if let Some(provider_defaults) = manifest.providers.get(&provider_key) {
                            let merged_config = build_provider_config(
                                provider_defaults,
                                &existing.config,
                                &provider_key,
                            );
                            let provider = providers::get_provider_for_job(
                                &job_type,
                                &merged_config,
                                existing.plugin_version.as_deref(),
                                existing.plugin_source.as_deref(),
                                &plugin_host_config,
                            )
                            .await?;
                            provider.destroy(&diff.name, &existing.resources).await?;
                        }
                    }

                    storage.delete_job(&diff.name).await
                        .with_context(|| format!("failed to delete state for job \"{}\"", diff.name))?;

                    let script_path = root_dir
                        .join(".yard/generated")
                        .join(format!("{}.py", diff.name));
                    if script_path.exists() {
                        let _ = tokio::fs::remove_file(script_path).await;
                    }

                    result.deleted.push(diff.name.clone());
                }
                _ => {}
            }
        }

        Ok(result)
    }
    .await;

    // Always release all locks, even on error.
    if let Err(e) = lock_guard.release().await {
        eprintln!("Warning: lock release failed during apply: {e}");
    }

    apply_result
}

/// Compute the filtered diff set for a project -- the read-only mirror of `apply`.
///
/// Returns already-filtered `job_diffs` per the target contract.
/// `target=None` returns the full diff set; `target=Some(name)` validates
/// `name` against jobs and filters the diff vec to that name.
///
/// # Errors
///
/// Returns an error if validation fails, diff computation fails, or if the
/// target name does not match any known job.
pub async fn plan(
    manifest: &ProjectManifest,
    current_state: &ProjectState,
    root_dir: &Path,
    target: Option<String>,
) -> Result<PlanResult> {
    // Construct plugin host config for diff computation.
    let plugin_host_config = PluginHostConfig {
        plugins_dir: root_dir.join(".yard/plugins"),
        lock_file_path: Some(root_dir.join("yard.lock")),
        ..Default::default()
    };

    // Target validation: shared helper, identical contract with apply.
    validate_target(manifest, target.as_deref())?;

    // Job diffs against the full manifest; filter on output.
    let mut job_diffs = calculate_diff(manifest, current_state, &plugin_host_config).await
        .context("failed to compute job diffs for plan")?;
    if let Some(ref name) = target {
        job_diffs.retain(|d| &d.name == name);
    }

    Ok(PlanResult { job_diffs })
}

/// Validate the state backend is reachable.
///
/// Local: creates the state directory if it doesn't exist.
/// S3: runs `head_bucket` to validate credentials and bucket existence.
///
/// # Errors
///
/// Returns an error if the local directory cannot be created, if the S3
/// bucket cannot be reached, or if the backend variant is unsupported.
pub async fn init_state_backend(
    backend: &yard_structs::StateBackend,
    aws_cfg: Option<&yard_structs::AwsCredentialConfig>,
) -> Result<()> {
    match backend {
        yard_structs::StateBackend::Local { path } => {
            tokio::fs::create_dir_all(path)
                .await
                .with_context(|| format!("Failed to create state dir {}", path.display()))?;
            println!("Initialized state at {}", path.display());
        }
        yard_structs::StateBackend::S3 { bucket, region, .. } => {
            let aws_value: Option<serde_json::Value> = aws_cfg
                .map(serde_json::to_value)
                .transpose()
                .context("Failed to serialize state-init AWS credentials to JSON")?;
            let config = providers::aws_config(region, aws_value.as_ref()).await;
            let client = aws_sdk_s3::Client::new(&config);
            client
                .head_bucket()
                .bucket(bucket)
                .send()
                .await
                .with_context(|| format!("Failed to reach S3 bucket {bucket} in {region}"))?;
            println!("Verified S3 state bucket {bucket} ({region})");
        }
        _ => anyhow::bail!("unsupported state backend variant"),
    }
    Ok(())
}

/// Force-unlock a job. Returns the `LockInfo` of the previous holder, or `None` if not locked.
///
/// # Errors
///
/// Returns an error if the state backend cannot be initialized or if
/// the lock file cannot be read or deleted.
pub async fn force_unlock(
    backend: &yard_structs::StateBackend,
    job_name: &str,
) -> Result<Option<yard_structs::LockInfo>> {
    let storage = storage::get_storage(backend).await
        .context("failed to initialize storage for force-unlock")?;
    let existing = storage.get_lock(job_name).await
        .with_context(|| format!("failed to read lock for job \"{job_name}\""))?;
    if existing.is_some() {
        storage.force_unlock(job_name).await
            .with_context(|| format!("failed to force-unlock job \"{job_name}\""))?;
    }
    Ok(existing)
}

/// Result of destroying jobs.
pub struct DestroyResult {
    /// Job names that were destroyed.
    pub destroyed: Vec<String>,
}

/// Destroy a single job: tear down provider resources, delete state, delete generated script.
///
/// Returns `true` if the job existed and was destroyed, `false` if no state was found.
///
/// # Errors
///
/// Returns an error if the state backend cannot be initialized, if locking
/// fails, if provider destruction fails, or if state deletion fails.
pub async fn destroy_job(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, serde_json::Value>,
    job_name: &str,
    root_dir: &Path,
    dry_run: bool,
) -> Result<bool> {
    let storage = storage::get_storage(backend).await
        .context("failed to initialize storage for destroy-job")?;

    let job_state = match storage.read_job(job_name).await
        .with_context(|| format!("failed to read state for job \"{job_name}\""))? {
        Some(s) => s,
        None => return Ok(false),
    };

    let lock = storage.lock(job_name).await
        .with_context(|| format!("failed to acquire lock for job \"{job_name}\""))?;
    let lock_guard = LockGuard::new(&storage, vec![(job_name.to_string(), lock)]);

    let plugin_host_config = PluginHostConfig {
        plugins_dir: root_dir.join(".yard/plugins"),
        lock_file_path: Some(root_dir.join("yard.lock")),
        ..Default::default()
    };

    let result: Result<()> = async {
        // Destroy provider resources if they exist
        if !dry_run && !job_state.deployment.resources.is_empty() {
            let job_type: JobType = job_state
                .deployment
                .config
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("Job '{}' state is missing a 'type' field", job_name))?
                .parse()
                .with_context(|| format!("invalid job type in state for job '{job_name}'"))?;

            let provider_key = job_type.to_string();
            if let Some(provider_defaults) = provider_configs.get(&provider_key) {
                let merged_config = build_provider_config(
                    provider_defaults,
                    &job_state.deployment.config,
                    &provider_key,
                );
                let provider = providers::get_provider_for_job(
                    &job_type,
                    &merged_config,
                    job_state.deployment.plugin_version.as_deref(),
                    job_state.deployment.plugin_source.as_deref(),
                    &plugin_host_config,
                )
                .await?;
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
            let _ = tokio::fs::remove_file(script_path).await;
        }

        Ok(())
    }
    .await;

    if let Err(e) = lock_guard.release().await {
        eprintln!("Warning: lock release failed during destroy_job: {e}");
    }
    result?;

    Ok(true)
}

/// Destroy all jobs that have state.
///
/// # Errors
///
/// Returns an error if any individual job destruction fails.
pub async fn destroy_all(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, serde_json::Value>,
    _aws: Option<&yard_structs::AwsCredentialConfig>,
    root_dir: &Path,
    dry_run: bool,
) -> Result<DestroyResult> {
    let storage = storage::get_storage(backend).await
        .context("failed to initialize storage for destroy-all")?;
    let job_names = storage.list_jobs().await
        .context("failed to list jobs for destroy-all")?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::parsing::{
        parse_airflow_job_block, parse_body, parse_imports, parse_job_file, parse_sink,
        parse_sources, parse_transforms,
    };
    use serde_json::json;
    use yard_structs::{JobDefinition, StateBackend};

    fn make_job(job_type: JobType, config: serde_json::Value) -> JobDefinition {
        let imports = parse_imports(&config);
        let body = parse_body(&config);
        let job_file = parse_job_file(&config);
        let sources = parse_sources(&config, "test").expect("test fixture must parse");
        let sink = parse_sink(&config, "test").expect("test fixture must parse");
        let transforms = parse_transforms(&config, "test").expect("test fixture must parse");
        let airflow = parse_airflow_job_block(&config, "test").expect("test fixture must parse");

        JobDefinition {
            job_type,
            imports,
            body,
            job_file,
            sources,
            sink,
            transforms,
            airflow,
            mask_pii: Vec::new(),
            partition_by: Vec::new(),
            partition_timestamp_column: None,
            create_timestamp: false,
            config,
            dir: std::path::PathBuf::new(),
            base_name: String::new(),
            plugin_version: None,
            plugin_source: None,
        }
    }

    fn make_deployment(config_hash: &str, config: serde_json::Value) -> Deployment {
        Deployment {
            env: None,
            config_hash: config_hash.to_string(),
            config,
            status: DeploymentStatus::Generated,
            applied_at: "2025-01-01T00:00:00Z".to_string(),
            resources: Vec::new(),
            plugin_version: None,
            plugin_source: None,
        }
    }

    fn empty_state() -> ProjectState {
        ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::new(),
        }
    }

    fn test_plugin_config() -> PluginHostConfig {
        PluginHostConfig {
            plugins_dir: std::path::PathBuf::from("/tmp/yard-test-plugins"),
            ..Default::default()
        }
    }

    /// Compute the combined hash the same way calculate_diff does (without codegen)
    fn job_hash_no_codegen(name: &str, job: &JobDefinition) -> String {
        // In v2.0, without a plugin binary available, codegen returns empty string
        let script = String::new();
        let config_str = serde_json::to_string(&job.config).unwrap_or_default();
        let trigger_str = job
            .airflow
            .as_ref()
            .and_then(|a| a.overrides.trigger.as_ref())
            .map(|t| serde_json::to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let combined = format!("{script}\n{config_str}\n{trigger_str}");
        crate::utils::calculate_hash(&combined)
    }

    #[tokio::test]
    async fn diff_detects_create() {
        let job = make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("new_job".to_string(), job)]),
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &empty_state(), &test_plugin_config()).await.unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Create));
        assert_eq!(diffs[0].name, "new_job");
    }

    #[tokio::test]
    async fn diff_detects_delete() {
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
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state, &test_plugin_config()).await.unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Delete));
        assert_eq!(diffs[0].name, "old_job");
    }

    #[tokio::test]
    async fn diff_detects_no_change() {
        let job = make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "stable"}));
        let hash = job_hash_no_codegen("stable", &job);

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
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state, &test_plugin_config()).await.unwrap();
        assert!(diffs.is_empty());
    }

    #[tokio::test]
    async fn diff_detects_config_only_change() {
        let old_config = json!({"type": "glue", "glue": {"worker_type": "G.1X"}});
        let new_config = json!({"type": "glue", "glue": {"worker_type": "G.2X"}});

        let old_job = make_job(JobType::Plugin("glue".to_string()), old_config.clone());
        let hash = job_hash_no_codegen("my_job", &old_job);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "my_job".to_string(),
                make_deployment(&hash, old_config),
            )]),
        };

        let new_job = make_job(JobType::Plugin("glue".to_string()), new_config);
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("my_job".to_string(), new_job)]),
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state, &test_plugin_config()).await.unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[tokio::test]
    async fn diff_detects_modify() {
        let old_config = json!({"type": "glue", "script_name": "v1"});
        let new_job = make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "v2"}));

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
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state, &test_plugin_config()).await.unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[tokio::test]
    async fn diff_mixed_create_modify_delete() {
        let keep_job = make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "keep"}));
        let keep_hash = job_hash_no_codegen("keep", &keep_job);

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
                    make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "v": "2"})),
                ),
                (
                    "new_job".to_string(),
                    make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue"})),
                ),
            ]),
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state, &test_plugin_config()).await.unwrap();
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

        let job = make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: state_dir.clone(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("new_job".to_string(), job)]),
            aws: None,
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
        assert_eq!(job_state.job_name.as_str(), "new_job");
        assert_eq!(job_state.deployment.status, DeploymentStatus::Generated);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn destroy_job_removes_state_and_script() {
        let dir = std::env::temp_dir().join(format!("yard_destroy_{}", std::process::id()));
        let state_dir = dir.join(".yard/state");

        let job = make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "doomed"}));
        let backend = StateBackend::Local {
            path: state_dir.clone(),
        };
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: backend.clone(),
            providers: HashMap::new(),
            jobs: HashMap::from([("doomed".to_string(), job)]),
            aws: None,
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
                    make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "a"})),
                ),
                (
                    "job_b".to_string(),
                    make_job(JobType::Plugin("glue".to_string()), json!({"type": "glue", "script_name": "b"})),
                ),
            ]),
            aws: None,
        };

        // Apply both
        apply(&manifest, &empty_state(), &dir, true, None).await.unwrap();
        assert!(state_dir.join("job_a.json").exists());
        assert!(state_dir.join("job_b.json").exists());

        // Destroy all
        let result = destroy_all(&backend, &HashMap::new(), None, &dir, true)
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
}
