//! Top-level orchestration for apply, plan, and destroy commands.
//!
//! This module composes the data-flow pipeline (resolve, diff, codegen,
//! validation) with provider deployment and state persistence to implement
//! the three core CLI operations:
//!
//! - [`apply`] — validate, diff, deploy changed jobs/DAGs, persist state
//! - [`plan`] — read-only diff preview (same validation, no side effects)
//! - [`destroy_job`] / [`destroy_all`] — tear down resources, delete state
//!
//! All state mutations are protected by per-job locking via [`LockGuard`].
//! Locks are acquired upfront and released on exit (success or error), with
//! a TTL backstop for crash recovery.

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;
use yard_structs::{
    DagDiff, Deployment, DeploymentStatus, DiffType, JobDiff, JobName, JobState, JobType,
    ProjectManifest, ProjectState, ResourceStatus, SchemaResponse,
};

use crate::airflow_dag;
use crate::codegen;
use crate::providers;
use crate::storage;
use crate::storage::LockGuard;
use crate::utils;
use crate::validation;

use crate::config_merge::{build_provider_config, is_task_only};
use crate::diff::calculate_diff;
use crate::dag_lifecycle::{apply_dags, destroy_all_dags};

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
) -> Result<HashMap<String, Vec<ResourceStatus>>> {
    let mut results: HashMap<String, Vec<ResourceStatus>> = HashMap::new();

    for (job_name, deployment) in &state.deployments {
        if deployment.resources.is_empty() {
            continue;
        }

        // Determine the job type from the deployment config. Unparseable or
        // missing values silently skip — drift verification is best-effort.
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
        let provider = providers::get_provider(job_type, &merged_config).await?;
        let statuses = provider
            .verify_resources(job_name, &deployment.resources)
            .await?;

        results.insert(job_name.clone(), statuses);
    }

    Ok(results)
}

/// Result of applying changes to jobs and DAGs.
///
/// Each field lists the names of jobs (or DAGs) that were created, modified,
/// or deleted during the apply operation.
#[derive(Debug)]
pub struct ApplyResult {
    /// Job names that were newly deployed.
    pub created: Vec<String>,
    /// Job names whose configuration changed and were redeployed.
    pub modified: Vec<String>,
    /// Job names that were removed from the manifest and destroyed.
    pub deleted: Vec<String>,
    /// DAG names that were newly generated/deployed.
    pub dag_created: Vec<String>,
    /// DAG names whose content changed and were regenerated/redeployed.
    pub dag_modified: Vec<String>,
    /// DAG names that were removed and destroyed.
    pub dag_deleted: Vec<String>,
    /// Distinct cross-account Airflow connections required by created/modified
    /// DAGs. Operators must configure these in MWAA before the DAG runs.
    pub dag_required_connections: Vec<airflow_dag::RequiredConnection>,
}

/// Result of planning changes -- the already-filtered diff set.
///
/// Mirrors `ApplyResult` for grep parity; minimal two-field shape per D-06
/// of Phase 13 (extend only when a concrete consumer needs more).
#[derive(Debug)]
pub struct PlanResult {
    /// Per-job diffs (create, modify, or delete).
    pub job_diffs: Vec<JobDiff>,
    /// Per-DAG diffs (create, modify, or delete).
    pub dag_diffs: Vec<DagDiff>,
}

/// Validate that `target` (if Some) matches either a job in `manifest.jobs`
/// or a DAG in `pre_dags` by name. Returns `Ok(())` when target is `None`.
///
/// Shared by `apply` and `plan` to guarantee an identical user-visible error
/// contract across both commands (D-01 of Phase 13; mirrors Phase 12 D-01/D-02).
///
/// # Errors
///
/// Returns an error if the target name does not match any known job or DAG.
pub fn validate_target(
    manifest: &ProjectManifest,
    pre_dags: &[airflow_dag::ResolvedDag],
    target: Option<&str>,
) -> Result<()> {
    if let Some(name) = target {
        let is_job = manifest.jobs.contains_key(name);
        let is_dag = pre_dags.iter().any(|d| d.name == name);
        if !is_job && !is_dag {
            return Err(anyhow!(
                "target '{name}' not found — no job or DAG with that name"
            ));
        }
    }
    Ok(())
}

/// Apply changes: generate scripts, deploy via provider, update state.
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
    // Build schema cache from built-in providers (D-06). Populated once per
    // provider type, then passed through the validation pipeline.
    let mut schema_cache: HashMap<String, SchemaResponse> = HashMap::new();
    for job_def in manifest.jobs.values() {
        let key = job_def.job_type.to_string();
        if !schema_cache.contains_key(&key) {
            if let Some(schema) = providers::built_in_schema(job_def.job_type) {
                schema_cache.insert(key, schema);
            }
        }
    }

    // Validate all jobs up front (schema + syntax) — abort before making any changes
    let mut all_errors: Vec<(String, Vec<yard_structs::ValidationError>)> = Vec::with_capacity(manifest.jobs.len());
    for (name, job_def) in &manifest.jobs {
        let schema = schema_cache.get(&job_def.job_type.to_string());
        let errors = validation::validate_job_full_with_schema(name, job_def, schema);
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

    // Validate orphan airflow blocks (airflow: on jobs outside any DAG dir)
    let pre_dags = airflow_dag::collect_dags(root_dir, manifest)
        .context("failed to collect DAGs for apply")?;
    let orphans = airflow_dag::validate_orphan_airflow_blocks(manifest, &pre_dags);
    if !orphans.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, err) in &orphans {
            let _ = writeln!(msg, "  [{name}] {err}");
        }
        return Err(anyhow!("{msg}"));
    }

    // Validate trigger config across all DAGs (TRIG-04..TRIG-07).
    // Errors accumulate per DAG and roll up into a single anyhow::Error before any codegen.
    let mut all_dag_errors: Vec<(String, Vec<yard_structs::ValidationError>)> = Vec::new();
    for dag in &pre_dags {
        let errors = validation::validate_dag_full(dag);
        if !errors.is_empty() {
            all_dag_errors.push((dag.name.clone(), errors));
        }
    }
    if !all_dag_errors.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, errors) in &all_dag_errors {
            for e in errors {
                let _ = writeln!(msg, "  [{}] {}: {}", name, e.field, e.message);
            }
        }
        return Err(anyhow!("{msg}"));
    }

    // PUB-03: cross-DAG broken-link soft warning (D-07/D-08).
    // Never returns Err — emits one stderr line per (DAG, missing-URI) pair.
    for w in validation::validate_project(&pre_dags) {
        eprintln!("{w}");
    }

    // Target validation: shared helper, identical contract for apply + plan (D-02).
    validate_target(manifest, &pre_dags, target.as_deref())?;

    let storage = storage::get_storage(&manifest.state).await
        .context("failed to initialize storage for apply")?;

    // Preliminary diff to identify which jobs need locking
    let mut preliminary_diffs = calculate_diff(manifest, current_state)
        .context("failed to compute preliminary diff for apply")?;
    if let Some(ref name) = target {
        preliminary_diffs.retain(|d| &d.name == name);
    }

    // Lock ALL affected jobs upfront — prevents concurrent applies from
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

        // Authoritative diff against fresh state (full manifest preserved per TGT-03; filter runs on output).
        let mut diffs = calculate_diff(manifest, &fresh_state)?;
        if let Some(ref name) = target {
            diffs.retain(|d| &d.name == name);
        }

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
                    // Task-only job types (bash, ...) have no Provider impl and
                    // skip straight to state bookkeeping.
                    let resources = if dry_run || is_task_only(job_def.job_type) {
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
                            let provider =
                                providers::get_provider(job_def.job_type, &merged_config).await?;
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
                            let provider =
                                providers::get_provider(job_type, &merged_config).await?;
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

        // --- DAG phase: generate, diff, deploy DAG files ---
        // Skip entirely when target is a job name (D-09): narrow-scope applies
        // must not touch DAG state for DAGs the operator didn't mention.
        let target_is_job_only = match &target {
            Some(name) => manifest.jobs.contains_key(name)
                && !pre_dags.iter().any(|d| &d.name == name),
            None => false,
        };
        if !target_is_job_only {
            let mut dags = airflow_dag::collect_dags(root_dir, manifest)?;
            // D-04: when target names a DAG (not a job), deploy only that DAG.
            // Unrelated DAGs must not be diffed, deployed, or written to state.
            if let Some(name) = &target {
                dags.retain(|d| &d.name == name);
            }
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
                        let _ = tokio::fs::remove_file(dag_path).await;
                    }
                    result.dag_deleted.push(dag_name);
                }
            }
        }

        Ok(result)
    }
    .await;

    // Always release all locks, even on error.
    // Lock release is best-effort — TTL backstop (D-02) covers failures.
    if let Err(e) = lock_guard.release().await {
        eprintln!("Warning: lock release failed during apply: {e}");
    }

    apply_result
}

/// Compute the filtered diff set for a project -- the read-only mirror of `apply`.
///
/// Returns already-filtered `job_diffs` and `dag_diffs` per the target contract.
/// `target=None` returns the full diff set; `target=Some(name)` validates `name`
/// against jobs+DAGs (D-04) and filters both diff vecs to that name (D-07/D-08 of Phase 13).
///
/// # Errors
///
/// Returns an error if validation fails, if DAG collection or diff computation
/// fails, or if the target name does not match any known job or DAG.
pub async fn plan(
    manifest: &ProjectManifest,
    current_state: &ProjectState,
    root_dir: &Path,
    target: Option<String>,
) -> Result<PlanResult> {
    // Resolve DAGs from the full manifest (D-10 / TGT-03 invariant — unchanged manifest).
    let pre_dags = airflow_dag::collect_dags(root_dir, manifest)
        .context("failed to collect DAGs for plan")?;

    // Orphan-airflow structural validation runs on the full manifest (D-11(c) disposition: KEEP).
    let orphans = airflow_dag::validate_orphan_airflow_blocks(manifest, &pre_dags);
    if !orphans.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, err) in &orphans {
            let _ = writeln!(msg, "  [{name}] {err}");
        }
        return Err(anyhow!("{msg}"));
    }

    // Validate trigger config across all DAGs (TRIG-04..TRIG-07).
    // Errors accumulate per DAG and roll up into a single anyhow::Error before any codegen.
    let mut all_dag_errors: Vec<(String, Vec<yard_structs::ValidationError>)> = Vec::new();
    for dag in &pre_dags {
        let errors = validation::validate_dag_full(dag);
        if !errors.is_empty() {
            all_dag_errors.push((dag.name.clone(), errors));
        }
    }
    if !all_dag_errors.is_empty() {
        let mut msg = String::from("Validation failed:\n");
        for (name, errors) in &all_dag_errors {
            for e in errors {
                let _ = writeln!(msg, "  [{}] {}: {}", name, e.field, e.message);
            }
        }
        return Err(anyhow!("{msg}"));
    }

    // PUB-03: cross-DAG broken-link soft warning (D-07/D-08).
    // Never returns Err — emits one stderr line per (DAG, missing-URI) pair.
    for w in validation::validate_project(&pre_dags) {
        eprintln!("{w}");
    }

    // Target validation: shared helper, identical contract with apply (D-01 / D-04).
    validate_target(manifest, &pre_dags, target.as_deref())?;

    // Job diffs against the full manifest; filter on output.
    let mut job_diffs = calculate_diff(manifest, current_state)
        .context("failed to compute job diffs for plan")?;
    if let Some(ref name) = target {
        job_diffs.retain(|d| &d.name == name);
    }

    // DAG diffs against the full manifest; filter on output.
    let dag_state = crate::dag_lifecycle::load_dag_state(&manifest.state).await
        .context("failed to load DAG state for plan")?;

    // Pre-load JobStates so the renderer (via calculate_dag_diffs ->
    // generate_dag) can read each Glue task's persisted script_location
    // per DAG-02. Mirrors apply_dags' bulk-load.
    let script_locations = crate::dag_lifecycle::load_script_locations(&manifest.state).await
        .context("failed to load script locations for plan")?;

    let mut dag_diffs = crate::dag_lifecycle::calculate_dag_diffs(
        manifest,
        &pre_dags,
        &dag_state,
        &script_locations,
    )?;
    if let Some(ref name) = target {
        dag_diffs.retain(|d| &d.name == name);
    }

    Ok(PlanResult { job_diffs, dag_diffs })
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
            // The providers::aws_config boundary stays Value-typed (D-14);
            // convert the typed credentials at the call site.
            let aws_value: Option<Value> = aws_cfg
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

/// Result of destroying jobs and DAGs.
pub struct DestroyResult {
    /// Job names that were destroyed.
    pub destroyed: Vec<String>,
    /// DAG names that were destroyed.
    pub dags_destroyed: Vec<String>,
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
    provider_configs: &HashMap<String, Value>,
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
            let _ = tokio::fs::remove_file(script_path).await;
        }

        Ok(())
    }
    .await;

    // Lock release is best-effort — TTL backstop (D-02) covers failures.
    if let Err(e) = lock_guard.release().await {
        eprintln!("Warning: lock release failed during destroy_job: {e}");
    }
    result?;

    Ok(true)
}

/// Destroy all jobs and DAGs that have state.
///
/// # Errors
///
/// Returns an error if any individual job or DAG destruction fails.
pub async fn destroy_all(
    backend: &yard_structs::StateBackend,
    provider_configs: &HashMap<String, Value>,
    aws: Option<&yard_structs::AwsCredentialConfig>,
    root_dir: &Path,
    dry_run: bool,
) -> Result<DestroyResult> {
    let storage = storage::get_storage(backend).await
        .context("failed to initialize storage for destroy-all")?;
    let job_names = storage.list_jobs().await
        .context("failed to list jobs for destroy-all")?;
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
            mask_pii: Vec::new(),
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
        let trigger_str = job
            .airflow
            .as_ref()
            .and_then(|a| a.overrides.trigger.as_ref())
            .map(|t| serde_json::to_string(t).unwrap_or_default())
            .unwrap_or_default();
        let combined = format!("{script}\n{config_str}\n{trigger_str}");
        crate::utils::calculate_hash(&combined)
    }

    fn make_deployment(config_hash: &str, config: serde_json::Value) -> Deployment {
        Deployment {
            env: None,
            config_hash: config_hash.to_string(),
            config,
            status: DeploymentStatus::Generated,
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
        let job = make_job(JobType::Glue, json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("new_job".to_string(), job)]),
            aws: None,
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
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Delete));
        assert_eq!(diffs[0].name, "old_job");
    }

    #[test]
    fn diff_detects_no_change() {
        let job = make_job(JobType::Glue, json!({"type": "glue", "script_name": "stable"}));
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
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert!(diffs.is_empty());
    }

    #[test]
    fn diff_detects_config_only_change() {
        // Same script, different config (e.g. worker_type changed)
        let old_config = json!({"type": "glue", "glue": {"worker_type": "G.1X"}});
        let new_config = json!({"type": "glue", "glue": {"worker_type": "G.2X"}});

        let old_job = make_job(JobType::Glue, old_config.clone());
        let hash = job_hash("my_job", &old_job);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "my_job".to_string(),
                make_deployment(&hash, old_config),
            )]),
        };

        let new_job = make_job(JobType::Glue, new_config);
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: ".yard/state".into(),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("my_job".to_string(), new_job)]),
            aws: None,
        };

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[test]
    fn diff_detects_modify() {
        let old_config = json!({"type": "glue", "script_name": "v1"});
        let new_job = make_job(JobType::Glue, json!({"type": "glue", "script_name": "v2"}));

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

        let diffs = calculate_diff(&manifest, &state).unwrap();
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[test]
    fn diff_mixed_create_modify_delete() {
        let keep_job = make_job(JobType::Glue, json!({"type": "glue", "script_name": "keep"}));
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
                    make_job(JobType::Glue, json!({"type": "glue", "v": "2"})),
                ),
                (
                    "new_job".to_string(),
                    make_job(JobType::Glue, json!({"type": "glue"})),
                ),
            ]),
            aws: None,
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

        let job = make_job(JobType::Glue, json!({"type": "glue", "script_name": "new_job"}));
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

        let job = make_job(JobType::Glue, json!({"type": "glue", "script_name": "doomed"}));
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
                    make_job(JobType::Glue, json!({"type": "glue", "script_name": "a"})),
                ),
                (
                    "job_b".to_string(),
                    make_job(JobType::Glue, json!({"type": "glue", "script_name": "b"})),
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

    // Test `apply_rejects_invalid_jobs` was deleted in Phase 21 plan 21-01:
    // Unknown job-type wire strings (e.g. "spark_streaming") are now rejected
    // by serde at deserialize time via JobType's `unknown variant` error,
    // which is structurally upstream of `apply` and cannot be exercised by
    // constructing a JobDefinition directly. The behavior is covered by
    // `yard-structs::config::tests::job_type_deserialize_unknown_rejects`.

    // --- validate_dag_full integration tests (Phase 29 — TRIG-07 wiring) ---

    /// Build a minimal on-disk yard project with one DAG dir holding a single
    /// bash task. `dag_yaml_body` becomes the contents of `<root>/<dag_name>/dag.yaml`.
    /// Returns (root_dir, manifest). Caller is responsible for cleanup via
    /// `std::fs::remove_dir_all(&root_dir)`.
    ///
    /// The on-disk layout matches the airflow_dag-module test fixtures
    /// (`yard.yaml` + `account.yaml` + `region.yaml` ancestry needed by
    /// `resolve::load_context`). Bash task is chosen so `validate_job_full`
    /// passes cleanly — the trigger gate fires AFTER job validation.
    fn build_dag_project_fixture(
        slug: &str,
        dag_name: &str,
        dag_yaml_body: &str,
    ) -> (std::path::PathBuf, ProjectManifest) {
        let root = std::env::temp_dir().join(format!(
            "yard_validate_dag_full_{}_{}",
            std::process::id(),
            slug
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("yard.yaml"), "project: test\n").unwrap();
        std::fs::write(root.join("account.yaml"), "account:\n  id: \"123\"\n").unwrap();
        std::fs::write(root.join("region.yaml"), "region:\n  id: us-east-1\n").unwrap();

        let dag_dir = root.join(dag_name);
        std::fs::create_dir_all(&dag_dir).unwrap();
        std::fs::write(dag_dir.join("dag.yaml"), dag_yaml_body).unwrap();

        let bash = JobDefinition {
            job_type: JobType::Bash,
            config: json!({"type": "bash", "command": "echo hi"}),
            dir: dag_dir.clone(),
            ..Default::default()
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
                path: root.join(".yard/state"),
            },
            providers: HashMap::new(),
            jobs: HashMap::from([("runit".to_string(), bash)]),
            aws: None,
        };

        (root, manifest)
    }

    #[tokio::test]
    async fn plan_rejects_dag_with_schedule_and_trigger() {
        // dag.yaml carries BOTH top-level `schedule:` AND a `trigger:` block.
        // The cascade resolver lifts both into the resolved AirflowSection
        // (parsing.rs:279-297 preserves them independently), so TRIG-04 fires.
        let (root, manifest) = build_dag_project_fixture(
            "plan_sched_and_trig",
            "pipeline",
            "schedule: \"@daily\"\ntrigger:\n  dataset:\n    uri: \"s3://bucket/key\"\n",
        );

        let result = plan(&manifest, &empty_state(), &root, None).await;
        let _ = std::fs::remove_dir_all(&root);

        let err = result.expect_err("plan must reject dag with schedule + trigger").to_string();
        assert!(err.contains("Validation failed:"), "got: {err}");
        // The DAG name is project-prefixed + sanitized: `test_pipeline`.
        assert!(err.contains("[test_pipeline]"), "got: {err}");
        assert!(err.contains("airflow.trigger:"), "got: {err}");
        assert!(err.contains("mutually exclusive"), "got: {err}");
    }

    #[tokio::test]
    async fn apply_rejects_dag_with_empty_any() {
        // dag.yaml has `trigger: { any: [] }` — TRIG-05b fires.
        // Apply runs with `dry_run: true` so no AWS provider invocation.
        let (root, manifest) = build_dag_project_fixture(
            "apply_empty_any",
            "pipeline",
            "trigger:\n  any: []\n",
        );

        let result = apply(&manifest, &empty_state(), &root, true, None).await;
        let _ = std::fs::remove_dir_all(&root);

        let err = result.expect_err("apply must reject dag with empty any:").to_string();
        assert!(err.contains("Validation failed:"), "got: {err}");
        assert!(err.contains("[test_pipeline]"), "got: {err}");
        assert!(err.contains("airflow.trigger.any:"), "got: {err}");
        assert!(err.contains("empty 'any: []' composite"), "got: {err}");
    }

    // --- Phase 32 plan 32-02: validate_project soft-warning rollup wiring ---

    #[tokio::test]
    async fn validate_project_runs_in_plan_path_and_does_not_fail() {
        // DAG triggers on a Dataset URI no DAG in the project publishes —
        // PUB-03 emits a warning to stderr, but plan must still return Ok.
        // (Stderr capture not asserted; the contract under test is "no Err".)
        let (root, manifest) = build_dag_project_fixture(
            "plan_broken_link",
            "pipeline",
            "trigger:\n  dataset:\n    uri: \"s3://nobody/publishes/this\"\n",
        );

        let result = plan(&manifest, &empty_state(), &root, None).await;
        let _ = std::fs::remove_dir_all(&root);

        assert!(
            result.is_ok(),
            "plan must NOT fail on cross-DAG broken Dataset link: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn validate_project_runs_in_apply_path_and_does_not_fail() {
        // Symmetric to the plan-path test: apply with dry_run=true MUST also
        // return Ok in the presence of a broken cross-DAG Dataset link.
        let (root, manifest) = build_dag_project_fixture(
            "apply_broken_link",
            "pipeline",
            "trigger:\n  dataset:\n    uri: \"s3://nobody/publishes/this\"\n",
        );

        let result = apply(&manifest, &empty_state(), &root, true, None).await;
        let _ = std::fs::remove_dir_all(&root);

        assert!(
            result.is_ok(),
            "apply must NOT fail on cross-DAG broken Dataset link: {:?}",
            result.err()
        );
    }
}
