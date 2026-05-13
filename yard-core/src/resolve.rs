use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use yaml_rust2::YamlLoader;
use yard_structs::{
    DiscoveredEnvironment, JobDefinition, JobType, ProjectManifest, ProjectState, RegionSummary,
    StateBackend, YARDContext,
};

/// Allowed top-level keys on a yard.yaml manifest (TYPE-03 D-19). Any other
/// key surfaces as an `unknown field 'X' at yard.yaml ...` error at parse
/// time.
const ALLOWED_TOP_LEVEL: &[&str] = &["project", "state", "providers", "jobs", "aws"];

/// Allowed keys on a yard.yaml `state:` block (StateBackend wire surface).
/// Both `Local` (path) and `S3` (bucket/region/key/aws) variants share the
/// same flat allow-list — serde dispatches on the `type` discriminator.
const ALLOWED_STATE_BLOCK: &[&str] = &["type", "bucket", "region", "key", "path", "aws"];

/// Static allowed keys on a job-doc (the `JobDefinition` wire surface, as
/// the user types it in `<job>.yaml`). Dynamic provider-block keys (the
/// wire strings from each `JobType` variant) are appended at runtime per
/// D-21 to keep this list in sync with TYPE-01.
const STATIC_JOB_DOC_ALLOWED: &[&str] = &[
    "type",
    "role",
    "imports",
    "body",
    "job_file",
    "sources",
    "source",
    "sink",
    "transforms",
    "airflow",
    "partition_by",
    "partition_timestamp_column",
    "create_timestamp",
    "_aws",
];

/// Build the full allowed-keys list for a job doc by combining the static
/// fields with the wire-form of every `JobType` variant (so `glue: {...}`,
/// `emr: {...}`, `bash: {...}` provider-block siblings are accepted but
/// `sprk: {...}` is rejected). D-21: list is derived from the live
/// `JobType` enum, not hard-coded — adding a fourth variant in TYPE-01
/// flows through here without an edit.
fn job_doc_allowed_keys() -> Vec<String> {
    let mut all: Vec<String> = STATIC_JOB_DOC_ALLOWED
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    for variant in [JobType::Glue, JobType::Emr, JobType::Bash] {
        all.push(variant.to_string());
    }
    all
}

pub struct ResolvedProject {
    pub manifest: ProjectManifest,
    pub current_state: ProjectState,
    pub root_dir: PathBuf,
}

pub async fn resolve_project(base_path: &Path) -> Result<ResolvedProject> {
    // 1. Find yard.yaml
    let root_path = find_in_parent_folders(base_path, "yard.yaml")
        .context("No yard.yaml found. You must have a root yard.yaml to define state.")?;
    let root_dir = root_path
        .parent()
        .context("yard.yaml path has no parent directory")?
        .to_path_buf();

    let root_content = fs::read_to_string(&root_path)?;
    let root_docs = YamlLoader::load_from_str(&root_content)?;
    let root_doc = root_docs
        .first()
        .ok_or_else(|| anyhow!("yard.yaml is empty"))?;

    // TYPE-03 D-19: gate the yard.yaml top-level + the `state:` block
    // against unknown keys. Catches the user-typo footgun (e.g. `provider:`
    // instead of `providers:`) at parse time with a structured error.
    // Validates against the JSON-converted view so the helper's `as_object`
    // check works the same way it does for the `parse_*` callers.
    let root_value = yaml_to_json(root_doc);
    crate::parsing::validate_unknown_keys(&root_value, ALLOWED_TOP_LEVEL, "yard.yaml")?;
    if let Some(state_value) = root_value.get("state") {
        crate::parsing::validate_unknown_keys(
            state_value,
            ALLOWED_STATE_BLOCK,
            "yard.yaml.state",
        )?;
    }

    // 2. Extract global config
    let project = root_doc["project"]
        .as_str()
        .context("Missing project name in root")?
        .to_string();
    let state_node = &root_doc["state"];

    let state_backend = match state_node["type"].as_str().context("Missing state type")? {
        "local" => StateBackend::Local {
            path: root_dir.join(state_node["path"].as_str().unwrap_or(".yard/state/")),
        },
        "s3" => StateBackend::S3 {
            bucket: state_node["bucket"]
                .as_str()
                .filter(|s| !s.is_empty())
                .context("S3 state backend requires a non-empty 'bucket' field")?
                .to_string(),
            region: state_node["region"]
                .as_str()
                .unwrap_or("us-east-1")
                .to_string(),
            key: state_node["key"].as_str().unwrap_or("state/").to_string(),
            // Plan 02 owns reading `state.aws` from yaml and env-merging
            // `YARD_STATE_AWS_*`. Until then, default to None so the
            // `#[serde(default)]` deserialization behavior is preserved here
            // and nothing in today's codepath changes. (TYPE-02 retypes the
            // field; the runtime intent is unchanged.)
            aws: None,
        },
        _ => return Err(anyhow!("Unsupported state type in root")),
    };

    // 3. Recursive job discovery
    let search_root = if base_path.join("jobs").exists() {
        base_path.join("jobs")
    } else {
        base_path.to_path_buf()
    };

    let all_jobs = discover_jobs(&search_root)?;

    // 4. Parse providers config
    let mut providers = HashMap::new();
    if let Some(providers_hash) = root_doc["providers"].as_hash() {
        for (key, val) in providers_hash {
            if let Some(name) = key.as_str() {
                providers.insert(name.to_string(), yaml_to_json(val));
            }
        }
    }

    // Root-level aws block — yard's own AWS credential config (AssumeRole etc.)
    // Kept as Value here for `cascade_provider_defaults` (which still merges
    // into the per-job `JobDefinition.config: Value` blob via `_aws` per
    // D-09 / D-14). The structural manifest field is the typed
    // `Option<AwsCredentialConfig>` produced via best-effort parse —
    // malformed root `aws:` blocks fall through to None today (plan 21-03
    // owns strict typo gating via `validate_unknown_keys`).
    let root_aws = yaml_to_json(&root_doc["aws"]);

    // Cascade provider defaults into each job's `config.<job_type>` block so
    // codegen and validation see the merged view (e.g. warehouse, default_engine).
    // Deploy-time provider instantiation still re-merges via `merge_provider_config`;
    // this cascade only widens visibility — precedence is unchanged.
    let all_jobs = cascade_provider_defaults(all_jobs, &providers, &root_aws);

    let typed_root_aws: Option<yard_structs::AwsCredentialConfig> = if root_aws.is_null() {
        None
    } else {
        serde_json::from_value(root_aws.clone()).ok()
    };

    let manifest = ProjectManifest {
        project: project.clone(),
        state: state_backend.clone(),
        providers,
        jobs: all_jobs,
        aws: typed_root_aws,
    };

    // 5. Load current state
    let current_state = crate::load_state(&state_backend, &project).await?;

    Ok(ResolvedProject {
        manifest,
        current_state,
        root_dir,
    })
}

fn discover_jobs(search_root: &Path) -> Result<HashMap<String, JobDefinition>> {
    let mut all_jobs = HashMap::new();
    let mut context_cache: HashMap<PathBuf, YARDContext> = HashMap::new();

    for entry in walkdir::WalkDir::new(search_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "yaml"))
    {
        let path = entry.path();
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow!("Path has no file name: {}", path.display()))?
            .to_str()
            .ok_or_else(|| anyhow!("Non-UTF8 file name: {}", path.display()))?;

        if file_name == "yard.yaml"
            || file_name == "account.yaml"
            || file_name == "region.yaml"
            || file_name == "transforms.yaml"
            || file_name == "dag.yaml"
        {
            continue;
        }

        let job_dir = path
            .parent()
            .ok_or_else(|| anyhow!("Job file has no parent directory: {}", path.display()))?
            .to_path_buf();

        let ctx = match context_cache.get(&job_dir) {
            Some(cached) => cached,
            None => {
                let loaded = load_context(&job_dir)?;
                context_cache.insert(job_dir.clone(), loaded);
                context_cache.get(&job_dir).ok_or_else(|| {
                    anyhow!(
                        "Failed to retrieve cached context for {}",
                        job_dir.display()
                    )
                })?
            }
        };

        let raw_job_content = fs::read_to_string(path)?;
        let resolved_job_str = crate::utils::resolve_variables(&raw_job_content, ctx)?;

        let job_docs = YamlLoader::load_from_str(&resolved_job_str)?;
        let job_doc = job_docs
            .first()
            .ok_or_else(|| anyhow!("Job file {} is empty", file_name))?;

        let base_name = file_name.replace(".yaml", "");
        let folder = job_dir
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());
        let env = {
            let mut parts = Vec::new();
            for comp in job_dir.components() {
                parts.push(comp.as_os_str().to_string_lossy().to_string());
            }
            parts
                .iter()
                .position(|p| p == "envs")
                .and_then(|i| parts.get(i + 1).cloned())
        };
        let job_name = [env, folder, Some(base_name.clone())]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("-");
        let job_type: JobType = job_doc["type"]
            .as_str()
            .ok_or_else(|| {
                anyhow!(
                    "Job '{}' is missing a 'type' field (glue, emr, bash)",
                    job_name
                )
            })?
            .parse()
            .with_context(|| format!("invalid job type for job '{job_name}'"))?;
        let config = yaml_to_json(job_doc);

        // TYPE-03 D-19/D-21: gate the job doc against the static
        // `JobDefinition` keys plus the dynamic `JobType`-derived
        // provider-block keys. Catches user typos like `sceudule:`,
        // `transformsss:`, or `glu: { ... }` at parse time with a
        // structured error.
        let allowed_owned = job_doc_allowed_keys();
        let allowed_borrowed: Vec<&str> = allowed_owned.iter().map(String::as_str).collect();
        let job_path = format!("jobs.{job_name}");
        crate::parsing::validate_unknown_keys(&config, &allowed_borrowed, &job_path)?;

        let imports = crate::parse_imports(&config);
        let body = crate::parse_body(&config);
        let sources = crate::parse_sources(&config, &job_path)?;
        let sink = crate::parse_sink(&config, &job_path)?;
        let transforms = crate::parse_transforms(&config, &job_path)?;
        let airflow = crate::parse_airflow_job_block(&config, &job_path)?;

        // Resolve job_file path relative to the job YAML's directory
        let job_file = crate::parse_job_file(&config).map(|p| {
            let resolved = job_dir.join(&p);
            resolved.to_string_lossy().to_string()
        });

        let partition_by = crate::parse_partition_by(&config);
        let partition_timestamp_column = crate::parse_partition_timestamp_column(&config);
        let create_timestamp = crate::parse_create_timestamp(&config);

        all_jobs.insert(
            job_name,
            JobDefinition {
                job_type,
                imports,
                body,
                job_file,
                sources,
                sink,
                transforms,
                airflow,
                partition_by,
                partition_timestamp_column,
                create_timestamp,
                config,
                dir: job_dir.clone(),
                base_name,
            },
        );
    }

    Ok(all_jobs)
}

/// Extract a field from the nearest ancestor context file (account.yaml or
/// region.yaml) by walking up from `dir`. Returns `Value::Null` when the
/// file doesn't exist or the field is absent.
fn context_field(dir: &Path, filename: &str, field: &str) -> Value {
    find_and_parse_context(dir, filename, false)
        .ok()
        .and_then(|v| v.get(field).cloned())
        .unwrap_or(Value::Null)
}

/// Deep-merge cascade applied to every job:
///
/// (`text` fence: this block is ASCII art, not Rust code — fencing as `text`
/// keeps it out of doctest compilation entirely, which is more correct than
/// `no_run` for non-Rust content.)
///
/// ```text
/// root (yard.yaml)  →  account.yaml  →  region.yaml  →  job-inline
/// ```
///
/// Each layer wins over the one before it via `merge_provider_config` (recursive
/// deep-merge). Both `providers.<type>` and `aws:` follow the same four-layer
/// precedence chain.
fn cascade_provider_defaults(
    mut jobs: HashMap<String, JobDefinition>,
    providers: &HashMap<String, Value>,
    root_aws: &Value,
) -> HashMap<String, JobDefinition> {
    for job in jobs.values_mut() {
        // --- providers.<type> cascade ---
        // The providers HashMap is keyed by wire-string job type ("glue",
        // "emr", "bash"); JobType::to_string() gives the canonical wire form.
        let job_type_key = job.job_type.to_string();
        let root_provider = providers
            .get(&job_type_key)
            .cloned()
            .unwrap_or(Value::Null);
        let account_provider = context_field(&job.dir, "account.yaml", &job_type_key);
        let region_provider = context_field(&job.dir, "region.yaml", &job_type_key);
        let job_inline_provider = job
            .config
            .get(&job_type_key)
            .cloned()
            .unwrap_or(Value::Null);

        let merged = cascade_merge(&[
            &root_provider,
            &account_provider,
            &region_provider,
            &job_inline_provider,
        ]);
        if let Some(obj) = job.config.as_object_mut() {
            obj.insert(job_type_key, merged);
        }

        // --- aws cascade ---
        let account_aws = context_field(&job.dir, "account.yaml", "aws");
        let region_aws = context_field(&job.dir, "region.yaml", "aws");
        let job_inline_aws = job.config.get("aws").cloned().unwrap_or(Value::Null);

        let merged_aws = cascade_merge(&[root_aws, &account_aws, &region_aws, &job_inline_aws]);
        if let Some(obj) = job.config.as_object_mut() {
            obj.insert("_aws".to_string(), merged_aws);
        }
    }
    jobs
}

/// Fold N layers left-to-right via deep-merge; later layers win.
fn cascade_merge(layers: &[&Value]) -> Value {
    layers
        .iter()
        .copied()
        .fold(Value::Null, |acc, layer| {
            crate::merge_provider_config(&acc, layer)
        })
}

// ---- Context loading ----

pub fn find_in_parent_folders(start_path: &Path, filename: &str) -> Option<PathBuf> {
    let mut current = start_path.to_path_buf();
    loop {
        let target = current.join(filename);
        if target.exists() {
            return Some(target);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

pub fn find_and_parse_context(start_path: &Path, filename: &str, required: bool) -> Result<Value> {
    let mut current = start_path.to_path_buf();

    loop {
        let target = current.join(filename);
        if target.exists() {
            let content = fs::read_to_string(&target)
                .with_context(|| format!("Failed to read {}", target.display()))?;
            let docs = YamlLoader::load_from_str(&content)
                .map_err(|e| anyhow!("YAML error in {}: {}", target.display(), e))?;
            let doc = docs
                .first()
                .ok_or_else(|| anyhow!("{} is empty", target.display()))?;
            return Ok(yaml_to_json(doc));
        }
        if !current.pop() {
            break;
        }
    }

    if required {
        Err(anyhow!(
            "Required context file '{}' not found in {} or parents",
            filename,
            start_path.display()
        ))
    } else {
        Ok(Value::Object(serde_json::Map::new()))
    }
}

pub fn load_context(current_dir: &Path) -> Result<YARDContext> {
    let account = find_and_parse_context(current_dir, "account.yaml", true)?;
    let region = find_and_parse_context(current_dir, "region.yaml", true)?;
    let transforms = find_and_parse_context(current_dir, "transforms.yaml", false)?;
    let dag = find_and_parse_context(current_dir, "dag.yaml", false)?;

    Ok(YARDContext {
        account,
        region,
        transforms,
        dag,
    })
}

// ---- YAML to JSON conversion ----

pub fn yaml_to_json(yaml: &yaml_rust2::Yaml) -> Value {
    match yaml {
        yaml_rust2::Yaml::Real(s) | yaml_rust2::Yaml::String(s) => Value::String(s.clone()),
        yaml_rust2::Yaml::Integer(i) => Value::Number((*i).into()),
        yaml_rust2::Yaml::Boolean(b) => Value::Bool(*b),
        yaml_rust2::Yaml::Array(a) => Value::Array(a.iter().map(yaml_to_json).collect()),
        yaml_rust2::Yaml::Hash(h) => {
            let mut map = serde_json::Map::new();
            for (k, v) in h {
                if let Some(key_str) = k.as_str() {
                    map.insert(key_str.to_string(), yaml_to_json(v));
                }
            }
            Value::Object(map)
        }
        yaml_rust2::Yaml::Null => Value::Null,
        _ => Value::Null,
    }
}

/// Reserved YAML filenames that are NOT counted as job files during
/// environment discovery. Matches the exclusion list in `discover_jobs()`.
const RESERVED_YAML_FILES: &[&str] = &[
    "yard.yaml",
    "account.yaml",
    "region.yaml",
    "transforms.yaml",
    "dag.yaml",
];

/// Discover environments by walking the repo directory structure following the
/// `root/{env}/{region}/**` convention (D-04, D-07). Resolves the yard.yaml
/// config cascade per environment WITHOUT loading state. Each directory that
/// contains `account.yaml` is treated as an environment directory; each
/// subdirectory within it that contains `region.yaml` is treated as a region.
///
/// Returns `Vec<DiscoveredEnvironment>` with environment name (directory name,
/// not account ID per D-12), optional account_id/role_arn from account.yaml,
/// and per-region job/DAG summaries.
pub fn discover_environments(root_path: &Path) -> Result<Vec<DiscoveredEnvironment>> {
    // 1. Locate yard.yaml to establish the project root (Pitfall 3).
    let yard_yaml_path = find_in_parent_folders(root_path, "yard.yaml")
        .ok_or_else(|| anyhow!("No yard.yaml found in {} or parent directories", root_path.display()))?;
    let project_root = yard_yaml_path
        .parent()
        .ok_or_else(|| anyhow!("yard.yaml path has no parent directory"))?;

    let mut environments = Vec::new();

    // 2. Walk immediate children of the project root. Each child directory
    //    that contains account.yaml is treated as an environment (Pitfall 5).
    let entries = fs::read_dir(project_root)
        .with_context(|| format!("Failed to read directory: {}", project_root.display()))?;

    for entry in entries {
        let entry = entry.with_context(|| {
            format!("Failed to read entry in {}", project_root.display())
        })?;
        let entry_path = entry.path();

        // Skip non-directories
        if !entry_path.is_dir() {
            continue;
        }

        // Environment marker: must contain account.yaml (Pitfall 5, Pitfall 6).
        // Directories without account.yaml are silently skipped.
        if !entry_path.join("account.yaml").exists() {
            continue;
        }

        let env_name = entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow!("Non-UTF8 directory name: {}", entry_path.display()))?
            .to_string();

        // 3. Parse account.yaml to extract account_id and role_arn.
        let account_config = find_and_parse_context(&entry_path, "account.yaml", false)?;
        let account_id = account_config.get("account_id").and_then(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .or_else(|| v.as_u64().map(|n| n.to_string()))
                .or_else(|| v.as_i64().map(|n| n.to_string()))
        });
        let role_arn = account_config
            .pointer("/aws/assume_role")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // 4. Walk immediate children of the env directory for region directories.
        let mut regions = Vec::new();
        let region_entries = fs::read_dir(&entry_path)
            .with_context(|| format!("Failed to read env directory: {}", entry_path.display()))?;

        for region_entry in region_entries {
            let region_entry = region_entry.with_context(|| {
                format!("Failed to read entry in {}", entry_path.display())
            })?;
            let region_path = region_entry.path();

            // Skip non-directories
            if !region_path.is_dir() {
                continue;
            }

            // Region marker: must contain region.yaml
            if !region_path.join("region.yaml").exists() {
                continue;
            }

            let region_name = region_path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| anyhow!("Non-UTF8 directory name: {}", region_path.display()))?
                .to_string();

            // 5. Count jobs and DAGs within the region directory.
            let mut job_count: u64 = 0;
            let mut dag_count: u64 = 0;
            let mut jobs = Vec::new();

            let region_files = fs::read_dir(&region_path)
                .with_context(|| format!("Failed to read region directory: {}", region_path.display()))?;

            for file_entry in region_files {
                let file_entry = file_entry.with_context(|| {
                    format!("Failed to read entry in {}", region_path.display())
                })?;
                let file_path = file_entry.path();

                // Only process .yaml files
                if file_path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                    continue;
                }

                let file_name = file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| anyhow!("Non-UTF8 file name: {}", file_path.display()))?;

                // Count dag.yaml marker files
                if file_name == "dag.yaml" {
                    dag_count += 1;
                    continue;
                }

                // Skip other reserved files
                if RESERVED_YAML_FILES.contains(&file_name) {
                    continue;
                }

                // This is a job file — parse just the type field for JobSummary
                let job_name = file_name.strip_suffix(".yaml")
                    .ok_or_else(|| anyhow!("Expected .yaml extension: {}", file_path.display()))?
                    .to_string();

                let content = fs::read_to_string(&file_path)
                    .with_context(|| format!("Failed to read {}", file_path.display()))?;
                let docs = YamlLoader::load_from_str(&content)
                    .map_err(|e| anyhow!("YAML error in {}: {}", file_path.display(), e))?;

                if let Some(doc) = docs.first() {
                    let job_value = yaml_to_json(doc);
                    if let Some(type_str) = job_value.get("type").and_then(|v| v.as_str())
                        && let Ok(job_type) = type_str.parse::<JobType>()
                    {
                        jobs.push(yard_structs::JobSummary {
                            name: job_name,
                            job_type,
                        });
                        job_count += 1;
                    }
                }
            }

            regions.push(RegionSummary {
                name: region_name,
                job_count,
                dag_count,
                jobs,
            });
        }

        environments.push(DiscoveredEnvironment {
            name: env_name,
            account_id,
            role_arn,
            regions,
        });
    }

    Ok(environments)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let p = std::env::temp_dir()
                .join(format!("yard_resolve_{}_{}", std::process::id(), n));
            let _ = fs::remove_dir_all(&p);
            fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn glue_job(dir: &Path, inline_aws: Option<Value>) -> JobDefinition {
        let mut cfg = json!({"type": "glue"});
        if let Some(aws) = inline_aws {
            cfg.as_object_mut().unwrap().insert("aws".into(), aws);
        }
        JobDefinition {
            job_type: JobType::Glue,
            config: cfg,
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn run_cascade(
        jobs: Vec<(String, JobDefinition)>,
        root_aws: Value,
    ) -> HashMap<String, JobDefinition> {
        run_cascade_with_providers(jobs, root_aws, HashMap::new())
    }

    fn run_cascade_with_providers(
        jobs: Vec<(String, JobDefinition)>,
        root_aws: Value,
        providers: HashMap<String, Value>,
    ) -> HashMap<String, JobDefinition> {
        let map: HashMap<String, JobDefinition> = jobs.into_iter().collect();
        cascade_provider_defaults(map, &providers, &root_aws)
    }

    fn aws_field<'a>(job: &'a JobDefinition, key: &str) -> Option<&'a str> {
        job.config.get("_aws")?.get(key)?.as_str()
    }

    fn provider_field<'a>(job: &'a JobDefinition, provider: &str, key: &str) -> Option<&'a str> {
        job.config.get(provider)?.get(key)?.as_str()
    }

    // --- aws cascade: root → account → region → job ---

    #[test]
    fn inline_aws_overrides_root() {
        let tmp = TempDir::new();
        let job = glue_job(
            &tmp.0,
            Some(json!({"assume_role": "arn:aws:iam::222222222222:role/Deploy"})),
        );
        let out = run_cascade(
            vec![("j".into(), job)],
            json!({"assume_role": "arn:aws:iam::111111111111:role/Root"}),
        );
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::222222222222:role/Deploy")
        );
    }

    #[test]
    fn inline_aws_overrides_account_yaml() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::222222222222:role/Account\n",
        )
        .unwrap();
        let job = glue_job(
            &tmp.0,
            Some(json!({"assume_role": "arn:aws:iam::333333333333:role/Inline"})),
        );
        let out = run_cascade(
            vec![("j".into(), job)],
            json!({"assume_role": "arn:aws:iam::111111111111:role/Root"}),
        );
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::333333333333:role/Inline")
        );
    }

    #[test]
    fn inline_aws_deep_merges_with_account_yaml_siblings() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::222222222222:role/Account\n  region: eu-west-1\n",
        )
        .unwrap();
        let job = glue_job(
            &tmp.0,
            Some(json!({"assume_role": "arn:aws:iam::333333333333:role/Inline"})),
        );
        let out = run_cascade(vec![("j".into(), job)], Value::Null);
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::333333333333:role/Inline")
        );
        assert_eq!(aws_field(&out["j"], "region"), Some("eu-west-1"));
    }

    #[test]
    fn no_inline_falls_back_to_account_yaml() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::222222222222:role/Account\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, None);
        let out = run_cascade(
            vec![("j".into(), job)],
            json!({"assume_role": "arn:aws:iam::111111111111:role/Root"}),
        );
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::222222222222:role/Account")
        );
    }

    #[test]
    fn no_inline_no_account_uses_root() {
        let tmp = TempDir::new();
        let job = glue_job(&tmp.0, None);
        let out = run_cascade(
            vec![("j".into(), job)],
            json!({"assume_role": "arn:aws:iam::111111111111:role/Root"}),
        );
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::111111111111:role/Root")
        );
    }

    #[test]
    fn region_yaml_aws_overrides_account_yaml() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::222222222222:role/Account\n  region: us-east-1\n",
        )
        .unwrap();
        fs::write(
            tmp.0.join("region.yaml"),
            "aws:\n  region: eu-west-1\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, None);
        let out = run_cascade(vec![("j".into(), job)], Value::Null);
        // region.yaml wins for region
        assert_eq!(aws_field(&out["j"], "region"), Some("eu-west-1"));
        // account.yaml preserved for assume_role (region.yaml didn't set it)
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::222222222222:role/Account")
        );
    }

    #[test]
    fn full_four_layer_aws_cascade() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  from_account: account\n  from_region: will_be_overridden\n",
        )
        .unwrap();
        fs::write(
            tmp.0.join("region.yaml"),
            "aws:\n  from_region: region\n  from_job: will_be_overridden\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, Some(json!({"from_job": "job"})));
        let out = run_cascade(
            vec![("j".into(), job)],
            json!({"from_root": "root", "from_account": "will_be_overridden"}),
        );
        assert_eq!(aws_field(&out["j"], "from_root"), Some("root"));
        assert_eq!(aws_field(&out["j"], "from_account"), Some("account"));
        assert_eq!(aws_field(&out["j"], "from_region"), Some("region"));
        assert_eq!(aws_field(&out["j"], "from_job"), Some("job"));
    }

    #[test]
    fn aws_conn_id_cascades_per_field_with_assume_role() {
        // Mixed-layer override: assume_role at root, aws_conn_id at account.yaml.
        // Both must survive into _aws — cascade is per-field on the aws block.
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  aws_conn_id: my_acct_conn\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, None);
        let out = run_cascade(
            vec![("j".into(), job)],
            json!({"assume_role": "arn:aws:iam::111111111111:role/Root"}),
        );
        assert_eq!(aws_field(&out["j"], "aws_conn_id"), Some("my_acct_conn"));
        assert_eq!(
            aws_field(&out["j"], "assume_role"),
            Some("arn:aws:iam::111111111111:role/Root"),
            "assume_role from root must survive when account.yaml only sets aws_conn_id"
        );
    }

    #[test]
    fn aws_conn_id_inline_job_overrides_lower_layers() {
        // job-inline aws_conn_id beats account.yaml's value.
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "aws:\n  aws_conn_id: from_account\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, Some(json!({"aws_conn_id": "from_inline"})));
        let out = run_cascade(vec![("j".into(), job)], Value::Null);
        assert_eq!(aws_field(&out["j"], "aws_conn_id"), Some("from_inline"));
    }

    // --- provider cascade: root → account → region → job ---

    #[test]
    fn provider_root_flows_through_when_no_overrides() {
        let tmp = TempDir::new();
        let job = glue_job(&tmp.0, None);
        let providers = HashMap::from([(
            "glue".to_string(),
            json!({"script_bucket": "root-bucket", "warehouse": "s3://root/"}),
        )]);
        let out = run_cascade_with_providers(vec![("j".into(), job)], Value::Null, providers);
        assert_eq!(
            provider_field(&out["j"], "glue", "script_bucket"),
            Some("root-bucket")
        );
        assert_eq!(
            provider_field(&out["j"], "glue", "warehouse"),
            Some("s3://root/")
        );
    }

    #[test]
    fn provider_account_yaml_overrides_root() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "glue:\n  script_bucket: account-bucket\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, None);
        let providers = HashMap::from([(
            "glue".to_string(),
            json!({"script_bucket": "root-bucket", "warehouse": "s3://root/"}),
        )]);
        let out = run_cascade_with_providers(vec![("j".into(), job)], Value::Null, providers);
        assert_eq!(
            provider_field(&out["j"], "glue", "script_bucket"),
            Some("account-bucket")
        );
        // Unset fields preserved from root
        assert_eq!(
            provider_field(&out["j"], "glue", "warehouse"),
            Some("s3://root/")
        );
    }

    #[test]
    fn provider_region_yaml_overrides_account() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "glue:\n  script_bucket: account-bucket\n  warehouse: s3://account/\n",
        )
        .unwrap();
        fs::write(
            tmp.0.join("region.yaml"),
            "glue:\n  warehouse: s3://region/\n",
        )
        .unwrap();
        let job = glue_job(&tmp.0, None);
        let providers = HashMap::from([(
            "glue".to_string(),
            json!({"script_bucket": "root-bucket", "warehouse": "s3://root/"}),
        )]);
        let out = run_cascade_with_providers(vec![("j".into(), job)], Value::Null, providers);
        // account wins over root for script_bucket
        assert_eq!(
            provider_field(&out["j"], "glue", "script_bucket"),
            Some("account-bucket")
        );
        // region wins over account for warehouse
        assert_eq!(
            provider_field(&out["j"], "glue", "warehouse"),
            Some("s3://region/")
        );
    }

    #[test]
    fn provider_job_inline_overrides_all_layers() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "glue:\n  script_bucket: account-bucket\n",
        )
        .unwrap();
        fs::write(
            tmp.0.join("region.yaml"),
            "glue:\n  warehouse: s3://region/\n",
        )
        .unwrap();
        let mut job = glue_job(&tmp.0, None);
        job.config
            .as_object_mut()
            .unwrap()
            .insert("glue".into(), json!({"script_bucket": "job-bucket"}));
        let providers = HashMap::from([(
            "glue".to_string(),
            json!({"script_bucket": "root-bucket", "warehouse": "s3://root/"}),
        )]);
        let out = run_cascade_with_providers(vec![("j".into(), job)], Value::Null, providers);
        // Job wins for script_bucket
        assert_eq!(
            provider_field(&out["j"], "glue", "script_bucket"),
            Some("job-bucket")
        );
        // Region still wins for warehouse (job didn't override it)
        assert_eq!(
            provider_field(&out["j"], "glue", "warehouse"),
            Some("s3://region/")
        );
    }

    #[test]
    fn full_four_layer_provider_cascade() {
        let tmp = TempDir::new();
        fs::write(
            tmp.0.join("account.yaml"),
            "glue:\n  from_account: account\n  from_region: will_be_overridden\n",
        )
        .unwrap();
        fs::write(
            tmp.0.join("region.yaml"),
            "glue:\n  from_region: region\n  from_job: will_be_overridden\n",
        )
        .unwrap();
        let mut job = glue_job(&tmp.0, None);
        job.config
            .as_object_mut()
            .unwrap()
            .insert("glue".into(), json!({"from_job": "job"}));
        let providers = HashMap::from([(
            "glue".to_string(),
            json!({"from_root": "root", "from_account": "will_be_overridden"}),
        )]);
        let out = run_cascade_with_providers(vec![("j".into(), job)], Value::Null, providers);
        assert_eq!(provider_field(&out["j"], "glue", "from_root"), Some("root"));
        assert_eq!(provider_field(&out["j"], "glue", "from_account"), Some("account"));
        assert_eq!(provider_field(&out["j"], "glue", "from_region"), Some("region"));
        assert_eq!(provider_field(&out["j"], "glue", "from_job"), Some("job"));
    }

    // --- job-doc allow-list (regression for missing `role:` top-level key) ---

    #[test]
    fn job_doc_allow_list_admits_role_at_top_level() {
        // Glue jobs put `role` at the top level of the job doc (sibling to
        // `glue:`); this is the shape documented in README/GETTING-STARTED
        // and required by `glue::validate_config`. The phase 21-03 typo
        // gate must not reject it.
        let tmp = TempDir::new();
        fs::write(tmp.0.join("account.yaml"), "{}").unwrap();
        fs::write(tmp.0.join("region.yaml"), "{}").unwrap();
        fs::write(
            tmp.0.join("my_job.yaml"),
            "type: glue\nrole: arn:aws:iam::123456789012:role/GlueJob\nglue:\n  script_bucket: my-bucket\n",
        )
        .unwrap();
        let jobs = discover_jobs(&tmp.0).expect("discover must accept role at top level");
        assert_eq!(jobs.len(), 1, "got: {:?}", jobs.keys());
        let (_, def) = jobs.iter().next().unwrap();
        assert_eq!(
            def.config.get("role").and_then(|v| v.as_str()),
            Some("arn:aws:iam::123456789012:role/GlueJob"),
        );
    }

    #[test]
    fn job_doc_allow_list_still_rejects_typos() {
        // Sanity: the gate still catches genuine typos at the job-doc level.
        let tmp = TempDir::new();
        fs::write(tmp.0.join("account.yaml"), "{}").unwrap();
        fs::write(tmp.0.join("region.yaml"), "{}").unwrap();
        fs::write(
            tmp.0.join("my_job.yaml"),
            "type: glue\nrolle: arn:aws:iam::123456789012:role/GlueJob\n",
        )
        .unwrap();
        let err = discover_jobs(&tmp.0).expect_err("typo must reject");
        let msg = format!("{err}");
        assert!(msg.contains("unknown field 'rolle'"), "got: {msg}");
    }

    // --- discover_environments (Phase 40) ---

    /// Helper: create a minimal yard project fixture with env/region structure.
    fn make_yard_project(root: &Path) {
        fs::write(
            root.join("yard.yaml"),
            "project: test\nstate:\n  type: local\n  path: .yard/state\n",
        )
        .unwrap();
    }

    #[test]
    fn discover_environments_single_env_single_region() {
        let tmp = TempDir::new();
        make_yard_project(&tmp.0);

        // Create production/us-east-1 with account.yaml, region.yaml, and 2 jobs
        let env_dir = tmp.0.join("production");
        let region_dir = env_dir.join("us-east-1");
        fs::create_dir_all(&region_dir).unwrap();
        fs::write(env_dir.join("account.yaml"), "{}").unwrap();
        fs::write(region_dir.join("region.yaml"), "{}").unwrap();
        fs::write(region_dir.join("orders.yaml"), "type: glue\n").unwrap();
        fs::write(region_dir.join("users.yaml"), "type: emr\n").unwrap();

        let envs = discover_environments(&tmp.0).unwrap();
        assert_eq!(envs.len(), 1, "expected 1 env, got: {envs:?}");
        assert_eq!(envs[0].name, "production");
        assert_eq!(envs[0].regions.len(), 1);
        assert_eq!(envs[0].regions[0].name, "us-east-1");
        assert_eq!(envs[0].regions[0].job_count, 2);
        assert_eq!(envs[0].regions[0].jobs.len(), 2);
    }

    #[test]
    fn discover_environments_multi_env() {
        let tmp = TempDir::new();
        make_yard_project(&tmp.0);

        // dev with one region
        let dev_dir = tmp.0.join("dev");
        let dev_region = dev_dir.join("us-east-1");
        fs::create_dir_all(&dev_region).unwrap();
        fs::write(dev_dir.join("account.yaml"), "{}").unwrap();
        fs::write(dev_region.join("region.yaml"), "{}").unwrap();
        fs::write(dev_region.join("job1.yaml"), "type: glue\n").unwrap();

        // prod with one region
        let prod_dir = tmp.0.join("prod");
        let prod_region = prod_dir.join("eu-west-1");
        fs::create_dir_all(&prod_region).unwrap();
        fs::write(prod_dir.join("account.yaml"), "{}").unwrap();
        fs::write(prod_region.join("region.yaml"), "{}").unwrap();
        fs::write(prod_region.join("job2.yaml"), "type: bash\n").unwrap();

        let envs = discover_environments(&tmp.0).unwrap();
        assert_eq!(envs.len(), 2, "expected 2 envs, got: {envs:?}");
        // Should be findable by name
        let dev = envs.iter().find(|e| e.name == "dev").expect("dev missing");
        let prod = envs.iter().find(|e| e.name == "prod").expect("prod missing");
        assert_eq!(dev.regions[0].name, "us-east-1");
        assert_eq!(prod.regions[0].name, "eu-west-1");
    }

    #[test]
    fn discover_environments_missing_account_yaml_skipped() {
        let tmp = TempDir::new();
        make_yard_project(&tmp.0);

        // good env has account.yaml
        let good_dir = tmp.0.join("good");
        let good_region = good_dir.join("us-east-1");
        fs::create_dir_all(&good_region).unwrap();
        fs::write(good_dir.join("account.yaml"), "{}").unwrap();
        fs::write(good_region.join("region.yaml"), "{}").unwrap();

        // bad env lacks account.yaml — should be skipped
        let bad_dir = tmp.0.join("bad");
        let bad_region = bad_dir.join("us-west-2");
        fs::create_dir_all(&bad_region).unwrap();
        // No account.yaml here
        fs::write(bad_region.join("region.yaml"), "{}").unwrap();

        let envs = discover_environments(&tmp.0).unwrap();
        assert_eq!(envs.len(), 1, "expected only 1 env (bad skipped), got: {envs:?}");
        assert_eq!(envs[0].name, "good");
    }

    #[test]
    fn discover_environments_role_arn_from_account_yaml() {
        let tmp = TempDir::new();
        make_yard_project(&tmp.0);

        let env_dir = tmp.0.join("staging");
        let region_dir = env_dir.join("us-east-1");
        fs::create_dir_all(&region_dir).unwrap();
        fs::write(
            env_dir.join("account.yaml"),
            "aws:\n  assume_role: arn:aws:iam::987654321098:role/Deploy\n",
        )
        .unwrap();
        fs::write(region_dir.join("region.yaml"), "{}").unwrap();

        let envs = discover_environments(&tmp.0).unwrap();
        assert_eq!(envs.len(), 1);
        assert_eq!(
            envs[0].role_arn.as_deref(),
            Some("arn:aws:iam::987654321098:role/Deploy")
        );
    }

    #[test]
    fn discover_environments_dag_count() {
        let tmp = TempDir::new();
        make_yard_project(&tmp.0);

        let env_dir = tmp.0.join("production");
        let region_dir = env_dir.join("us-east-1");
        fs::create_dir_all(&region_dir).unwrap();
        fs::write(env_dir.join("account.yaml"), "{}").unwrap();
        fs::write(region_dir.join("region.yaml"), "{}").unwrap();
        fs::write(region_dir.join("dag.yaml"), "schedule: '@daily'\n").unwrap();
        fs::write(region_dir.join("orders.yaml"), "type: glue\n").unwrap();

        let envs = discover_environments(&tmp.0).unwrap();
        assert_eq!(envs[0].regions[0].dag_count, 1);
        assert_eq!(envs[0].regions[0].job_count, 1); // dag.yaml not counted as job
    }

    #[test]
    fn discover_environments_numeric_account_id() {
        let tmp = TempDir::new();
        make_yard_project(&tmp.0);

        let env_dir = tmp.0.join("production");
        let region_dir = env_dir.join("us-east-1");
        fs::create_dir_all(&region_dir).unwrap();
        fs::write(
            env_dir.join("account.yaml"),
            "account_id: 123456789012\n",
        )
        .unwrap();
        fs::write(region_dir.join("region.yaml"), "{}").unwrap();

        let envs = discover_environments(&tmp.0).unwrap();
        assert_eq!(envs[0].account_id.as_deref(), Some("123456789012"));
    }

    #[test]
    fn discover_environments_no_yard_yaml_errors() {
        let tmp = TempDir::new();
        // No yard.yaml at all
        let result = discover_environments(&tmp.0);
        assert!(result.is_err(), "expected error when no yard.yaml");
        let msg = format!("{}", result.unwrap_err());
        assert!(
            msg.contains("yard.yaml"),
            "error should mention yard.yaml, got: {msg}"
        );
    }
}
