use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use yaml_rust2::YamlLoader;
use yard_structs::{
    JobDefinition, JobType, ProjectManifest, ProjectState, StateBackend, YARDContext,
};

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
        let imports = crate::parse_imports(&config);
        let body = crate::parse_body(&config);
        let sources = crate::parse_sources(&config);
        let sink = crate::parse_sink(&config);
        let transforms = crate::parse_transforms(&config);
        let airflow = crate::parse_airflow_job_block(&config);

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
}
