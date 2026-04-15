use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use yaml_rust2::YamlLoader;
use yard_structs::{JobDefinition, ProjectManifest, ProjectState, StateBackend, YARDContext};

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
    let root_aws = yaml_to_json(&root_doc["aws"]);

    // Cascade provider defaults into each job's `config.<job_type>` block so
    // codegen and validation see the merged view (e.g. warehouse, default_engine).
    // Deploy-time provider instantiation still re-merges via `merge_provider_config`;
    // this cascade only widens visibility — precedence is unchanged.
    let all_jobs = cascade_provider_defaults(all_jobs, &providers, &root_aws);

    let manifest = ProjectManifest {
        project: project.clone(),
        state: state_backend.clone(),
        providers,
        jobs: all_jobs,
        aws: root_aws,
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
        let job_name = [env, folder, Some(base_name)]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("-");
        let job_type = job_doc["type"]
            .as_str()
            .ok_or_else(|| {
                anyhow!(
                    "Job '{}' is missing a 'type' field (glue, emr, bash)",
                    job_name
                )
            })?
            .to_string();
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
            },
        );
    }

    Ok(all_jobs)
}

fn cascade_provider_defaults(
    mut jobs: HashMap<String, JobDefinition>,
    providers: &HashMap<String, Value>,
    root_aws: &Value,
) -> HashMap<String, JobDefinition> {
    for job in jobs.values_mut() {
        if let Some(defaults) = providers.get(&job.job_type) {
            let overrides = job
                .config
                .get(&job.job_type)
                .cloned()
                .unwrap_or(Value::Null);
            let merged = crate::merge_provider_config(defaults, &overrides);
            if let Some(obj) = job.config.as_object_mut() {
                obj.insert(job.job_type.clone(), merged);
            }
        }

        // Merge root aws with the job's nearest-ancestor account.yaml `aws:`
        // block and stash under `config._aws` for providers to read.
        let account_aws = find_and_parse_context(&job.dir, "account.yaml", false)
            .ok()
            .and_then(|v| v.get("aws").cloned())
            .unwrap_or(Value::Null);
        let merged_aws = crate::merge_provider_config(root_aws, &account_aws);
        if let Some(obj) = job.config.as_object_mut() {
            obj.insert("_aws".to_string(), merged_aws);
        }
    }
    jobs
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
