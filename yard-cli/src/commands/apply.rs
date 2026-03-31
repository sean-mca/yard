use crate::utils::yaml_to_json;
use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use yaml_rust2::YamlLoader;
use yard_structs::{JobDefinition, JobState, ProjectManifest, State, StateBackend, YardAction};

pub fn execute(directory: Option<String>) -> Result<Option<YardAction>> {
    let base_path = match directory {
        Some(d) => PathBuf::from(d),
        None => std::env::current_dir().context("Failed to get current directory")?,
    };

    // 1. FIND THE ROOT yard.yaml
    let root_path = crate::context::find_in_parent_folders(&base_path, "yard.yaml")
        .context("No yard.yaml found. You must have a root yard.yaml to define state.")?;
    let root_dir = root_path.parent().unwrap().to_path_buf();

    let root_content = fs::read_to_string(&root_path)?;
    let root_docs = YamlLoader::load_from_str(&root_content)?;
    let root_doc = root_docs
        .get(0)
        .ok_or_else(|| anyhow!("yard.yaml is empty"))?;

    // 2. EXTRACT GLOBAL CONFIG
    let project = root_doc["project"]
        .as_str()
        .context("Missing project name in root")?
        .to_string();
    let state_node = &root_doc["state"];

    let state_backend = match state_node["type"].as_str().context("Missing state type")? {
        "local" => StateBackend::Local {
            path: root_dir.join(state_node["path"].as_str().unwrap_or(".yard/state.json")),
        },
        "s3" => StateBackend::S3 {
            bucket: state_node["bucket"].as_str().unwrap_or("").to_string(),
            region: state_node["region"]
                .as_str()
                .unwrap_or("us-east-1")
                .to_string(),
            key: state_node["key"]
                .as_str()
                .unwrap_or("state.json")
                .to_string(),
        },
        _ => return Err(anyhow!("Unsupported state type in root")),
    };

    // 3. RECURSIVE JOB DISCOVERY
    let mut all_jobs = HashMap::new();
    let search_root = if base_path.join("jobs").exists() {
        base_path.join("jobs")
    } else {
        base_path
    };

    for entry in walkdir::WalkDir::new(search_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "yaml"))
    {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        if file_name == "yard.yaml"
            || file_name == "account.yaml"
            || file_name == "region.yaml"
            || file_name == "transforms.yaml"
        {
            continue;
        }

        let job_dir = path.parent().unwrap();
        let ctx = crate::context::load_context(job_dir)?;

        let raw_job_content = fs::read_to_string(path)?;
        let resolved_job_str = yard_core::utils::resolve_variables(&raw_job_content, &ctx)?;

        let job_docs = YamlLoader::load_from_str(&resolved_job_str)?;
        let job_doc = job_docs
            .get(0)
            .ok_or_else(|| anyhow!("Job file {} is empty", file_name))?;

        let job_name = file_name.replace(".yaml", "");
        let job_type = job_doc["type"].as_str().unwrap_or("unknown").to_string();

        all_jobs.insert(
            job_name,
            JobDefinition {
                job_type,
                config: yaml_to_json(job_doc),
            },
        );
    }

    let manifest = ProjectManifest {
        project: project.clone(),
        state: state_backend.clone(),
        jobs: all_jobs,
    };

    // 4. LOAD CURRENT STATE
    let mut current_state: State = if let StateBackend::Local { path } = &state_backend {
        if path.exists() {
            let state_raw = fs::read_to_string(path)?;
            serde_json::from_str(&state_raw)?
        } else {
            State {
                project: project.clone(),
                deployments: HashMap::new(),
                last_updated: chrono::Utc::now().to_rfc3339(),
            }
        }
    } else {
        return Err(anyhow!("S3 state not yet supported for apply"));
    };

    // 5. CALCULATE DIFF & APPLY
    let diffs = yard_core::calculate_diff(&manifest, &current_state);

    if diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(None);
    }

    println!("Applying changes for {}...", project);

    for diff in diffs {
        match diff.diff_type {
            yard_structs::DiffType::Create | yard_structs::DiffType::Modify { .. } => {
                let job_def = manifest.jobs.get(&diff.name).unwrap();

                // USE THE Deployment STRUCT, NOT JobState
                current_state.deployments.insert(
                    diff.name.clone(),
                    yard_structs::JobState {
                        config_hash: diff.new_hash.clone().unwrap(), // Matches struct field name
                        config: job_def.config.clone(),
                        status: "success".to_string(),
                        applied_at: chrono::Utc::now().to_rfc3339(),
                        resources: Vec::new(),
                    },
                );
            }
            yard_structs::DiffType::Delete => {
                current_state.deployments.remove(&diff.name);
            }
            _ => {}
        }
    }

    // 6. PERSIST UPDATED STATE
    if let StateBackend::Local { path } = &state_backend {
        // Ensure the .yard directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let updated_json = serde_json::to_string_pretty(&current_state)?;
        fs::write(path, updated_json)?;
        println!("\n✅ State updated successfully at {}", path.display());
    }

    Ok(None)
}
