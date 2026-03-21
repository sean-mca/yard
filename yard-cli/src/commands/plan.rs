use crate::utils::yaml_to_json;
use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use yaml_rust2::YamlLoader;
use yard_structs::{JobDefinition, ProjectManifest, StateBackend, YardAction};

pub fn execute(directory: Option<String>) -> Result<Option<YardAction>> {
    let base_path = match directory {
        Some(d) => PathBuf::from(d),
        None => std::env::current_dir().context("Failed to get current directory")?,
    };

    let manifest_path = base_path.join("yard.yaml");
    let content = fs::read_to_string(&manifest_path)
        .with_context(|| format!("yard.yaml not found at {:?}", manifest_path))?;

    let docs =
        YamlLoader::load_from_str(&content).map_err(|e| anyhow!("YAML Scan Error: {}", e))?;
    let doc = docs.get(0).ok_or_else(|| anyhow!("yard.yaml is empty"))?;

    // 1. Project & State (Same as init)
    let project = doc["project"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing project name"))?
        .to_string();

    let state_node = &doc["state"];
    let state_type = state_node["type"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing state type"))?;

    let state = match state_type {
        "local" => StateBackend::Local {
            path: PathBuf::from(state_node["path"].as_str().unwrap_or(".yard/state.json")),
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
        _ => return Err(anyhow!("Unsupported state type")),
    };

    // 2. Jobs
    let mut jobs = HashMap::new();
    if let Some(jobs_hash) = doc["jobs"].as_hash() {
        for (key, val) in jobs_hash {
            let name = key.as_str().unwrap().to_string();
            let job_type = val["type"].as_str().unwrap_or("unknown").to_string();
            jobs.insert(
                name,
                JobDefinition {
                    job_type,
                    config: yaml_to_json(val),
                },
            );
        }
    }

    let manifest = ProjectManifest {
        project,
        state,
        jobs,
    };

    // Return the Plan action!
    Ok(Some(YardAction::Plan { manifest }))
}
