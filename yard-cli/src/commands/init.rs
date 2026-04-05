use crate::utils;
use anyhow::{Context, Result, anyhow};
use std::{collections::HashMap, fs, path::PathBuf};
use yaml_rust2::YamlLoader;
use yard_structs::{JobDefinition, ProjectManifest, StateBackend};

pub async fn execute(directory: Option<String>) -> Result<()> {
    let base_path = match directory {
        Some(d) => PathBuf::from(d),
        None => std::env::current_dir().context("Failed to get current directory")?,
    };

    let manifest_path = base_path.join("yard.yaml");
    let content = fs::read_to_string(&manifest_path)
        .with_context(|| format!("yard.yaml not found at {:?}", manifest_path))?;

    let docs =
        YamlLoader::load_from_str(&content).map_err(|e| anyhow!("YAML Scan Error: {}", e))?;

    let doc = docs.first().ok_or_else(|| anyhow!("yard.yaml is empty"))?;

    let project = doc["project"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing 'project' name in yard.yaml"))?
        .to_string();

    let state_node = &doc["state"];
    let state_type = state_node["type"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing state 'type' (local/s3) in yard.yaml"))?;

    let state = match state_type {
        "local" => {
            let path_str = state_node["path"]
                .as_str()
                .ok_or_else(|| anyhow!("Missing 'path' for local state"))?;
            StateBackend::Local {
                path: PathBuf::from(path_str),
            }
        }
        "s3" => StateBackend::S3 {
            bucket: state_node["bucket"].as_str().unwrap_or("").to_string(),
            region: state_node["region"]
                .as_str()
                .unwrap_or("us-east-1")
                .to_string(),
            key: state_node["key"].as_str().unwrap_or("state/").to_string(),
        },
        _ => return Err(anyhow!("Unsupported state type: {}", state_type)),
    };

    let mut jobs = HashMap::new();
    if let Some(jobs_hash) = doc["jobs"].as_hash() {
        for (key, val) in jobs_hash {
            let name = key
                .as_str()
                .ok_or_else(|| anyhow!("Job name must be a string"))?
                .to_string();

            let job_type = val["type"]
                .as_str()
                .ok_or_else(|| anyhow!("Job '{}' is missing a 'type'", name))?
                .to_string();

            let config_json = utils::yaml_to_json(val);
            let imports = yard_core::parse_imports(&config_json);
            let body = yard_core::parse_body(&config_json);
            let sources = yard_core::parse_sources(&config_json);
            let sink = yard_core::parse_sink(&config_json);
            let transforms = yard_core::parse_transforms(&config_json);

            jobs.insert(
                name,
                JobDefinition {
                    job_type,
                    imports,
                    body,
                    sources,
                    sink,
                    transforms,
                    config: config_json,
                },
            );
        }
    }

    let mut providers = HashMap::new();
    if let Some(providers_hash) = doc["providers"].as_hash() {
        for (key, val) in providers_hash {
            if let Some(name) = key.as_str() {
                providers.insert(name.to_string(), utils::yaml_to_json(val));
            }
        }
    }

    let manifest = ProjectManifest {
        project,
        state,
        providers,
        jobs,
    };

    yard_core::init(&manifest).await?;

    Ok(())
}
