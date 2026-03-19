use anyhow::{Context, Result, anyhow};
use std::fs;
use std::path::PathBuf;
use yaml_rust2::YamlLoader;
use yard_structs::{ProjectManifest, StateBackend, YardAction};

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

    // 1. Extract Project Name
    let project = doc["project"]
        .as_str()
        .ok_or_else(|| anyhow!("Missing 'project' name in yard.yaml"))?
        .to_string();

    // 2. Extract State Info
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
            key: state_node["key"]
                .as_str()
                .unwrap_or("state.json")
                .to_string(),
        },
        _ => return Err(anyhow!("Unsupported state type: {}", state_type)),
    };

    // 3. Construct Manifest (Variable names now match Struct fields)
    let manifest = ProjectManifest { project, state };

    Ok(Some(YardAction::Init { manifest }))
}
