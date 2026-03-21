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

    let state = match state_node["type"].as_str().context("Missing state type")? {
        "local" => StateBackend::Local {
            // Anchor state to the root yard.yaml location
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

    // We search from the root down. If you want to only plan a sub-folder,
    // you'd pass that folder as an argument.
    let search_root = if base_path.join("jobs").exists() {
        base_path.join("jobs")
    } else {
        base_path
    };

    // Note: You'll need `walkdir = "2"` in yard-cli/Cargo.toml
    for entry in walkdir::WalkDir::new(search_root)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "yaml"))
    {
        let path = entry.path();
        let file_name = path.file_name().unwrap().to_str().unwrap();

        // Skip config/context files
        if file_name == "yard.yaml"
            || file_name == "account.yaml"
            || file_name == "region.yaml"
            || file_name == "transforms.yaml"
        {
            continue;
        }

        // 4. LOAD CONTEXT (Crawl UP from the JOB'S specific location)
        let job_dir = path.parent().unwrap();
        let ctx = crate::context::load_context(job_dir)?;

        // 5. RESOLVE & PARSE
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
        project,
        state,
        jobs: all_jobs,
    };

    println!("DEBUG: Found {} jobs in the tree", manifest.jobs.len());
    for job_name in manifest.jobs.keys() {
        println!(" - Job: {}", job_name);
    }

    Ok(Some(YardAction::Plan { manifest }))
}
