pub mod apply;
pub mod init;
pub mod plan;
// pub mod destroy;

use crate::utils::yaml_to_json;
use anyhow::{Context, Result, anyhow};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use yaml_rust2::YamlLoader;
use yard_structs::{JobDefinition, ProjectManifest, ProjectState, StateBackend, YARDContext};

pub struct ResolvedProject {
    pub manifest: ProjectManifest,
    pub current_state: ProjectState,
    pub root_dir: PathBuf,
}

pub async fn resolve_project(directory: Option<String>) -> Result<ResolvedProject> {
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

    // 3. RECURSIVE JOB DISCOVERY (with context caching)
    let search_root = if base_path.join("jobs").exists() {
        base_path.join("jobs")
    } else {
        base_path
    };

    let all_jobs = discover_jobs(&search_root)?;

    let manifest = ProjectManifest {
        project: project.clone(),
        state: state_backend.clone(),
        jobs: all_jobs,
    };

    // 4. LOAD CURRENT STATE
    let current_state = load_state(&state_backend, &project).await?;

    Ok(ResolvedProject {
        manifest,
        current_state,
        root_dir,
    })
}

fn discover_jobs(search_root: &PathBuf) -> Result<HashMap<String, JobDefinition>> {
    let mut all_jobs = HashMap::new();
    let mut context_cache: HashMap<PathBuf, YARDContext> = HashMap::new();

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

        let job_dir = path.parent().unwrap().to_path_buf();

        let ctx = match context_cache.get(&job_dir) {
            Some(cached) => cached,
            None => {
                let loaded = crate::context::load_context(&job_dir)?;
                context_cache.insert(job_dir.clone(), loaded);
                context_cache.get(&job_dir).unwrap()
            }
        };

        let raw_job_content = fs::read_to_string(path)?;
        let resolved_job_str = yard_core::utils::resolve_variables(&raw_job_content, ctx)?;

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

    Ok(all_jobs)
}

async fn load_state(backend: &StateBackend, project: &str) -> Result<ProjectState> {
    let storage = yard_core::storage::get_storage(backend).await?;
    match storage.read().await {
        Ok(state) => Ok(state),
        Err(_) => {
            // No state yet — return empty state so plan/apply treat everything as new
            Ok(ProjectState {
                project: project.to_string(),
                deployments: HashMap::new(),
                last_updated: chrono::Utc::now().to_rfc3339(),
            })
        }
    }
}
