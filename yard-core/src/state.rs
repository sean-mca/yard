// yard-core/src/state.rs
use crate::storage;
use crate::utils;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use std::collections::HashMap;
use yard_structs::{
    Deployment, JobDefinition, ProjectManifest, ProjectState, StateBackend, StateChange,
};

pub async fn initialize_backend(
    project_name: &str,
    backend: &StateBackend,
    jobs: &HashMap<String, JobDefinition>, // Add this!
) -> Result<()> {
    let storage = storage::get_storage(backend).await?;

    // 1. Calculate the initial deployments from the jobs provided
    let mut deployments = HashMap::new();
    for (name, job_def) in jobs {
        deployments.insert(
            name.clone(),
            Deployment {
                env: "default".to_string(),
                config_hash: utils::calculate_hash(&job_def.config),
                status: "initialized".to_string(),
                applied_at: Utc::now().to_rfc3339(),
                resources: Vec::new(),
            },
        );
    }

    // 2. Build the state with ACTUAL deployments
    let new_state = ProjectState {
        project: project_name.to_string(),
        last_updated: Utc::now().to_rfc3339(),
        deployments,
    };

    storage.write_new(&new_state).await?;
    Ok(())
}

pub fn calculate_proposed_state(manifest: &ProjectManifest) -> ProjectState {
    let mut deployments = HashMap::new();

    for (name, job_def) in &manifest.jobs {
        // Here is the structure that actually matches your struct
        deployments.insert(
            name.clone(),
            Deployment {
                env: "default".to_string(), // Or pull from manifest if we add an env field
                config_hash: utils::calculate_hash(&job_def.config),
                status: "pending".to_string(),
                applied_at: "".to_string(),
                resources: Vec::new(),
            },
        );
    }

    ProjectState {
        project: manifest.project.clone(),
        last_updated: Utc::now().to_rfc3339(),
        deployments,
    }
}

pub fn calculate_diff(actual: &ProjectState, proposed: &ProjectState) -> Vec<StateChange> {
    let mut changes = Vec::new();

    // Check for Creates and Modifications
    for (name, proposed_deploy) in &proposed.deployments {
        match actual.deployments.get(name) {
            Some(actual_deploy) => {
                if actual_deploy.config_hash != proposed_deploy.config_hash {
                    changes.push(StateChange::Modify(name.clone()));
                } else {
                    changes.push(StateChange::NoChange(name.clone()));
                }
            }
            None => changes.push(StateChange::Create(name.clone())),
        }
    }

    // Check for Deletions
    for name in actual.deployments.keys() {
        if !proposed.deployments.contains_key(name) {
            changes.push(StateChange::Delete(name.clone()));
        }
    }

    changes
}
