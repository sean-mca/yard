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
                env: Some("default".to_string()),
                config_hash: utils::calculate_json_hash(&job_def.config),
                config: job_def.config.clone(),
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
                env: Some("default".to_string()), // Or pull from manifest if we add an env field
                config_hash: utils::calculate_json_hash(&job_def.config),
                config: job_def.config.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_manifest(jobs: HashMap<String, JobDefinition>) -> ProjectManifest {
        ProjectManifest {
            project: "test-project".to_string(),
            state: StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            jobs,
        }
    }

    // --- calculate_proposed_state ---

    #[test]
    fn proposed_state_has_all_jobs() {
        let jobs = HashMap::from([
            (
                "job_a".to_string(),
                JobDefinition {
                    job_type: "glue".to_string(),
                    config: json!({"type": "glue"}),
                },
            ),
            (
                "job_b".to_string(),
                JobDefinition {
                    job_type: "glue".to_string(),
                    config: json!({"type": "glue"}),
                },
            ),
        ]);

        let state = calculate_proposed_state(&make_manifest(jobs));
        assert_eq!(state.deployments.len(), 2);
        assert!(state.deployments.contains_key("job_a"));
        assert!(state.deployments.contains_key("job_b"));
    }

    #[test]
    fn proposed_state_sets_pending_status() {
        let jobs = HashMap::from([(
            "job_a".to_string(),
            JobDefinition {
                job_type: "glue".to_string(),
                config: json!({"type": "glue"}),
            },
        )]);

        let state = calculate_proposed_state(&make_manifest(jobs));
        assert_eq!(state.deployments["job_a"].status, "pending");
    }

    #[test]
    fn proposed_state_hashes_config() {
        let config = json!({"type": "glue", "script_name": "etl"});
        let jobs = HashMap::from([(
            "job_a".to_string(),
            JobDefinition {
                job_type: "glue".to_string(),
                config: config.clone(),
            },
        )]);

        let state = calculate_proposed_state(&make_manifest(jobs));
        let expected_hash = utils::calculate_json_hash(&config);
        assert_eq!(state.deployments["job_a"].config_hash, expected_hash);
    }

    // --- calculate_diff ---

    #[test]
    fn state_diff_create() {
        let actual = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::new(),
        };

        let proposed = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "new_job".to_string(),
                Deployment {
                    env: None,
                    config_hash: "abc".to_string(),
                    config: json!({}),
                    status: "pending".to_string(),
                    applied_at: "".to_string(),
                    resources: Vec::new(),
                },
            )]),
        };

        let changes = calculate_diff(&actual, &proposed);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0], StateChange::Create(ref n) if n == "new_job"));
    }

    #[test]
    fn state_diff_delete() {
        let actual = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "old_job".to_string(),
                Deployment {
                    env: None,
                    config_hash: "abc".to_string(),
                    config: json!({}),
                    status: "generated".to_string(),
                    applied_at: "".to_string(),
                    resources: Vec::new(),
                },
            )]),
        };

        let proposed = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::new(),
        };

        let changes = calculate_diff(&actual, &proposed);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0], StateChange::Delete(ref n) if n == "old_job"));
    }

    #[test]
    fn state_diff_no_change() {
        let deployment = Deployment {
            env: None,
            config_hash: "same_hash".to_string(),
            config: json!({}),
            status: "generated".to_string(),
            applied_at: "".to_string(),
            resources: Vec::new(),
        };

        let actual = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([("stable".to_string(), deployment.clone())]),
        };

        let proposed = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([("stable".to_string(), deployment)]),
        };

        let changes = calculate_diff(&actual, &proposed);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0], StateChange::NoChange(ref n) if n == "stable"));
    }

    #[test]
    fn state_diff_modify() {
        let actual = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "job".to_string(),
                Deployment {
                    env: None,
                    config_hash: "old_hash".to_string(),
                    config: json!({}),
                    status: "generated".to_string(),
                    applied_at: "".to_string(),
                    resources: Vec::new(),
                },
            )]),
        };

        let proposed = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "job".to_string(),
                Deployment {
                    env: None,
                    config_hash: "new_hash".to_string(),
                    config: json!({}),
                    status: "pending".to_string(),
                    applied_at: "".to_string(),
                    resources: Vec::new(),
                },
            )]),
        };

        let changes = calculate_diff(&actual, &proposed);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0], StateChange::Modify(ref n) if n == "job"));
    }
}
