use anyhow::{Context, Result};
pub mod codegen;
mod state;
pub mod storage;
pub mod utils;
use serde_json::Value;
use std::collections::HashMap;
use yard_structs::{DiffType, JobDiff, ProjectManifest, ProjectState, YardAction};

pub async fn dispatch(action: YardAction) -> Result<()> {
    match action {
        YardAction::Init { manifest } => {
            state::initialize_backend(&manifest.project, &manifest.state, &manifest.jobs).await?;
        }
        YardAction::Plan { manifest } => {
            let storage = storage::get_storage(&manifest.state).await?;
            let actual_state = storage.read().await.context("Run init first!")?;
            let proposed_state = state::calculate_proposed_state(&manifest);

            let changes = state::calculate_diff(&actual_state, &proposed_state);
        }
        YardAction::Apply { manifest } => {
            let storage = storage::get_storage(&manifest.state).await?;
            let mut state = storage.read().await.context("Run init first!")?;

            // This is where the Python generation and Diffing logic
            // should actually live to be "architecturally correct"
            let diffs = calculate_diff(&manifest, &state);

            for diff in diffs {
                // 1. Generate Python
                // 2. Upload to S3
                // 3. Update 'state' object
            }

            storage.write(&state).await?;
        }
        _ => {}
    }
    Ok(())
}

pub fn calculate_diff(manifest: &ProjectManifest, state: &ProjectState) -> Vec<JobDiff> {
    let mut diffs = Vec::new();

    for (name, job_def) in &manifest.jobs {
        // --- THE FIX IS HERE ---
        // Plan must generate the script to see if the hash actually changed
        let script_content = crate::codegen::generate_python_script(name, job_def)
            .unwrap_or_else(|_| "".to_string());

        let current_proposed_hash = crate::utils::calculate_hash(&script_content);

        if let Some(existing) = state.deployments.get(name) {
            // Compare the generated script hash vs the hash stored in state
            if existing.config_hash != current_proposed_hash {
                let changes = compare_json(&existing.config, &job_def.config);
                diffs.push(JobDiff {
                    name: name.clone(),
                    diff_type: DiffType::Modify { changes },
                    old_hash: Some(existing.config_hash.clone()),
                    new_hash: Some(current_proposed_hash),
                });
            }
        } else {
            // CREATE
            diffs.push(JobDiff {
                name: name.clone(),
                diff_type: DiffType::Create,
                old_hash: None,
                new_hash: Some(current_proposed_hash),
            });
        }
    }

    // 2. Check for Deleted Jobs
    for (name, existing_state) in &state.deployments {
        if !manifest.jobs.contains_key(name.as_str()) {
            // DELETE
            diffs.push(JobDiff {
                name: name.clone(),
                diff_type: DiffType::Delete,
                old_hash: Some(existing_state.config_hash.clone()),
                new_hash: None,
            });
        }
    }

    diffs
}

// The "Find Changes" helper (Actual implementation)
fn compare_json(old: &Value, new: &Value) -> HashMap<String, (String, String)> {
    let mut changes = HashMap::new();
    if let (Value::Object(old_obj), Value::Object(new_obj)) = (old, new) {
        for (k, v) in new_obj {
            let old_val = old_obj.get(k).unwrap_or(&Value::Null);
            if old_val != v {
                changes.insert(k.clone(), (old_val.to_string(), v.to_string()));
            }
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use yard_structs::{Deployment, JobDefinition};

    fn make_job(job_type: &str, config: serde_json::Value) -> JobDefinition {
        JobDefinition {
            job_type: job_type.to_string(),
            config,
        }
    }

    fn make_deployment(config_hash: &str, config: serde_json::Value) -> Deployment {
        Deployment {
            env: None,
            config_hash: config_hash.to_string(),
            config,
            status: "generated".to_string(),
            applied_at: "2025-01-01T00:00:00Z".to_string(),
            resources: Vec::new(),
        }
    }

    fn empty_state() -> ProjectState {
        ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::new(),
        }
    }

    #[test]
    fn diff_detects_create() {
        let job = make_job("glue", json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            jobs: HashMap::from([("new_job".to_string(), job)]),
        };

        let diffs = calculate_diff(&manifest, &empty_state());
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Create));
        assert_eq!(diffs[0].name, "new_job");
    }

    #[test]
    fn diff_detects_delete() {
        let config = json!({"type": "glue"});
        let hash = crate::utils::calculate_hash("some old script");
        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([
                ("old_job".to_string(), make_deployment(&hash, config)),
            ]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            jobs: HashMap::new(),
        };

        let diffs = calculate_diff(&manifest, &state);
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Delete));
        assert_eq!(diffs[0].name, "old_job");
    }

    #[test]
    fn diff_detects_no_change() {
        let job = make_job("glue", json!({"type": "glue", "script_name": "stable"}));
        // Generate the script hash the same way calculate_diff does internally
        let script = crate::codegen::generate_python_script("stable", &job).unwrap();
        let hash = crate::utils::calculate_hash(&script);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "stable".to_string(),
                make_deployment(&hash, job.config.clone()),
            )]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            jobs: HashMap::from([("stable".to_string(), job)]),
        };

        let diffs = calculate_diff(&manifest, &state);
        assert!(diffs.is_empty());
    }

    #[test]
    fn diff_detects_modify() {
        let old_config = json!({"type": "glue", "script_name": "v1"});
        let new_job = make_job("glue", json!({"type": "glue", "script_name": "v2"}));

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([(
                "my_job".to_string(),
                make_deployment("stale_hash", old_config),
            )]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            jobs: HashMap::from([("my_job".to_string(), new_job)]),
        };

        let diffs = calculate_diff(&manifest, &state);
        assert_eq!(diffs.len(), 1);
        assert!(matches!(diffs[0].diff_type, DiffType::Modify { .. }));
    }

    #[test]
    fn diff_mixed_create_modify_delete() {
        let keep_job = make_job("glue", json!({"type": "glue", "script_name": "keep"}));
        let keep_script = crate::codegen::generate_python_script("keep", &keep_job).unwrap();
        let keep_hash = crate::utils::calculate_hash(&keep_script);

        let state = ProjectState {
            project: "test".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::from([
                (
                    "keep".to_string(),
                    make_deployment(&keep_hash, keep_job.config.clone()),
                ),
                (
                    "to_delete".to_string(),
                    make_deployment("old", json!({"type": "glue"})),
                ),
                (
                    "to_modify".to_string(),
                    make_deployment("outdated", json!({"type": "glue", "v": "1"})),
                ),
            ]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: yard_structs::StateBackend::Local {
                path: ".yard/state.json".into(),
            },
            jobs: HashMap::from([
                ("keep".to_string(), keep_job),
                (
                    "to_modify".to_string(),
                    make_job("glue", json!({"type": "glue", "v": "2"})),
                ),
                (
                    "new_job".to_string(),
                    make_job("glue", json!({"type": "glue"})),
                ),
            ]),
        };

        let diffs = calculate_diff(&manifest, &state);
        assert_eq!(diffs.len(), 3); // modify, create, delete (keep is no-change)

        let names: Vec<&str> = diffs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"to_delete"));
        assert!(names.contains(&"to_modify"));
        assert!(names.contains(&"new_job"));
    }
}
