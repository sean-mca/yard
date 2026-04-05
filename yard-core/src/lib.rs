pub mod codegen;
pub mod storage;
pub mod utils;

use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use yard_structs::{
    Deployment, DiffType, JobDiff, ProjectManifest, ProjectState,
};

/// Compute the diff between the manifest and the current state.
/// Used by both plan (read-only) and apply (before executing changes).
pub fn calculate_diff(manifest: &ProjectManifest, state: &ProjectState) -> Vec<JobDiff> {
    let mut diffs = Vec::new();

    for (name, job_def) in &manifest.jobs {
        let script_content = crate::codegen::generate_python_script(name, job_def)
            .unwrap_or_else(|_| "".to_string());

        let current_proposed_hash = crate::utils::calculate_hash(&script_content);

        if let Some(existing) = state.deployments.get(name) {
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
            diffs.push(JobDiff {
                name: name.clone(),
                diff_type: DiffType::Create,
                old_hash: None,
                new_hash: Some(current_proposed_hash),
            });
        }
    }

    for (name, existing_state) in &state.deployments {
        if !manifest.jobs.contains_key(name.as_str()) {
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

/// Result of applying changes.
pub struct ApplyResult {
    pub created: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

/// Apply changes: generate scripts, update state, persist to backend.
/// `root_dir` is where `.yard/generated/` lives.
pub async fn apply(
    manifest: &ProjectManifest,
    current_state: &ProjectState,
    root_dir: &Path,
) -> Result<ApplyResult> {
    let diffs = calculate_diff(manifest, current_state);
    let mut updated_state = current_state.clone();
    let mut result = ApplyResult {
        created: Vec::new(),
        modified: Vec::new(),
        deleted: Vec::new(),
    };

    for diff in &diffs {
        match &diff.diff_type {
            DiffType::Create | DiffType::Modify { .. } => {
                let job_def = manifest
                    .jobs
                    .get(&diff.name)
                    .context("Job definition missing during apply")?;

                let script_content = codegen::generate_python_script(&diff.name, job_def)
                    .context("Failed to generate Python script")?;
                let script_hash = utils::calculate_hash(&script_content);

                let gen_dir = root_dir.join(".yard/generated");
                std::fs::create_dir_all(&gen_dir)?;
                let script_path = gen_dir.join(format!("{}.py", diff.name));
                std::fs::write(&script_path, &script_content)?;

                updated_state.deployments.insert(
                    diff.name.clone(),
                    Deployment {
                        config_hash: script_hash,
                        config: job_def.config.clone(),
                        status: "generated".to_string(),
                        applied_at: chrono::Utc::now().to_rfc3339(),
                        resources: Vec::new(),
                        env: None,
                    },
                );

                match &diff.diff_type {
                    DiffType::Create => result.created.push(diff.name.clone()),
                    DiffType::Modify { .. } => result.modified.push(diff.name.clone()),
                    _ => {}
                }
            }
            DiffType::Delete => {
                updated_state.deployments.remove(&diff.name);

                let script_path = root_dir
                    .join(".yard/generated")
                    .join(format!("{}.py", diff.name));
                if script_path.exists() {
                    let _ = std::fs::remove_file(script_path);
                }

                result.deleted.push(diff.name.clone());
            }
            _ => {}
        }
    }

    // Persist state
    let storage = storage::get_storage(&manifest.state).await?;
    storage.write(&updated_state).await?;

    Ok(result)
}

/// Initialize the state backend with the given manifest.
pub async fn init(manifest: &ProjectManifest) -> Result<()> {
    let storage = storage::get_storage(&manifest.state).await?;

    let mut deployments = HashMap::new();
    for (name, job_def) in &manifest.jobs {
        deployments.insert(
            name.clone(),
            Deployment {
                env: Some("default".to_string()),
                config_hash: utils::calculate_json_hash(&job_def.config),
                config: job_def.config.clone(),
                status: "initialized".to_string(),
                applied_at: chrono::Utc::now().to_rfc3339(),
                resources: Vec::new(),
            },
        );
    }

    let new_state = ProjectState {
        project: manifest.project.clone(),
        last_updated: chrono::Utc::now().to_rfc3339(),
        deployments,
    };

    storage.write_new(&new_state).await?;
    Ok(())
}

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
    use yard_structs::{JobDefinition, StateBackend};

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
            state: StateBackend::Local {
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
            deployments: HashMap::from([("old_job".to_string(), make_deployment(&hash, config))]),
        };

        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local {
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
            state: StateBackend::Local {
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
            state: StateBackend::Local {
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
            state: StateBackend::Local {
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
        assert_eq!(diffs.len(), 3);

        let names: Vec<&str> = diffs.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"to_delete"));
        assert!(names.contains(&"to_modify"));
        assert!(names.contains(&"new_job"));
    }

    #[tokio::test]
    async fn apply_creates_scripts_and_updates_state() {
        let dir = std::env::temp_dir().join(format!("yard_apply_{}", std::process::id()));
        let state_path = dir.join(".yard/state.json");

        let job = make_job("glue", json!({"type": "glue", "script_name": "new_job"}));
        let manifest = ProjectManifest {
            project: "test".to_string(),
            state: StateBackend::Local { path: state_path },
            jobs: HashMap::from([("new_job".to_string(), job)]),
        };

        let result = apply(&manifest, &empty_state(), &dir).await.unwrap();

        assert_eq!(result.created, vec!["new_job"]);
        assert!(result.modified.is_empty());
        assert!(result.deleted.is_empty());

        // Verify script was written
        let script_path = dir.join(".yard/generated/new_job.py");
        assert!(script_path.exists());

        // Verify state was persisted
        let state_path = dir.join(".yard/state.json");
        assert!(state_path.exists());
        let state: ProjectState =
            serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
        assert!(state.deployments.contains_key("new_job"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
