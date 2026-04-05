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
