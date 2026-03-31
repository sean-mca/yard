use anyhow::{Context, Result};
pub mod codegen;
mod state;
mod storage;
pub mod utils;
use serde_json::Value;
use std::collections::HashMap;
use yard_structs::{DiffType, JobDiff, ProjectManifest, State, YardAction};

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
        _ => {}
    }
    Ok(())
}

pub fn calculate_diff(manifest: &ProjectManifest, state: &State) -> Vec<JobDiff> {
    let mut diffs = Vec::new();

    // 1. Check for New or Modified Jobs
    for (name, job) in &manifest.jobs {
        // Calculate what the hash WOULD be if we applied this
        let new_hash = crate::utils::calculate_json_hash(&job.config);

        if let Some(existing) = state.deployments.get(name) {
            if existing.config_hash != new_hash {
                // MODIFIED
                let changes = compare_json(&existing.config, &job.config);
                diffs.push(JobDiff {
                    name: name.clone(),
                    diff_type: DiffType::Modify { changes },
                    old_hash: Some(existing.config_hash.clone()),
                    new_hash: Some(new_hash),
                });
            }
        } else {
            // CREATE
            diffs.push(JobDiff {
                name: name.clone(),
                diff_type: DiffType::Create,
                old_hash: None,
                new_hash: Some(new_hash),
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
