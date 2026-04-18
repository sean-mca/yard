use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use yard_structs::{DiffType, JobDiff, ProjectManifest, ProjectState};

use crate::codegen;
use crate::utils;

/// Compute the diff between the manifest and the current state.
/// Used by both plan (read-only) and apply (before executing changes).
pub fn calculate_diff(manifest: &ProjectManifest, state: &ProjectState) -> Result<Vec<JobDiff>> {
    let mut diffs = Vec::new();

    for (name, job_def) in &manifest.jobs {
        let script_content = codegen::generate_python_script(name, job_def)
            .with_context(|| format!("Failed to generate script for job \"{name}\""))?;

        // Hash both the script and the full job config so config-only changes
        // (e.g. worker_type, timeout) are detected even if the script is unchanged
        let config_str = serde_json::to_string(&job_def.config)
            .with_context(|| format!("Failed to serialize config for job \"{name}\""))?;
        let combined = format!("{script_content}\n{config_str}");
        let current_proposed_hash = utils::calculate_hash(&combined);

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

    Ok(diffs)
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
