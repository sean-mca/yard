//! Manifest-vs-state diff calculation.
//!
//! Compares the current [`ProjectManifest`] (what the user declares in YAML)
//! against the persisted [`ProjectState`] (what was last deployed) and produces
//! a list of [`JobDiff`] entries describing creates, modifies, and deletes.
//!
//! The diff is deterministic: both manifest jobs and state deployments are
//! iterated via [`BTreeMap`] collects so output order is stable across
//! processes (DIFF-01 invariant).

use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use yard_structs::{Deployment, DiffType, JobDefinition, JobDiff, ProjectManifest, ProjectState};

use crate::plugin_host::PluginHostConfig;
use crate::providers;
use crate::utils;

/// Compute the diff between the manifest and the current state.
///
/// Uses plugin codegen (via [`providers::get_provider_for_job`]) to generate
/// script content for hash computation. Falls back to empty script content
/// when no plugin binary is available (e.g. during early project setup or
/// for job types that don't produce scripts).
///
/// Phase 28 / D-14: iterates `manifest.jobs` and `state.deployments` via
/// a BTreeMap-collect so output is deterministic across processes
/// (DIFF-01 invariant).
///
/// # Errors
///
/// Returns an error if config serialization fails for any job in the manifest.
pub async fn calculate_diff(
    manifest: &ProjectManifest,
    state: &ProjectState,
    plugin_host_config: &PluginHostConfig,
) -> Result<Vec<JobDiff>> {
    let mut diffs = Vec::new();

    let sorted_jobs: BTreeMap<&String, &JobDefinition> = manifest.jobs.iter().collect();
    for (name, job_def) in sorted_jobs {
        // Attempt plugin codegen for script content. If the plugin is not
        // available (no plugin_version/plugin_source, or binary missing),
        // fall back to empty string. This keeps diff computation working
        // for jobs that don't have a plugin configured yet.
        let script_content = match providers::get_provider_for_job(
            &job_def.job_type,
            &job_def.config,
            job_def.plugin_version.as_deref(),
            job_def.plugin_source.as_deref(),
            plugin_host_config,
        )
        .await
        {
            Ok(provider) => provider
                .codegen(name, &job_def.config)
                .await
                .unwrap_or(None)
                .unwrap_or_default(),
            Err(e) => {
                eprintln!(
                    "Warning: plugin codegen failed for job \"{name}\", using empty script for hash: {e}"
                );
                String::new()
            }
        };

        // Hash script + config + trigger so changes to any of them produce a
        // different state hash. Trigger flows through here because task-only
        // jobs generate empty script content and trigger-only changes still
        // need to fire drift detection (HASH-01).
        let config_str = serde_json::to_string(&job_def.config)
            .with_context(|| format!("Failed to serialize config for job \"{name}\""))?;
        let trigger_str = match job_def.airflow.as_ref().and_then(|a| a.overrides.trigger.as_ref()) {
            Some(t) => serde_json::to_string(t)
                .with_context(|| format!("Failed to serialize trigger for job \"{name}\""))?,
            None => String::new(),
        };
        let combined = format!("{script_content}\n{config_str}\n{trigger_str}");
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

    let sorted_deployments: BTreeMap<&String, &Deployment> = state.deployments.iter().collect();
    for (name, existing_state) in sorted_deployments {
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

/// Compare two JSON objects and return a map of changed keys to `(old, new)` value strings.
///
/// Detects added, modified, AND deleted keys. A key present in `old` but
/// absent in `new` is reported as `(old_value, "null")`.
#[must_use]
fn compare_json(old: &Value, new: &Value) -> BTreeMap<String, (String, String)> {
    let mut changes = BTreeMap::new();
    if let (Value::Object(old_obj), Value::Object(new_obj)) = (old, new) {
        // Added or modified keys
        for (k, v) in new_obj {
            let old_val = old_obj.get(k).unwrap_or(&Value::Null);
            if old_val != v {
                changes.insert(k.clone(), (old_val.to_string(), v.to_string()));
            }
        }
        // Deleted keys (present in old, absent in new)
        for (k, v) in old_obj {
            if !new_obj.contains_key(k) {
                changes.insert(k.clone(), (v.to_string(), "null".to_string()));
            }
        }
    }
    changes
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yard_structs::{
        AirflowJobBlock, AirflowSection, DatasetTrigger, JobDefinition, JobType, ProjectManifest,
        ProjectState, S3Trigger, ScheduleTrigger, SingleSource, StateBackend, Trigger,
    };

    /// Build a `JobDefinition` whose only varying field is `airflow.trigger`.
    /// Uses `Plugin("bash")` job type -- plugin codegen will fail gracefully
    /// (no binary available) and fall back to empty script content, which
    /// keeps the fixture cheap while still exercising hash computation.
    fn job_def_with_trigger(trigger: Option<Trigger>) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Plugin("bash".to_string()),
            airflow: Some(AirflowJobBlock {
                overrides: AirflowSection {
                    trigger,
                    ..Default::default()
                },
                ..Default::default()
            }),
            config: serde_json::json!({}),
            ..Default::default()
        }
    }

    fn test_plugin_config() -> PluginHostConfig {
        PluginHostConfig {
            plugins_dir: std::path::PathBuf::from("/tmp/yard-test-plugins"),
            ..Default::default()
        }
    }

    fn empty_manifest() -> ProjectManifest {
        ProjectManifest {
            project: "test-fixture".to_string(),
            state: StateBackend::Local {
                path: std::path::PathBuf::new(),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: None,
        }
    }

    fn empty_state() -> ProjectState {
        ProjectState {
            project: "test-fixture".to_string(),
            last_updated: String::new(),
            deployments: HashMap::new(),
        }
    }

    /// DIFF-01: `calculate_diff` produces byte-identical output across 100
    /// successive calls with the same manifest.
    #[tokio::test]
    async fn calculate_diff_byte_identical_across_100_calls() {
        let mut manifest = empty_manifest();
        manifest.jobs.insert("a".into(), job_def_with_trigger(None));
        manifest.jobs.insert("b".into(), job_def_with_trigger(None));
        manifest.jobs.insert("c".into(), job_def_with_trigger(None));
        let state = empty_state();
        let phc = test_plugin_config();
        let first =
            serde_json::to_string(&calculate_diff(&manifest, &state, &phc).await.unwrap()).unwrap();
        for i in 0..99 {
            let again =
                serde_json::to_string(&calculate_diff(&manifest, &state, &phc).await.unwrap()).unwrap();
            assert_eq!(first, again, "calculate_diff output drifted on iteration {i}");
        }
    }

    /// HASH-01: Adding or changing a job's `airflow.trigger` block produces a
    /// non-zero state-hash diff.
    #[tokio::test]
    async fn calculate_diff_hashes_change_when_trigger_changes() {
        let def_schedule = job_def_with_trigger(Some(Trigger::Single(SingleSource::Schedule(
            ScheduleTrigger {
                value: "@daily".into(),
            },
        ))));
        let def_s3 = job_def_with_trigger(Some(Trigger::Single(SingleSource::S3(S3Trigger {
            bucket: "x".into(),
            prefix: Some("y".into()),
            ..Default::default()
        }))));
        let mut manifest_schedule = empty_manifest();
        manifest_schedule
            .jobs
            .insert("job-1".into(), def_schedule);
        let mut manifest_s3 = empty_manifest();
        manifest_s3.jobs.insert("job-1".into(), def_s3);
        let state = empty_state();
        let phc = test_plugin_config();
        let diffs_schedule = calculate_diff(&manifest_schedule, &state, &phc).await.unwrap();
        let diffs_s3 = calculate_diff(&manifest_s3, &state, &phc).await.unwrap();
        let hash_schedule = diffs_schedule[0]
            .new_hash
            .as_ref()
            .expect("create has new_hash");
        let hash_s3 = diffs_s3[0].new_hash.as_ref().expect("create has new_hash");
        assert_ne!(
            hash_schedule, hash_s3,
            "trigger change must produce different state hashes"
        );
    }

    /// HASH-02 end-to-end: composite `Trigger::All` lists serialize to
    /// byte-identical wire form regardless of input order.
    #[tokio::test]
    async fn calculate_diff_hash_stable_under_composite_reorder() {
        let def_a = job_def_with_trigger(Some(Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
        ])));
        let def_b = job_def_with_trigger(Some(Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
        ])));
        let mut manifest_a = empty_manifest();
        manifest_a.jobs.insert("job-1".into(), def_a);
        let mut manifest_b = empty_manifest();
        manifest_b.jobs.insert("job-1".into(), def_b);
        let state = empty_state();
        let phc = test_plugin_config();
        let diffs_a = calculate_diff(&manifest_a, &state, &phc).await.unwrap();
        let diffs_b = calculate_diff(&manifest_b, &state, &phc).await.unwrap();
        let hash_a = diffs_a[0].new_hash.as_ref().expect("create has new_hash");
        let hash_b = diffs_b[0].new_hash.as_ref().expect("create has new_hash");
        assert_eq!(
            hash_a, hash_b,
            "composite trigger reorder must produce identical state hashes (HASH-02 end-to-end)"
        );
    }
}
