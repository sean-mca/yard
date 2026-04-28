use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use yard_structs::{Deployment, DiffType, JobDefinition, JobDiff, ProjectManifest, ProjectState};

use crate::codegen;
use crate::utils;

/// Compute the diff between the manifest and the current state.
/// Used by both plan (read-only) and apply (before executing changes).
///
/// Phase 28 / D-14: iterates `manifest.jobs` and `state.deployments` via
/// a BTreeMap-collect so output is deterministic across processes
/// (DIFF-01 invariant). The BTreeMap-collect idiom matches
/// `airflow_dag::connections::required_connections_for_dag` (the in-tree
/// precedent at line 96).
pub fn calculate_diff(manifest: &ProjectManifest, state: &ProjectState) -> Result<Vec<JobDiff>> {
    let mut diffs = Vec::new();

    let sorted_jobs: BTreeMap<&String, &JobDefinition> = manifest.jobs.iter().collect();
    for (name, job_def) in sorted_jobs {
        let script_content = codegen::generate_python_script(name, job_def)
            .with_context(|| format!("Failed to generate script for job \"{name}\""))?;

        // Hash script + config + trigger so changes to any of them produce a
        // different state hash. Trigger flows through here because Bash jobs
        // generate empty script content and trigger-only changes still need
        // to fire drift detection (HASH-01).
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

fn compare_json(old: &Value, new: &Value) -> BTreeMap<String, (String, String)> {
    let mut changes = BTreeMap::new();
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yard_structs::{
        AirflowJobBlock, AirflowSection, DatasetTrigger, JobDefinition, JobType, ProjectManifest,
        ProjectState, S3Trigger, ScheduleTrigger, SingleSource, StateBackend, Trigger,
    };

    /// Build a `JobDefinition` whose only varying field is `airflow.trigger`.
    /// `JobDefinition` has a `Default` impl at config.rs:283; `AirflowJobBlock`
    /// and `AirflowSection` both `derive(Default)`. Bash job_type is task-only
    /// so `generate_python_script` returns an empty string — keeping the
    /// fixture cheap. The `airflow.overrides.trigger` field is the only
    /// non-default value.
    fn job_def_with_trigger(trigger: Option<Trigger>) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Bash,
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

    /// Build a minimal `ProjectManifest`. `ProjectManifest` does NOT derive
    /// `Default` (verified at planner time against config.rs:118-143). Use
    /// the `StateBackend::Local { path: PathBuf::new() }` smallest-valid
    /// variant — same value in both manifest fixtures so it doesn't perturb
    /// the hash comparison.
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

    /// Build a minimal `ProjectState`. `ProjectState` does NOT derive `Default`
    /// (verified against state.rs:33). Construct explicitly with empty
    /// `deployments`. Used for HASH-01 / HASH-02 fixtures where every job in
    /// the manifest produces a `DiffType::Create` (state is empty).
    fn empty_state() -> ProjectState {
        ProjectState {
            project: "test-fixture".to_string(),
            last_updated: String::new(),
            deployments: HashMap::new(),
        }
    }

    /// DIFF-01: `calculate_diff` produces byte-identical output across 100
    /// successive calls. Uses 3 jobs with distinct names a/b/c so the
    /// iteration order over `manifest.jobs` is non-trivial — without the
    /// BTreeMap-collect at line 14 / 45 this test would flake when HashMap's
    /// random seed produced varying iteration orders.
    #[test]
    fn calculate_diff_byte_identical_across_100_calls() {
        let mut manifest = empty_manifest();
        manifest.jobs.insert("a".into(), job_def_with_trigger(None));
        manifest.jobs.insert("b".into(), job_def_with_trigger(None));
        manifest.jobs.insert("c".into(), job_def_with_trigger(None));
        let state = empty_state();
        let first =
            serde_json::to_string(&calculate_diff(&manifest, &state).unwrap()).unwrap();
        for i in 0..99 {
            let again =
                serde_json::to_string(&calculate_diff(&manifest, &state).unwrap()).unwrap();
            assert_eq!(first, again, "calculate_diff output drifted on iteration {i}");
        }
    }

    /// HASH-01: Adding or changing a DAG's `airflow.trigger` block produces a
    /// non-zero state-hash diff. End-to-end regression that builds two
    /// `JobDefinition` values that differ ONLY in `airflow.trigger`, runs
    /// them through `calculate_diff` (which calls the same `calculate_hash`
    /// shape) and asserts the resulting `new_hash` values differ.
    #[test]
    fn calculate_diff_hashes_change_when_trigger_changes() {
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
        let diffs_schedule = calculate_diff(&manifest_schedule, &state).unwrap();
        let diffs_s3 = calculate_diff(&manifest_s3, &state).unwrap();
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
    /// byte-identical wire form regardless of input order, so the
    /// state-hash through `calculate_diff` is identical for two manifests
    /// whose only difference is the order of elements inside a
    /// `Trigger::All`. The type-level invariant is locked by
    /// `trigger::tests::trigger_serialize_sort_homogeneous` in plan 28-01;
    /// this test verifies the invariant survives the full `calculate_hash`
    /// pipeline (`format!("{script}\n{config}")`).
    #[test]
    fn calculate_diff_hash_stable_under_composite_reorder() {
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
        let diffs_a = calculate_diff(&manifest_a, &state).unwrap();
        let diffs_b = calculate_diff(&manifest_b, &state).unwrap();
        let hash_a = diffs_a[0].new_hash.as_ref().expect("create has new_hash");
        let hash_b = diffs_b[0].new_hash.as_ref().expect("create has new_hash");
        assert_eq!(
            hash_a, hash_b,
            "composite trigger reorder must produce identical state hashes (HASH-02 end-to-end)"
        );
    }
}
