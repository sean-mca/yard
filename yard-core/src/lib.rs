pub mod codegen;
pub mod storage;
pub mod utils;
pub mod validation;

use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use yard_structs::{
    Deployment, DiffType, Import, JobDiff, ProjectManifest, ProjectState, Sink, Source, Transform,
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

                if matches!(&diff.diff_type, DiffType::Create) {
                    result.created.push(diff.name.clone());
                } else {
                    result.modified.push(diff.name.clone());
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
        let script_content = codegen::generate_python_script(name, job_def)
            .unwrap_or_else(|_| "".to_string());
        let script_hash = utils::calculate_hash(&script_content);

        deployments.insert(
            name.clone(),
            Deployment {
                env: Some("default".to_string()),
                config_hash: script_hash,
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

/// Extract optional body override from a job config.
pub fn parse_body(config: &Value) -> Option<String> {
    config.get("body").and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// Extract imports from a job config's "imports" array.
pub fn parse_imports(config: &Value) -> Vec<Import> {
    let mut imports = Vec::new();
    if let Some(arr) = config.get("imports").and_then(|v| v.as_array()) {
        for item in arr {
            let name = match item.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let from = item.get("from").and_then(|v| v.as_str()).map(|s| s.to_string());
            imports.push(Import { name, from });
        }
    }
    imports
}

/// Helper to extract an optional string field from JSON.
fn str_field(obj: &Value, key: &str) -> Option<String> {
    obj.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// Helper to extract a string array field from JSON.
fn str_array_field(obj: &Value, key: &str) -> Vec<String> {
    obj.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

/// Helper to extract a string->string map field from JSON.
fn str_map_field(obj: &Value, key: &str) -> HashMap<String, String> {
    obj.get(key)
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_single_source(src: &Value, default_name: &str) -> Option<Source> {
    Some(Source {
        name: str_field(src, "name").unwrap_or_else(|| default_name.to_string()),
        source_type: src.get("type")?.as_str()?.to_string(),
        format: str_field(src, "format"),
        path: str_field(src, "path"),
        connection_url: str_field(src, "connection_url"),
        table: str_field(src, "table"),
        database: str_field(src, "database"),
        secret_id: str_field(src, "secret_id"),
    })
}

/// Extract sources from a job config. Supports both `source:` (single) and `sources:` (list).
pub fn parse_sources(config: &Value) -> Vec<Source> {
    // Try `sources:` (list) first
    if let Some(arr) = config.get("sources").and_then(|v| v.as_array()) {
        return arr
            .iter()
            .enumerate()
            .filter_map(|(i, item)| parse_single_source(item, &format!("source_{i}")))
            .collect();
    }
    // Fall back to `source:` (single)
    if let Some(src) = config.get("source") {
        if let Some(parsed) = parse_single_source(src, "source") {
            return vec![parsed];
        }
    }
    vec![]
}

/// Extract sink configuration from a job config.
pub fn parse_sink(config: &Value) -> Option<Sink> {
    let snk = config.get("sink")?;
    Some(Sink {
        source: str_field(snk, "source"),
        sink_type: snk.get("type")?.as_str()?.to_string(),
        format: str_field(snk, "format"),
        path: str_field(snk, "path"),
        connection_url: str_field(snk, "connection_url"),
        table: str_field(snk, "table"),
        database: str_field(snk, "database"),
        secret_id: str_field(snk, "secret_id"),
        mode: str_field(snk, "mode"),
        partition_by: str_array_field(snk, "partition_by"),
    })
}

/// Extract transforms from a job config.
pub fn parse_transforms(config: &Value) -> Vec<Transform> {
    let mut transforms = Vec::new();
    let Some(arr) = config.get("transforms").and_then(|v| v.as_array()) else {
        return transforms;
    };

    for item in arr {
        let Some(transform_type) = item.get("type").and_then(|v| v.as_str()) else {
            continue;
        };

        transforms.push(Transform {
            transform_type: transform_type.to_string(),
            source: str_field(item, "source"),
            output: str_field(item, "output"),
            condition: str_field(item, "condition"),
            query: str_field(item, "query"),
            columns: str_array_field(item, "columns"),
            mapping: str_map_field(item, "mapping"),
            name: str_field(item, "name"),
            expression: str_field(item, "expression"),
            left: str_field(item, "left"),
            right: str_field(item, "right"),
            on: str_field(item, "on"),
            how: str_field(item, "how"),
        });
    }

    transforms
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
        let imports = parse_imports(&config);
        let body = parse_body(&config);
        let sources = parse_sources(&config);
        let sink = parse_sink(&config);
        let transforms = parse_transforms(&config);
        JobDefinition {
            job_type: job_type.to_string(),
            imports,
            body,
            sources,
            sink,
            transforms,
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
