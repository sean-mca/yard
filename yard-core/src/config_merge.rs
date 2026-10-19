//! Provider configuration deep-merge.
//!
//! [`build_provider_config`] and [`merge_provider_config`] perform recursive
//! deep-merge of provider-level defaults with per-job overrides. Nested
//! objects merge key-by-key; arrays and scalars are replaced wholesale.
//! [`effective_job_config`] assembles the single JSON document a plugin
//! receives as `job_config`.

use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use yard_structs::JobDefinition;

/// Build the `Value` passed to `get_provider`: provider defaults shallow-
/// merged with the job's `<job_type>:` block, plus the per-job `_aws` block
/// (resolved at discovery time) injected alongside.
#[must_use]
pub fn build_provider_config(
    provider_defaults: &Value,
    full_config: &Value,
    job_type: &str,
) -> Value {
    let job_overrides = full_config.get(job_type).cloned().unwrap_or(Value::Null);
    let mut merged = merge_provider_config(provider_defaults, &job_overrides);
    if let Some(aws) = full_config.get("_aws")
        && let Some(obj) = merged.as_object_mut()
    {
        obj.insert("_aws".to_string(), aws.clone());
    }
    merged
}

/// Build the `job_config` document sent to a plugin.
///
/// The document is the job as yard parsed it (typed `sources`, `sink`,
/// `transforms`, `job_file`, `partition_by`, ... plus the raw `config`),
/// with every top-level key of the job file that yard does not model
/// (`role`, custom blocks) copied alongside, and the job's `<type>` block
/// replaced by `providers.<type>` from `yard.yaml` deep-merged with the
/// job's own block. The per-job `_aws` block rides along inside it.
///
/// `providers` is the manifest's `providers` map; when it has no block for
/// this job's type the plugin sees `{}` merged with whatever the job
/// declared.
///
/// # Errors
///
/// Returns an error if the job definition cannot be serialized to JSON.
pub fn effective_job_config(
    providers: &HashMap<String, Value>,
    job_def: &JobDefinition,
) -> Result<Value> {
    let mut doc = serde_json::to_value(job_def).context("failed to serialize job definition")?;
    let type_key = job_def.job_type.to_string();
    let provider_defaults = providers.get(&type_key).unwrap_or(&Value::Null);

    let Some(obj) = doc.as_object_mut() else {
        return Ok(doc);
    };

    if let Some(raw) = job_def.config.as_object() {
        for (key, value) in raw {
            obj.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    let mut merged = build_provider_config(provider_defaults, &job_def.config, &type_key);
    if merged.is_null() {
        merged = Value::Object(serde_json::Map::new());
    }
    obj.insert(type_key, merged);

    Ok(doc)
}

/// Merge provider-level defaults with job-level overrides.
///
/// Provider config from yard.yaml is the base, job-level block wins on conflicts.
/// Recurses into nested objects so a job overriding a single key of a nested
/// map (e.g. `glue.default_arguments.--job-language`) preserves the sibling
/// keys the provider defined. Arrays and scalars are replaced wholesale.
#[must_use]
pub fn merge_provider_config(provider_defaults: &Value, job_overrides: &Value) -> Value {
    match (provider_defaults, job_overrides) {
        (Value::Object(base), Value::Object(overrides)) => {
            let mut merged = base.clone();
            for (key, val) in overrides {
                let next = match merged.get(key) {
                    Some(existing) => merge_provider_config(existing, val),
                    None => val.clone(),
                };
                merged.insert(key.clone(), next);
            }
            Value::Object(merged)
        }
        (_, Value::Null) => provider_defaults.clone(),
        _ => job_overrides.clone(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merge_provider_config_job_overrides_defaults() {
        let defaults = json!({
            "script_bucket": "my-bucket",
            "worker_type": "G.1X",
            "number_of_workers": 2,
            "glue_version": "4.0"
        });
        let overrides = json!({
            "worker_type": "G.2X",
            "number_of_workers": 10,
            "timeout": 180
        });

        let merged = merge_provider_config(&defaults, &overrides);

        // Overrides win
        assert_eq!(merged["worker_type"], "G.2X");
        assert_eq!(merged["number_of_workers"], 10);
        assert_eq!(merged["timeout"], 180);
        // Defaults preserved
        assert_eq!(merged["script_bucket"], "my-bucket");
        assert_eq!(merged["glue_version"], "4.0");
    }

    #[test]
    fn merge_provider_config_no_overrides() {
        let defaults = json!({"worker_type": "G.1X", "number_of_workers": 2});
        let merged = merge_provider_config(&defaults, &Value::Null);
        assert_eq!(merged, defaults);
    }

    #[test]
    fn merge_provider_config_deep_merges_nested_maps() {
        // Job overrides one key of a nested map; siblings must survive.
        let defaults = json!({
            "default_arguments": {
                "--enable-metrics": "true",
                "--job-language": "python",
                "--TempDir": "s3://temp/"
            }
        });
        let overrides = json!({
            "default_arguments": {
                "--job-language": "scala"
            }
        });
        let merged = merge_provider_config(&defaults, &overrides);
        assert_eq!(merged["default_arguments"]["--job-language"], "scala");
        assert_eq!(merged["default_arguments"]["--enable-metrics"], "true");
        assert_eq!(merged["default_arguments"]["--TempDir"], "s3://temp/");
    }

    #[test]
    fn merge_provider_config_arrays_are_replaced() {
        // Arrays overlay-replace rather than concatenate — keeps semantics
        // predictable (use `key: null` or re-declare the full list to clear).
        let defaults = json!({"connections": ["conn-a", "conn-b"]});
        let overrides = json!({"connections": ["conn-c"]});
        let merged = merge_provider_config(&defaults, &overrides);
        assert_eq!(merged["connections"], json!(["conn-c"]));
    }

    #[test]
    fn merge_provider_config_scalar_overrides_nested() {
        // If a job redeclares a nested map as a scalar (or vice-versa),
        // override wins — we don't try to reconcile mismatched shapes.
        let defaults = json!({"tags": {"team": "data"}});
        let overrides = json!({"tags": "none"});
        let merged = merge_provider_config(&defaults, &overrides);
        assert_eq!(merged["tags"], "none");
    }

    #[test]
    fn merge_provider_config_adds_new_nested_keys() {
        let defaults = json!({"default_arguments": {"--a": "1"}});
        let overrides = json!({"default_arguments": {"--b": "2"}});
        let merged = merge_provider_config(&defaults, &overrides);
        assert_eq!(merged["default_arguments"]["--a"], "1");
        assert_eq!(merged["default_arguments"]["--b"], "2");
    }

    #[test]
    fn merge_provider_config_recurses_multiple_levels() {
        let defaults = json!({"a": {"b": {"c": 1, "d": 2}}});
        let overrides = json!({"a": {"b": {"c": 99}}});
        let merged = merge_provider_config(&defaults, &overrides);
        assert_eq!(merged["a"]["b"]["c"], 99);
        assert_eq!(merged["a"]["b"]["d"], 2);
    }

    // --- effective_job_config ---

    fn glue_job() -> JobDefinition {
        JobDefinition {
            job_type: yard_structs::JobType::Plugin("glue".to_string()),
            sources: vec![yard_structs::Source {
                name: "orders".into(),
                source_type: "s3".into(),
                path: Some("s3://b/raw/".into()),
                ..Default::default()
            }],
            config: json!({
                "type": "glue",
                "role": "arn:aws:iam::1:role/r",
                "sources": [{"name": "orders", "type": "s3", "path": "s3://b/raw/"}],
                "glue": {"worker_type": "G.2X"},
                "_aws": {"assume_role": "arn:aws:iam::1:role/deploy"}
            }),
            ..Default::default()
        }
    }

    #[test]
    fn effective_job_config_nests_merged_provider_block_under_type_key() {
        let providers = HashMap::from([(
            "glue".to_string(),
            json!({"script_bucket": "bkt", "worker_type": "G.1X"}),
        )]);

        let doc = effective_job_config(&providers, &glue_job()).unwrap();

        assert_eq!(doc["glue"]["script_bucket"], "bkt");
        assert_eq!(doc["glue"]["worker_type"], "G.2X", "job block wins");
        assert_eq!(doc["glue"]["_aws"]["assume_role"], "arn:aws:iam::1:role/deploy");
    }

    #[test]
    fn effective_job_config_keeps_typed_fields_and_copies_raw_keys() {
        let doc = effective_job_config(&HashMap::new(), &glue_job()).unwrap();

        // Typed form (what codegen consumes) wins over the raw YAML shape.
        assert_eq!(doc["job_type"], "glue");
        assert_eq!(doc["sources"][0]["source_type"], "s3");
        assert!(doc["sources"][0].get("type").is_none());
        // Raw top-level keys yard does not model are passed through.
        assert_eq!(doc["role"], "arn:aws:iam::1:role/r");
        assert_eq!(doc["type"], "glue");
        // The raw config is still available verbatim.
        assert_eq!(doc["config"]["role"], "arn:aws:iam::1:role/r");
    }

    #[test]
    fn effective_job_config_without_provider_block_yields_job_block_or_empty_object() {
        let doc = effective_job_config(&HashMap::new(), &glue_job()).unwrap();
        assert_eq!(doc["glue"]["worker_type"], "G.2X");

        let bare = JobDefinition {
            job_type: yard_structs::JobType::Plugin("emr".to_string()),
            config: json!({"type": "emr"}),
            ..Default::default()
        };
        let doc = effective_job_config(&HashMap::new(), &bare).unwrap();
        assert_eq!(doc["emr"], json!({}));
    }
}
