use serde_json::Value;
use yard_structs::JobType;

/// Job types that are Airflow tasks only — they don't have a Spark artifact to
/// generate and no provider to deploy through. Used in validation, codegen,
/// and apply to short-circuit the Spark path. **Single source of truth** —
/// callers must use this helper instead of hard-coding the list.
pub fn is_task_only(job_type: JobType) -> bool {
    matches!(job_type, JobType::Bash)
}

/// Build the `Value` passed to `get_provider`: provider defaults shallow-
/// merged with the job's `<job_type>:` block, plus the per-job `_aws` block
/// (resolved at discovery time) injected alongside.
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

/// Merge provider-level defaults with job-level overrides.
/// Provider config from yard.yaml is the base, job-level block wins on conflicts.
/// Recurses into nested objects so a job overriding a single key of a nested
/// map (e.g. `glue.default_arguments.--job-language`) preserves the sibling
/// keys the provider defined. Arrays and scalars are replaced wholesale.
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

    // --- is_task_only ---

    #[test]
    fn is_task_only_recognizes_bash() {
        assert!(is_task_only(JobType::Bash));
    }

    #[test]
    fn is_task_only_rejects_spark_types() {
        // The "unknown" assertion present before Phase 21 plan 21-01 is gone:
        // JobType is a closed three-variant enum, so an "unknown" value is not
        // expressible — that surface is now serde-deserialize-time and is
        // covered by `yard_structs::config::tests::job_type_deserialize_unknown_rejects`.
        assert!(!is_task_only(JobType::Glue));
        assert!(!is_task_only(JobType::Emr));
    }
}
