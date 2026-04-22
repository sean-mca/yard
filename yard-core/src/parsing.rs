use serde_json::Value;
use std::collections::HashMap;
use yard_structs::{
    AirflowJobBlock, AirflowSection, Import, Sink, Source, Transform,
};

/// Extract optional body override from a job config.
pub fn parse_body(config: &Value) -> Option<String> {
    config
        .get("body")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Extract optional job_file path from a job config.
pub fn parse_job_file(config: &Value) -> Option<String> {
    config
        .get("job_file")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Parse an `airflow:` section body into an [`AirflowSection`]. The same
/// parser is used at every layer of the inheritance chain (yard.yaml
/// `providers.airflow`, `account.yaml` / `region.yaml` / `dag.yaml` airflow
/// keys, and the `overrides` nested under a job's `airflow:` block).
///
/// `value` is the object directly under the `airflow:` key (or the object
/// passed as `providers.airflow`). Unknown fields are ignored — forward
/// compatibility with future PR additions.
pub fn parse_airflow_section(value: &Value) -> AirflowSection {
    let retries = value
        .get("retries")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    AirflowSection {
        schedule: value
            .get("schedule")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        owner: value
            .get("owner")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        retries,
        dags_bucket: value
            .get("dags_bucket")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        dags_prefix: value
            .get("dags_prefix")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        triggered_by: str_array_field(value, "triggered_by"),
        // Optional per-DAG-bucket AWS creds sub-block (Phase 9 D-05).
        // Passed through unchanged to dag_lifecycle where precedence is resolved.
        aws: value.get("aws").cloned().unwrap_or(Value::Null),
    }
}

/// Parse the per-job `airflow:` block, if present. Returns `None` if the job
/// has no `airflow:` key. The block mixes task-level fields (`depends_on`)
/// with [`AirflowSection`] overrides (schedule, retries, etc.).
pub fn parse_airflow_job_block(config: &Value) -> Option<AirflowJobBlock> {
    let block = config.get("airflow")?;
    Some(AirflowJobBlock {
        depends_on: str_array_field(block, "depends_on"),
        produces: str_array_field(block, "produces"),
        overrides: parse_airflow_section(block),
    })
}

/// Shallow-merge two [`AirflowSection`]s: each `Some` field in `overlay`
/// overrides the corresponding field in `base`. Unset fields in `overlay`
/// leave `base` unchanged. Used to compose the inheritance chain
/// `yard.yaml → account → region → dag → job`.
pub fn merge_airflow_sections(base: &AirflowSection, overlay: &AirflowSection) -> AirflowSection {
    AirflowSection {
        schedule: overlay.schedule.clone().or_else(|| base.schedule.clone()),
        owner: overlay.owner.clone().or_else(|| base.owner.clone()),
        retries: overlay.retries.or(base.retries),
        dags_bucket: overlay
            .dags_bucket
            .clone()
            .or_else(|| base.dags_bucket.clone()),
        dags_prefix: overlay
            .dags_prefix
            .clone()
            .or_else(|| base.dags_prefix.clone()),
        triggered_by: if overlay.triggered_by.is_empty() {
            base.triggered_by.clone()
        } else {
            overlay.triggered_by.clone()
        },
        // Overlay wins when non-null; Null means "not set" so fall back to base.
        aws: if overlay.aws.is_null() {
            base.aws.clone()
        } else {
            overlay.aws.clone()
        },
    }
}

/// Extract imports from a job config's "imports" array.
pub fn parse_partition_by(config: &Value) -> Vec<String> {
    config
        .get("partition_by")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_partition_timestamp_column(config: &Value) -> Option<String> {
    config
        .get("partition_timestamp_column")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub fn parse_create_timestamp(config: &Value) -> bool {
    config
        .get("create_timestamp")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

pub fn parse_imports(config: &Value) -> Vec<Import> {
    let mut imports = Vec::new();
    if let Some(arr) = config.get("imports").and_then(|v| v.as_array()) {
        for item in arr {
            let name = match item.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            let from = item
                .get("from")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
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
/// Helper to extract an order_by field: array of {column: string, desc: bool} objects.
fn order_by_field(obj: &Value, key: &str) -> Vec<yard_structs::OrderBySpec> {
    obj.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let column = item.get("column").and_then(|v| v.as_str())?.to_string();
                    let desc = item.get("desc").and_then(|v| v.as_bool()).unwrap_or(false);
                    Some(yard_structs::OrderBySpec { column, desc })
                })
                .collect()
        })
        .unwrap_or_default()
}

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
    let headers = src
        .get("headers")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let options = src
        .get("options")
        .and_then(|v| v.as_object())
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    Some(Source {
        name: str_field(src, "name").unwrap_or_else(|| default_name.to_string()),
        source_type: src.get("type")?.as_str()?.to_string(),
        format: str_field(src, "format"),
        path: str_field(src, "path"),
        connection_url: str_field(src, "connection_url"),
        table: str_field(src, "table"),
        database: str_field(src, "database"),
        secret_id: str_field(src, "secret_id"),
        engine: str_field(src, "engine"),
        connection_type: str_field(src, "connection_type"),
        topic: str_field(src, "topic"),
        url: str_field(src, "url"),
        headers,
        options,
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
    if let Some(src) = config.get("source")
        && let Some(parsed) = parse_single_source(src, "source")
    {
        return vec![parsed];
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
        fill_nulls: snk.get("fill_nulls").and_then(|v| v.as_bool()),
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
            group_by: str_array_field(item, "group_by"),
            aggs: str_map_field(item, "aggs"),
            partition_by: str_array_field(item, "partition_by"),
            order_by: order_by_field(item, "order_by"),
        });
    }

    transforms
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- parse_airflow_section ---

    #[test]
    fn parse_airflow_section_all_fields() {
        let v = json!({
            "schedule": "@daily",
            "owner": "data-team",
            "retries": 3,
            "dags_bucket": "my-dags",
            "dags_prefix": "airflow/"
        });
        let s = parse_airflow_section(&v);
        assert_eq!(s.schedule.as_deref(), Some("@daily"));
        assert_eq!(s.owner.as_deref(), Some("data-team"));
        assert_eq!(s.retries, Some(3));
        assert_eq!(s.dags_bucket.as_deref(), Some("my-dags"));
        assert_eq!(s.dags_prefix.as_deref(), Some("airflow/"));
    }

    #[test]
    fn parse_airflow_section_empty_has_no_fields() {
        let s = parse_airflow_section(&json!({}));
        assert_eq!(s, AirflowSection::default());
    }

    #[test]
    fn parse_airflow_section_ignores_unknown_fields() {
        let s = parse_airflow_section(&json!({"schedule": "@hourly", "future_field": true}));
        assert_eq!(s.schedule.as_deref(), Some("@hourly"));
    }

    // --- parse_airflow_job_block ---

    #[test]
    fn parse_airflow_job_block_absent_returns_none() {
        let config = json!({"type": "glue"});
        assert!(parse_airflow_job_block(&config).is_none());
    }

    #[test]
    fn parse_airflow_job_block_with_depends_on_and_overrides() {
        let config = json!({
            "type": "glue",
            "airflow": {
                "depends_on": ["customers", "products"],
                "schedule": "@hourly",
                "retries": 5
            }
        });
        let block = parse_airflow_job_block(&config).expect("expected block");
        assert_eq!(block.depends_on, vec!["customers", "products"]);
        assert_eq!(block.overrides.schedule.as_deref(), Some("@hourly"));
        assert_eq!(block.overrides.retries, Some(5));
    }

    #[test]
    fn parse_airflow_job_block_depends_on_only() {
        let config = json!({"type": "glue", "airflow": {"depends_on": ["a"]}});
        let block = parse_airflow_job_block(&config).expect("expected block");
        assert_eq!(block.depends_on, vec!["a"]);
        assert_eq!(block.overrides, AirflowSection::default());
    }

    // --- merge_airflow_sections ---

    #[test]
    fn merge_airflow_overlay_wins() {
        let base = AirflowSection {
            schedule: Some("@daily".to_string()),
            owner: Some("base-owner".to_string()),
            retries: Some(2),
            ..Default::default()
        };
        let overlay = AirflowSection {
            schedule: Some("@hourly".to_string()),
            retries: None,
            ..Default::default()
        };
        let merged = merge_airflow_sections(&base, &overlay);
        assert_eq!(merged.schedule.as_deref(), Some("@hourly")); // overlay wins
        assert_eq!(merged.owner.as_deref(), Some("base-owner")); // fallback to base
        assert_eq!(merged.retries, Some(2)); // overlay None -> fallback
    }

    #[test]
    fn merge_airflow_empty_overlay_is_identity() {
        let base = AirflowSection {
            schedule: Some("@daily".to_string()),
            retries: Some(1),
            ..Default::default()
        };
        let merged = merge_airflow_sections(&base, &AirflowSection::default());
        assert_eq!(merged, base);
    }

    #[test]
    fn merge_airflow_chain_three_levels() {
        // Simulates yard.yaml -> region.yaml -> per-job override
        let project = AirflowSection {
            schedule: Some("@daily".to_string()),
            owner: Some("data".to_string()),
            retries: Some(1),
            dags_bucket: Some("proj-bucket".to_string()),
            ..Default::default()
        };
        let region = AirflowSection {
            retries: Some(3), // region overrides retries
            ..Default::default()
        };
        let job = AirflowSection {
            schedule: Some("0 */6 * * *".to_string()), // job overrides schedule
            ..Default::default()
        };
        let after_region = merge_airflow_sections(&project, &region);
        let final_cfg = merge_airflow_sections(&after_region, &job);
        assert_eq!(final_cfg.schedule.as_deref(), Some("0 */6 * * *"));
        assert_eq!(final_cfg.owner.as_deref(), Some("data"));
        assert_eq!(final_cfg.retries, Some(3));
        assert_eq!(final_cfg.dags_bucket.as_deref(), Some("proj-bucket"));
    }
}
