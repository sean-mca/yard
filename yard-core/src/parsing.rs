use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use yard_structs::{
    AirflowJobBlock, AirflowSection, AwsCredentialConfig, Import, Sink, Source, Transform, Trigger,
};

/// Validate that a JSON object has no keys outside `allowed`. Used by every
/// `parse_*` fn in this file (and by `discover_jobs` / `resolve_project` in
/// resolve.rs) to surface user yard.yaml typos as parse-time errors.
///
/// The structural `path` argument is passed by the caller (which already
/// knows what it's parsing) so error messages are actionable. Path examples:
/// `"jobs.{job_name}.airflow"`, `"yard.yaml.state"`, `"jobs.{job_name}.sink"`.
///
/// **Behavior on non-object Values:** Returns `Ok(())`. The caller's
/// downstream extraction handles non-object inputs via its existing
/// `.get(...).as_str()`-style chain (typically returning `None` and
/// falling through to defaults). This keeps the validator focused on its
/// one job (catching unknown KEYS) without overlapping with type checks.
///
/// **Returned error format** (D-18):
///   `unknown field '<key>' at <path> (allowed: <csv>)`
pub fn validate_unknown_keys(
    value: &serde_json::Value,
    allowed: &[&str],
    path: &str,
) -> anyhow::Result<()> {
    let Some(obj) = value.as_object() else {
        return Ok(());
    };
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            let allowed_csv = allowed.join(", ");
            return Err(anyhow::anyhow!(
                "unknown field '{key}' at {path} (allowed: {allowed_csv})"
            ));
        }
    }
    Ok(())
}

/// Allowed keys on a parsed `airflow:` section block (TYPE-03 D-19).
/// Stricter list than `ALLOWED_AIRFLOW_JOB_BLOCK` — used at the manifest
/// `providers.airflow` site, account.yaml/region.yaml/dag.yaml airflow
/// blocks, and the recursive call from `parse_airflow_job_block` is
/// avoided via the `parse_airflow_section_inner` split.
///
/// `region` is permitted but not stored on `AirflowSection` — it is read
/// directly from the raw `manifest.providers["airflow"]["region"]` value
/// by `dag_lifecycle::extract_airflow_region` (Phase 35 BLOCKER-01).
const ALLOWED_AIRFLOW_SECTION: &[&str] = &[
    "schedule",
    "owner",
    "retries",
    "dags_bucket",
    "dags_prefix",
    "trigger",
    "publishes",
    "aws",
    "max_active_runs",
    "region",
];

/// Allowed keys on a per-job `airflow:` block (`AirflowJobBlock` —
/// includes the AirflowSection-flattened fields plus job-specific
/// `depends_on` and `publishes`). `AirflowJobBlock` is excluded from
/// `deny_unknown_fields` because of `#[serde(flatten)]`; this validator
/// covers its user-yaml typo path per D-CTX:177.
///
/// `region` mirrors `ALLOWED_AIRFLOW_SECTION` (Phase 35 BLOCKER-01).
/// At the per-job scope it is currently inert — `extract_airflow_region`
/// only reads `manifest.providers["airflow"]["region"]` — but accepting
/// it here keeps the invariant that every section key is also valid on
/// a per-job block (locked by the
/// `allowed_airflow_job_block_extends_section_with_depends_on_and_publishes`
/// test).
const ALLOWED_AIRFLOW_JOB_BLOCK: &[&str] = &[
    "depends_on",
    "publishes",
    "schedule",
    "owner",
    "retries",
    "dags_bucket",
    "dags_prefix",
    "trigger",
    "aws",
    "max_active_runs",
    "region",
];

/// Allowed keys on a single source entry (or single-source `source:` block).
const ALLOWED_SOURCE: &[&str] = &[
    "name",
    "type",
    "format",
    "path",
    "connection_url",
    "table",
    "database",
    "secret_id",
    "engine",
    "connection_type",
    "topic",
    "url",
    "headers",
    "options",
];

/// Allowed keys on a `sink:` block.
const ALLOWED_SINK: &[&str] = &[
    "source",
    "type",
    "format",
    "path",
    "connection_url",
    "table",
    "database",
    "secret_id",
    "mode",
    "partition_by",
    "fill_nulls",
];

/// Allowed keys on a single transform entry inside the `transforms:` array.
const ALLOWED_TRANSFORM: &[&str] = &[
    "type",
    "source",
    "output",
    "condition",
    "query",
    "columns",
    "mapping",
    "name",
    "expression",
    "left",
    "right",
    "on",
    "how",
    "group_by",
    "aggs",
    "partition_by",
    "order_by",
];

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

/// Inner extraction for [`AirflowSection`] without unknown-key validation.
/// Validation happens in the public callers so they can choose the right
/// allow-list for their structural context — `parse_airflow_section` uses
/// the strict `ALLOWED_AIRFLOW_SECTION` list, while `parse_airflow_job_block`
/// uses the wider `ALLOWED_AIRFLOW_JOB_BLOCK` list (which adds `depends_on`
/// and `publishes`). Without this split the per-job validator would reject
/// `depends_on` as unknown when delegating, or the standalone path would
/// silently accept those keys.
fn parse_airflow_section_inner(value: &Value) -> Result<AirflowSection> {
    let retries = value
        .get("retries")
        .and_then(|v| v.as_i64())
        .map(|v| v as i32);
    Ok(AirflowSection {
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
        trigger: parse_trigger_field(value, "trigger")?,
        publishes: str_array_field(value, "publishes"),
        // Optional per-DAG-bucket AWS creds sub-block (Phase 9 D-05).
        // Best-effort typed parse — malformed `aws:` blocks fall through
        // silently to None, preserving today's permissive behavior here.
        // (Strict typo gating for the user yard.yaml `aws:` block sits at
        // the structural extraction layer above via `validate_unknown_keys`.)
        aws: value
            .get("aws")
            .cloned()
            .and_then(|v| serde_json::from_value::<AwsCredentialConfig>(v).ok()),
        // CONC-01 / D-13: optional DAG-level Airflow knob. None preserves
        // Airflow's default of 16 for schedule-only DAGs; CONC-01's
        // event-driven default-to-1 fires at codegen time when None.
        // CONC-02 enforces `>= 1` at validate_dag_full.
        max_active_runs: value
            .get("max_active_runs")
            .and_then(|v| v.as_u64())
            .and_then(|n| u32::try_from(n).ok()),
    })
}

/// Parse a `trigger:` field from a YAML/JSON object into Option<Trigger>.
/// Returns None if the key is absent. Errors if present-but-malformed.
///
/// Used by `parse_airflow_section_inner` to deserialize the typed Trigger
/// model (Phase 28). Forwards to the hand-rolled `Trigger::Deserialize`
/// impl in yard-structs/src/trigger.rs which emits the actionable
/// `unknown trigger source 'X' — valid: ...` error on typos.
fn parse_trigger_field(value: &Value, key: &str) -> Result<Option<Trigger>> {
    let Some(v) = value.get(key) else {
        return Ok(None);
    };
    let parsed: Option<Trigger> = serde_json::from_value(v.clone())
        .map_err(|e| anyhow::anyhow!("invalid 'trigger:' shape: {e}"))?;
    Ok(parsed)
}

/// Parse an `airflow:` section body into an [`AirflowSection`]. The same
/// parser is used at every layer of the inheritance chain (yard.yaml
/// `providers.airflow`, `account.yaml` / `region.yaml` / `dag.yaml` airflow
/// keys, and the `overrides` nested under a job's `airflow:` block).
///
/// `value` is the object directly under the `airflow:` key (or the object
/// passed as `providers.airflow`). `path` is the structural yaml path the
/// caller is parsing — used in error messages from `validate_unknown_keys`
/// when the section contains an unknown key (TYPE-03 D-19). Returns
/// `Err` on the first unknown field; otherwise returns the parsed section.
pub fn parse_airflow_section(value: &Value, path: &str) -> Result<AirflowSection> {
    // Rename-pointer migration UX (D-21): legacy v1.5 field names get
    // actionable error messages pointing users at the new shape, before
    // the generic unknown-key validator fires. Mirrors the wording of the
    // typed serde Deserialize path on `AirflowSection` in yard-structs.
    if let Some(obj) = value.as_object()
        && obj.contains_key("triggered_by")
    {
        return Err(anyhow::anyhow!(
            "unknown field 'triggered_by' at {path} — use 'trigger: {{ dataset: {{ uri: \"...\" }} }}' instead. \
             For multiple URIs, use 'trigger: {{ all: [{{ dataset: ... }}, ...] }}'. See migration guide."
        ));
    }
    validate_unknown_keys(value, ALLOWED_AIRFLOW_SECTION, path)?;
    parse_airflow_section_inner(value)
}

/// Parse the per-job `airflow:` block, if present. Returns `Ok(None)` if
/// the job has no `airflow:` key. The block mixes task-level fields
/// (`depends_on`, `produces`) with [`AirflowSection`] overrides (schedule,
/// retries, etc.). Unknown keys at this scope produce a parse-time error
/// via `validate_unknown_keys` against `ALLOWED_AIRFLOW_JOB_BLOCK`
/// (the AirflowSection's strict list plus `depends_on`/`produces`).
///
/// `path` is the job-level structural path (e.g. `"jobs.{job_name}"`) —
/// the function appends `.airflow` for its error messages.
pub fn parse_airflow_job_block(config: &Value, path: &str) -> Result<Option<AirflowJobBlock>> {
    let Some(block) = config.get("airflow") else {
        return Ok(None);
    };
    let block_path = format!("{path}.airflow");
    // Rename-pointer migration UX (D-21): legacy v1.5 field names get
    // actionable error messages pointing users at the new shape, before
    // the generic unknown-key validator fires.
    if let Some(obj) = block.as_object() {
        if obj.contains_key("produces") {
            return Err(anyhow::anyhow!(
                "unknown field 'produces' at {block_path} — use 'publishes: [...]' instead. See migration guide."
            ));
        }
        if obj.contains_key("triggered_by") {
            return Err(anyhow::anyhow!(
                "unknown field 'triggered_by' at {block_path} — use 'trigger: {{ dataset: {{ uri: \"...\" }} }}' instead. \
                 For multiple URIs, use 'trigger: {{ all: [{{ dataset: ... }}, ...] }}'. See migration guide."
            ));
        }
    }
    validate_unknown_keys(block, ALLOWED_AIRFLOW_JOB_BLOCK, &block_path)?;
    Ok(Some(AirflowJobBlock {
        depends_on: str_array_field(block, "depends_on"),
        publishes: str_array_field(block, "publishes"),
        // Use the inner extractor so we don't double-validate against the
        // stricter ALLOWED_AIRFLOW_SECTION list — `depends_on`/`publishes`
        // would be rejected as unknowns there.
        overrides: parse_airflow_section_inner(block)?,
    }))
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
        // Trigger is Option<_> — overlay's Some wins; both None means None.
        // Mirrors the v1.5 semantic where `triggered_by: [a, b]` merged with
        // overlay `triggered_by: [c]` produced `[c]` (overlay wins entirely
        // if non-empty). With Option<Trigger>, "overlay's Some wins" is the
        // direct equivalent.
        trigger: overlay.trigger.clone().or_else(|| base.trigger.clone()),
        // Publishes mirrors the old triggered_by/produces vec-merge: overlay
        // wins entirely when non-empty.
        publishes: if overlay.publishes.is_empty() {
            base.publishes.clone()
        } else {
            overlay.publishes.clone()
        },
        // Per-field merge (overlay-wins-on-Some) so each AwsCredentialConfig
        // field cascades independently through yard.yaml → account → region →
        // dag.yaml. Pre-v1.4 behavior was overlay-block-wins-as-atomic, which
        // silently dropped sibling fields (e.g. setting external_id at the dag
        // level erased an inherited assume_role).
        aws: match (base.aws.as_ref(), overlay.aws.as_ref()) {
            (None, None) => None,
            (Some(b), None) => Some(b.clone()),
            (None, Some(o)) => Some(o.clone()),
            (Some(b), Some(o)) => Some(AwsCredentialConfig::merge(b, o)),
        },
        // Same overlay-wins-when-Some semantics as `aws` and `trigger` —
        // most-specific layer's explicit setting takes precedence; None
        // falls through to base.
        max_active_runs: overlay.max_active_runs.or(base.max_active_runs),
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

fn parse_single_source(src: &Value, default_name: &str, path: &str) -> Result<Option<Source>> {
    validate_unknown_keys(src, ALLOWED_SOURCE, path)?;
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

    // `type` is required; if absent the source is malformed — return Ok(None)
    // so the caller can choose to skip (today's behavior) instead of erroring.
    // Unknown-keys ARE errored above; missing `type` is a separate concern
    // handled by validation/rules.rs.
    let Some(source_type) = src.get("type").and_then(|v| v.as_str()) else {
        return Ok(None);
    };

    Ok(Some(Source {
        name: str_field(src, "name").unwrap_or_else(|| default_name.to_string()),
        source_type: source_type.to_string(),
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
    }))
}

/// Extract sources from a job config. Supports both `source:` (single) and `sources:` (list).
///
/// `path` is the job-level structural path (e.g. `"jobs.{job_name}"`); the
/// function appends `.sources[i]` or `.source` for each entry's error path.
pub fn parse_sources(config: &Value, path: &str) -> Result<Vec<Source>> {
    // Try `sources:` (list) first
    if let Some(arr) = config.get("sources").and_then(|v| v.as_array()) {
        let mut out = Vec::with_capacity(arr.len());
        for (i, item) in arr.iter().enumerate() {
            let item_path = format!("{path}.sources[{i}]");
            if let Some(s) = parse_single_source(item, &format!("source_{i}"), &item_path)? {
                out.push(s);
            }
        }
        return Ok(out);
    }
    // Fall back to `source:` (single)
    if let Some(src) = config.get("source") {
        let src_path = format!("{path}.source");
        if let Some(parsed) = parse_single_source(src, "source", &src_path)? {
            return Ok(vec![parsed]);
        }
    }
    Ok(vec![])
}

/// Extract sink configuration from a job config.
///
/// `path` is the job-level structural path; the function appends `.sink`
/// for its error messages.
pub fn parse_sink(config: &Value, path: &str) -> Result<Option<Sink>> {
    let Some(snk) = config.get("sink") else {
        return Ok(None);
    };
    let sink_path = format!("{path}.sink");
    validate_unknown_keys(snk, ALLOWED_SINK, &sink_path)?;
    let Some(sink_type) = snk.get("type").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    Ok(Some(Sink {
        source: str_field(snk, "source"),
        sink_type: sink_type.to_string(),
        format: str_field(snk, "format"),
        path: str_field(snk, "path"),
        connection_url: str_field(snk, "connection_url"),
        table: str_field(snk, "table"),
        database: str_field(snk, "database"),
        secret_id: str_field(snk, "secret_id"),
        mode: str_field(snk, "mode"),
        partition_by: str_array_field(snk, "partition_by"),
        fill_nulls: snk.get("fill_nulls").and_then(|v| v.as_bool()),
    }))
}

/// Extract transforms from a job config.
///
/// `path` is the job-level structural path; per-transform error paths
/// are built as `{path}.transforms[i]`.
pub fn parse_transforms(config: &Value, path: &str) -> Result<Vec<Transform>> {
    let Some(arr) = config.get("transforms").and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };

    let mut transforms = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let item_path = format!("{path}.transforms[{i}]");
        validate_unknown_keys(item, ALLOWED_TRANSFORM, &item_path)?;
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

    Ok(transforms)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
        let s = parse_airflow_section(&v, "test.providers.airflow").unwrap();
        assert_eq!(s.schedule.as_deref(), Some("@daily"));
        assert_eq!(s.owner.as_deref(), Some("data-team"));
        assert_eq!(s.retries, Some(3));
        assert_eq!(s.dags_bucket.as_deref(), Some("my-dags"));
        assert_eq!(s.dags_prefix.as_deref(), Some("airflow/"));
    }

    #[test]
    fn parse_airflow_section_empty_has_no_fields() {
        let s = parse_airflow_section(&json!({}), "test").unwrap();
        assert_eq!(s, AirflowSection::default());
    }

    /// Inverted from `parse_airflow_section_ignores_unknown_fields` after
    /// TYPE-03 wired `validate_unknown_keys` into `parse_airflow_section`.
    /// The OLD behavior (silently accept `future_field`) is exactly the
    /// "user yard.yaml typo" footgun this plan closes.
    #[test]
    fn parse_airflow_section_rejects_unknown_fields() {
        let err = parse_airflow_section(
            &json!({"schedule": "@hourly", "future_field": true}),
            "providers.airflow",
        )
        .expect_err("unknown field must be rejected");
        let msg = format!("{err}");
        assert!(msg.contains("unknown field 'future_field'"), "got: {msg}");
        assert!(msg.contains("at providers.airflow"), "got: {msg}");
    }

    /// Phase 35 BLOCKER-01: `region:` under `providers.airflow:` must be
    /// accepted by the parser. The value is consumed downstream by
    /// `dag_lifecycle::extract_airflow_region`, which reads it directly
    /// from the raw `manifest.providers["airflow"]["region"]` JSON, so
    /// the field is permitted in the allow-list even though
    /// `AirflowSection` does not carry it as a typed field.
    #[test]
    fn parse_airflow_section_accepts_region() {
        let v = json!({
            "region": "us-east-1",
            "dags_bucket": "my-dags",
            "dags_prefix": "dags/",
        });
        let s = parse_airflow_section(&v, "providers.airflow")
            .expect("region must be permitted under providers.airflow");
        assert_eq!(s.dags_bucket.as_deref(), Some("my-dags"));
        assert_eq!(s.dags_prefix.as_deref(), Some("dags/"));
    }

    // --- parse_airflow_job_block ---

    #[test]
    fn parse_airflow_job_block_absent_returns_none() {
        let config = json!({"type": "glue"});
        assert!(
            parse_airflow_job_block(&config, "jobs.foo")
                .unwrap()
                .is_none()
        );
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
        let block = parse_airflow_job_block(&config, "jobs.foo")
            .unwrap()
            .expect("expected block");
        assert_eq!(block.depends_on, vec!["customers", "products"]);
        assert_eq!(block.overrides.schedule.as_deref(), Some("@hourly"));
        assert_eq!(block.overrides.retries, Some(5));
    }

    #[test]
    fn parse_airflow_job_block_depends_on_only() {
        let config = json!({"type": "glue", "airflow": {"depends_on": ["a"]}});
        let block = parse_airflow_job_block(&config, "jobs.foo")
            .unwrap()
            .expect("expected block");
        assert_eq!(block.depends_on, vec!["a"]);
        assert_eq!(block.overrides, AirflowSection::default());
    }

    /// `parse_airflow_job_block` uses the wider allow-list so `depends_on`
    /// and `produces` are accepted alongside the AirflowSection keys; an
    /// unknown key is still rejected.
    #[test]
    fn parse_airflow_job_block_rejects_unknown_fields() {
        let config = json!({
            "type": "glue",
            "airflow": {
                "depends_on": ["a"],
                "scheule": "@daily"
            }
        });
        let err = parse_airflow_job_block(&config, "jobs.foo").expect_err("typo must reject");
        let msg = format!("{err}");
        assert!(msg.contains("unknown field 'scheule'"), "got: {msg}");
        assert!(msg.contains("at jobs.foo.airflow"), "got: {msg}");
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

    #[test]
    fn merge_airflow_sections_aws_field_per_field_merge() {
        // Per-field cascade for the aws block: dag.yaml overlay sets only
        // aws_conn_id; assume_role from yard.yaml must survive the merge.
        // (Pre-fix behavior was atomic-block-swap, which dropped assume_role.)
        let project = AirflowSection {
            aws: Some(AwsCredentialConfig {
                assume_role: Some("arn:aws:iam::111111111111:role/Root".to_string()),
                region: Some("us-east-1".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let dag = AirflowSection {
            aws: Some(AwsCredentialConfig {
                aws_conn_id: Some("dag_explicit_conn".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let merged = merge_airflow_sections(&project, &dag);
        let aws = merged.aws.expect("aws should be Some after merge");
        assert_eq!(aws.aws_conn_id.as_deref(), Some("dag_explicit_conn"));
        assert_eq!(
            aws.assume_role.as_deref(),
            Some("arn:aws:iam::111111111111:role/Root"),
            "assume_role from project must survive when dag overlay sets only aws_conn_id"
        );
        assert_eq!(aws.region.as_deref(), Some("us-east-1"));
    }

    // --- validate_unknown_keys (TYPE-03) ---

    #[test]
    fn validate_unknown_keys_accepts_subset() {
        let v = json!({"a": 1, "b": 2});
        assert!(validate_unknown_keys(&v, &["a", "b", "c"], "test.path").is_ok());
    }

    #[test]
    fn validate_unknown_keys_accepts_exact() {
        let v = json!({"a": 1});
        assert!(validate_unknown_keys(&v, &["a"], "test.path").is_ok());
    }

    #[test]
    fn validate_unknown_keys_rejects_unknown() {
        let v = json!({"a": 1, "wat": 2});
        let err = validate_unknown_keys(&v, &["a", "b"], "jobs.foo.airflow").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown field 'wat'"), "got: {msg}");
        assert!(msg.contains("at jobs.foo.airflow"), "got: {msg}");
        assert!(msg.contains("allowed: a, b"), "got: {msg}");
    }

    #[test]
    fn validate_unknown_keys_passes_through_non_object() {
        // Strings, numbers, arrays — not our concern; downstream extraction
        // handles them via its existing `.as_str()`-style chain.
        assert!(validate_unknown_keys(&json!("hello"), &["a"], "p").is_ok());
        assert!(validate_unknown_keys(&json!(42), &["a"], "p").is_ok());
        assert!(validate_unknown_keys(&json!([1, 2, 3]), &["a"], "p").is_ok());
        assert!(validate_unknown_keys(&json!(null), &["a"], "p").is_ok());
    }

    #[test]
    fn validate_unknown_keys_empty_object_is_ok() {
        assert!(validate_unknown_keys(&json!({}), &[], "p").is_ok());
        assert!(validate_unknown_keys(&json!({}), &["a", "b"], "p").is_ok());
    }

    // Sanity tests on the per-extractor allowed-keys lists. Each list is
    // wired into a `parse_*` fn in commit 2; these tests assert the lists
    // hold the expected keys today so future edits don't accidentally drop
    // a key. (They also keep the consts marked-used until commit 2 wires
    // them into actual parsers, satisfying the workspace clippy
    // dead_code gate.)

    #[test]
    fn allowed_airflow_section_has_expected_keys() {
        for k in ["schedule", "owner", "retries", "dags_bucket", "dags_prefix", "trigger", "publishes", "aws"] {
            assert!(
                ALLOWED_AIRFLOW_SECTION.contains(&k),
                "ALLOWED_AIRFLOW_SECTION missing '{k}'"
            );
        }
    }

    #[test]
    fn allowed_airflow_job_block_extends_section_with_depends_on_and_publishes() {
        assert!(ALLOWED_AIRFLOW_JOB_BLOCK.contains(&"depends_on"));
        assert!(ALLOWED_AIRFLOW_JOB_BLOCK.contains(&"publishes"));
        // every section key is also valid on the per-job block
        for k in ALLOWED_AIRFLOW_SECTION {
            assert!(
                ALLOWED_AIRFLOW_JOB_BLOCK.contains(k),
                "ALLOWED_AIRFLOW_JOB_BLOCK missing '{k}'"
            );
        }
    }

    #[test]
    fn allowed_source_has_expected_keys() {
        for k in ["name", "type", "format", "path", "options", "headers"] {
            assert!(ALLOWED_SOURCE.contains(&k), "ALLOWED_SOURCE missing '{k}'");
        }
    }

    #[test]
    fn allowed_sink_has_expected_keys() {
        for k in ["type", "path", "mode", "partition_by", "fill_nulls"] {
            assert!(ALLOWED_SINK.contains(&k), "ALLOWED_SINK missing '{k}'");
        }
    }

    #[test]
    fn allowed_transform_has_expected_keys() {
        for k in ["type", "source", "output", "condition", "query", "columns"] {
            assert!(ALLOWED_TRANSFORM.contains(&k), "ALLOWED_TRANSFORM missing '{k}'");
        }
    }
}
