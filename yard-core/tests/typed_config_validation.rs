//! Integration test for TYPE-03's user-yard.yaml typo gate. Verifies that
//! representative typos surface as parse-time errors with the expected
//! "unknown field" wording.
//!
//! Layered companion to the inline `mod tests` in `yard-core/src/parsing.rs`
//! and the deny_unknown_fields tests in `yard-structs/src/config.rs`. This
//! file exercises the public API surface of `parsing.rs` from outside the
//! crate so a downstream consumer (yard-cli, yard-server) sees the same
//! contract for the typo-gate's error wording.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use serde_json::json;
use yard_core::parsing::{
    parse_airflow_job_block, parse_airflow_section, parse_sink, parse_sources, parse_transforms,
    validate_unknown_keys,
};

#[test]
fn airflow_section_typo_at_top_level_is_caught() {
    let v = json!({"sceudule": "@daily"});
    let err = parse_airflow_section(&v, "providers.airflow").expect_err("typo must reject");
    let msg = format!("{err}");
    assert!(msg.contains("unknown field 'sceudule'"), "got: {msg}");
    assert!(msg.contains("at providers.airflow"), "got: {msg}");
}

#[test]
fn airflow_section_known_keys_still_parse() {
    // Sanity: the gate only rejects unknowns; subsets / exact-match still parse.
    let v = json!({"schedule": "@daily", "owner": "data-eng"});
    let parsed = parse_airflow_section(&v, "providers.airflow").unwrap();
    assert_eq!(parsed.schedule.as_deref(), Some("@daily"));
    assert_eq!(parsed.owner.as_deref(), Some("data-eng"));
}

#[test]
fn sink_typo_in_partition_by_is_caught() {
    let v = json!({"sink": {"type": "iceberg", "partition_byyy": ["day"]}});
    let err = parse_sink(&v, "jobs.foo").expect_err("typo must reject");
    let msg = format!("{err}");
    assert!(msg.contains("unknown field 'partition_byyy'"), "got: {msg}");
    assert!(msg.contains("at jobs.foo.sink"), "got: {msg}");
}

#[test]
fn sink_known_keys_still_parse() {
    let v = json!({"sink": {"type": "iceberg", "partition_by": ["day"], "fill_nulls": true}});
    let parsed = parse_sink(&v, "jobs.foo").unwrap().expect("sink parses");
    assert_eq!(parsed.sink_type, "iceberg");
    assert_eq!(parsed.partition_by, vec!["day"]);
    assert_eq!(parsed.fill_nulls, Some(true));
}

#[test]
fn transform_typo_at_array_index_includes_index_in_path() {
    let v = json!({"transforms": [
        {"type": "filter", "condition": "x > 0"},
        {"type": "select", "colummmns": ["a"]}
    ]});
    let err = parse_transforms(&v, "jobs.foo").expect_err("typo at index 1 must reject");
    let msg = format!("{err}");
    assert!(msg.contains("unknown field 'colummmns'"), "got: {msg}");
    assert!(msg.contains("at jobs.foo.transforms[1]"), "got: {msg}");
}

#[test]
fn source_typo_in_options_field_is_caught() {
    let v = json!({"sources": [
        {"name": "raw", "type": "s3", "format": "parquet", "compresion": "gzip"}
    ]});
    let err = parse_sources(&v, "jobs.foo").expect_err("typo must reject");
    let msg = format!("{err}");
    assert!(msg.contains("unknown field 'compresion'"), "got: {msg}");
    assert!(msg.contains("at jobs.foo.sources[0]"), "got: {msg}");
}

#[test]
fn airflow_job_block_with_depends_on_and_typo() {
    let v = json!({
        "airflow": {
            "depends_on": ["upstream"],
            "scheule": "@daily" // typo
        }
    });
    let err = parse_airflow_job_block(&v, "jobs.foo").expect_err("typo must reject");
    let msg = format!("{err}");
    assert!(msg.contains("unknown field 'scheule'"), "got: {msg}");
    assert!(msg.contains("at jobs.foo.airflow"), "got: {msg}");
}

#[test]
fn airflow_job_block_depends_on_is_accepted() {
    // Confirms the wider allow-list at the per-job airflow site lets
    // `depends_on` and `produces` through (these are NOT in
    // `ALLOWED_AIRFLOW_SECTION` — the wider list at the per-job block
    // is the only place that admits them).
    let v = json!({
        "airflow": {
            "depends_on": ["upstream"],
            "produces": ["s3://bucket/dataset"],
            "schedule": "@daily"
        }
    });
    let block = parse_airflow_job_block(&v, "jobs.foo")
        .unwrap()
        .expect("block parses");
    assert_eq!(block.depends_on, vec!["upstream"]);
    assert_eq!(block.produces, vec!["s3://bucket/dataset"]);
    assert_eq!(block.overrides.schedule.as_deref(), Some("@daily"));
}

#[test]
fn validate_unknown_keys_directly() {
    let err = validate_unknown_keys(&json!({"x": 1}), &["a", "b"], "test")
        .expect_err("unknown must reject");
    let msg = format!("{err}");
    assert!(msg.contains("unknown field 'x'"), "got: {msg}");
    assert!(msg.contains("allowed: a, b"), "got: {msg}");
    assert!(msg.contains("at test"), "got: {msg}");
}

#[test]
fn validate_unknown_keys_csv_join_is_comma_space() {
    // Locks the exact "comma + space" join character used in error messages
    // (the format consumers see in their console output).
    let err = validate_unknown_keys(&json!({"z": 1}), &["a", "b", "c"], "p")
        .expect_err("must reject");
    let msg = format!("{err}");
    assert!(msg.contains("allowed: a, b, c"), "got: {msg}");
}
