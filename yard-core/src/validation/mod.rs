mod rules;
mod syntax;

use yard_structs::{JobDefinition, ValidationError};

// Re-export public API (preserves crate::validation::X paths)
pub use rules::validate_job;
pub use syntax::validate_python_syntax;

// Import for local use in validate_job_full
use rules::err;

use crate::airflow_dag::ResolvedDag;
use std::collections::BTreeSet;
use yard_structs::{SingleSource, Trigger};

/// Validate a job definition and its generated script.
/// Runs schema validation, then generates the script and checks Python syntax.
pub fn validate_job_full(job_name: &str, job_def: &JobDefinition) -> Vec<ValidationError> {
    let mut errors = validate_job(job_def);

    // Only check syntax if schema validation passed — no point generating
    // a script from an invalid config
    if errors.is_empty() {
        match crate::codegen::generate_python_script(job_name, job_def) {
            Ok(script) => {
                if let Some(syntax_err) = validate_python_syntax(&script) {
                    errors.push(err(
                        "script",
                        &format!("generated script has a syntax error: {syntax_err}"),
                    ));
                }
            }
            Err(e) => {
                errors.push(err("script", &format!("failed to generate script: {e}")));
            }
        }
    }

    errors
}

/// Validate a resolved DAG's trigger configuration against TRIG-04..TRIG-06.
/// Caller (orchestrate::plan / orchestrate::apply) iterates `pre_dags` and
/// rolls up errors per DAG. Empty Vec means "DAG passed all four checks".
pub fn validate_dag_full(dag: &ResolvedDag) -> Vec<ValidationError> {
    let cfg = &dag.config;
    let mut errors: Vec<ValidationError> = Vec::new();

    // Rule order locked per CONTEXT D-08 — deterministic output for tests + rollup.
    if let Some(e) = check_mutual_exclusion(cfg.schedule.as_deref(), cfg.trigger.as_ref()) {
        errors.push(e);
    }
    errors.extend(check_empty_composites(cfg.trigger.as_ref()));
    if let Some(e) = check_heterogeneous_any(cfg.trigger.as_ref()) {
        errors.push(e);
    }

    errors
}

/// TRIG-04: top-level schedule and trigger are mutually exclusive.
fn check_mutual_exclusion(
    schedule: Option<&str>,
    trigger: Option<&Trigger>,
) -> Option<ValidationError> {
    if schedule.is_some() && trigger.is_some() {
        Some(err(
            "airflow.trigger",
            "cannot declare both top-level 'schedule:' and 'trigger:' — these are mutually exclusive. To schedule via the trigger block, use 'trigger: { schedule: \"<expr>\" }' and remove the top-level 'schedule:' field.",
        ))
    } else {
        None
    }
}

/// TRIG-05a / TRIG-05b: composite lists must be non-empty.
fn check_empty_composites(trigger: Option<&Trigger>) -> Vec<ValidationError> {
    let mut out: Vec<ValidationError> = Vec::new();
    match trigger {
        Some(Trigger::All(v)) if v.is_empty() => {
            out.push(err(
                "airflow.trigger.all",
                "empty 'all: []' composite — must contain at least one trigger source.",
            ));
        }
        Some(Trigger::Any(v)) if v.is_empty() => {
            out.push(err(
                "airflow.trigger.any",
                "empty 'any: []' composite — must contain at least one trigger source.",
            ));
        }
        _ => {}
    }
    out
}

/// TRIG-06: any: must contain only Dataset sources.
/// Excludes Dataset from the rendered {kinds} list; sorts and dedupes via BTreeSet.
fn check_heterogeneous_any(trigger: Option<&Trigger>) -> Option<ValidationError> {
    let sources = match trigger {
        Some(Trigger::Any(v)) if !v.is_empty() => v,
        _ => return None,
    };
    let kinds: BTreeSet<&'static str> = sources
        .iter()
        .filter(|s| !matches!(s, SingleSource::Dataset(_)))
        .map(|s| s.source_kind())
        .collect();
    if kinds.is_empty() {
        return None;
    }
    let joined = kinds.into_iter().collect::<Vec<_>>().join(", ");
    Some(err(
        "airflow.trigger.any",
        &format!(
            "'any:' supports only homogeneous Dataset sources (found: {joined}). Heterogeneous 'any:' has no clean Airflow primitive in v1.6 — split into separate DAGs per source, or use homogeneous Dataset triggers."
        ),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::airflow_dag::ResolvedDag;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use yard_structs::{
        AirflowSection, DatasetTrigger, JobType, S3Trigger, ScheduleTrigger, Sink, SingleSource,
        Source, SqsTrigger, Transform, Trigger,
    };

    fn valid_glue_job() -> JobDefinition {
        JobDefinition {
            job_type: JobType::Glue,
            imports: vec![],
            body: None,
            job_file: None,
            sources: vec![Source {
                name: "events".to_string(),
                source_type: "s3".to_string(),
                format: Some("parquet".to_string()),
                path: Some("s3://bucket/in/".to_string()),
                connection_url: None,
                table: None,
                database: None,
                secret_id: None,
            ..Default::default()
            }],
            sink: Some(Sink {
                source: None,
                sink_type: "s3".to_string(),
                format: Some("parquet".to_string()),
                path: Some("s3://bucket/out/".to_string()),
                connection_url: None,
                table: None,
                database: None,
                secret_id: None,
                mode: Some("overwrite".to_string()),
                partition_by: vec![],
            fill_nulls: None,
            }),
            transforms: vec![Transform {
                transform_type: "filter".to_string(),
                source: None,
                output: None,
                condition: Some("col('active')".to_string()),
                query: None,
                columns: vec![],
                mapping: HashMap::new(),
                name: None,
                expression: None,
                left: None,
                right: None,
                on: None,
                how: None,
                group_by: vec![],
                aggs: std::collections::HashMap::new(),
                partition_by: vec![],
                order_by: vec![],
            }],
            config: json!({"type": "glue", "role": "arn:aws:iam::123456789:role/GlueRole"}),
            ..Default::default()
        }
    }

    fn minimal_job() -> JobDefinition {
        JobDefinition {
            job_type: JobType::Glue,
            config: json!({"type": "glue", "role": "arn:aws:iam::123456789:role/GlueRole"}),
            ..Default::default()
        }
    }

    // --- Valid job passes ---

    #[test]
    fn valid_job_has_no_errors() {
        let errors = validate_job(&valid_glue_job());
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    // --- Job type ---
    //
    // The `invalid_job_type` test was deleted in Phase 21 plan 21-01: unknown
    // wire strings are now rejected at deserialize time by serde via
    // JobType's `unknown variant` error (covered by
    // `yard_structs::config::tests::job_type_deserialize_unknown_rejects`).
    // Constructing a JobDefinition with an invalid job_type is no longer
    // expressible — JobType is a closed three-variant enum.

    // --- Source types ---

    #[test]
    fn invalid_source_type() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "src".to_string(),
            source_type: "gcs".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "sources[0].type"));
    }

    // --- Source required fields ---

    #[test]
    fn s3_source_missing_path() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "src".to_string(),
            source_type: "s3".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"path\""))
        );
    }

    #[test]
    fn jdbc_source_missing_fields() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "src".to_string(),
            source_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"connection_url\""))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"table\""))
        );
    }

    #[test]
    fn catalog_source_missing_fields() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "src".to_string(),
            source_type: "catalog".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"database\""))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"table\""))
        );
    }

    // --- Transform types ---

    #[test]
    fn invalid_transform_type() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "pivot".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "transforms[0].type"));
    }

    // --- Transform required fields ---

    #[test]
    fn filter_missing_condition() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "filter".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"condition\""))
        );
    }

    #[test]
    fn sql_missing_query() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "sql".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"query\""))
        );
    }

    #[test]
    fn join_missing_fields() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "join".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"left\""))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"right\""))
        );
        assert!(errors.iter().any(|e| e.message.contains("requires \"on\"")));
    }

    #[test]
    fn drop_columns_empty() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "drop_columns".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires non-empty \"columns\""))
        );
    }

    #[test]
    fn rename_empty_mapping() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "rename".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires non-empty \"mapping\""))
        );
    }

    #[test]
    fn add_column_missing_fields() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "add_column".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"name\""))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"expression\""))
        );
    }

    // --- Sink validation ---

    #[test]
    fn invalid_sink_type() {
        let mut job = minimal_job();
        job.sink = Some(Sink {
            source: None,
            sink_type: "bigquery".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "sink.type"));
    }

    #[test]
    fn s3_sink_missing_path() {
        let mut job = minimal_job();
        job.sink = Some(Sink {
            source: None,
            sink_type: "s3".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
        });
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"path\""))
        );
    }

    // --- Reference checking ---

    #[test]
    fn transform_references_unknown_source() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "events".to_string(),
            source_type: "s3".to_string(),
            format: None,
            path: Some("s3://b/in".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        job.transforms = vec![Transform {
            transform_type: "filter".to_string(),
            source: Some("nonexistent".to_string()),
            output: None,
            condition: Some("True".to_string()),
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.field == "transforms[0].source" && e.message.contains("nonexistent"))
        );
    }

    #[test]
    fn sink_references_unknown_source() {
        let mut job = minimal_job();
        job.sink = Some(Sink {
            source: Some("nonexistent".to_string()),
            sink_type: "s3".to_string(),
            format: None,
            path: Some("s3://b/out".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
        });
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.field == "sink.source" && e.message.contains("nonexistent"))
        );
    }

    #[test]
    fn join_references_unknown_sources() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "orders".to_string(),
            source_type: "s3".to_string(),
            format: None,
            path: Some("s3://b/in".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        job.transforms = vec![Transform {
            transform_type: "join".to_string(),
            source: None,
            output: Some("joined".to_string()),
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: Some("orders".to_string()),
            right: Some("ghost".to_string()),
            on: Some("id".to_string()),
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.field == "transforms[0].right" && e.message.contains("ghost"))
        );
    }

    #[test]
    fn transform_output_becomes_known_name() {
        let mut job = minimal_job();
        job.sources = vec![
            Source {
                name: "a".to_string(),
                source_type: "s3".to_string(),
                format: None,
                path: Some("s3://b/a".to_string()),
                connection_url: None,
                table: None,
                database: None,
                secret_id: None,
            ..Default::default()
            },
            Source {
                name: "b".to_string(),
                source_type: "s3".to_string(),
                format: None,
                path: Some("s3://b/b".to_string()),
                connection_url: None,
                table: None,
                database: None,
                secret_id: None,
            ..Default::default()
            },
        ];
        job.transforms = vec![
            Transform {
                transform_type: "join".to_string(),
                source: None,
                output: Some("joined".to_string()),
                condition: None,
                query: None,
                columns: vec![],
                mapping: HashMap::new(),
                name: None,
                expression: None,
                left: Some("a".to_string()),
                right: Some("b".to_string()),
                on: Some("id".to_string()),
                how: None,
                group_by: vec![],
                aggs: std::collections::HashMap::new(),
                partition_by: vec![],
                order_by: vec![],
            },
            Transform {
                transform_type: "filter".to_string(),
                source: Some("joined".to_string()),
                output: None,
                condition: Some("True".to_string()),
                query: None,
                columns: vec![],
                mapping: HashMap::new(),
                name: None,
                expression: None,
                left: None,
                right: None,
                on: None,
                how: None,
                group_by: vec![],
                aggs: std::collections::HashMap::new(),
                partition_by: vec![],
                order_by: vec![],
            },
        ];
        job.sink = Some(Sink {
            source: Some("joined".to_string()),
            sink_type: "s3".to_string(),
            format: None,
            path: Some("s3://b/out".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
        });
        let errors = validate_job(&job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    // --- Multiple errors collected ---

    #[test]
    fn collects_all_errors() {
        // Note: prior to Phase 21 plan 21-01 this test mutated job.job_type to
        // "unknown" to also assert a job-type validation error. That arm now
        // lives at deserialize time (serde unknown-variant rejection on
        // JobType) and cannot be exercised by mutating a typed JobDefinition.
        // The test still validates that source/transform/sink errors collect
        // independently and don't short-circuit.
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "src".to_string(),
            source_type: "gcs".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        job.transforms = vec![Transform {
            transform_type: "pivot".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        job.sink = Some(Sink {
            source: None,
            sink_type: "bigquery".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
        });
        let errors = validate_job(&job);
        // Down from `>= 3` after Phase 21 21-01 deleted the job-type
        // validation arm (now enforced upstream by serde at deserialize).
        assert!(errors.len() >= 2);
        assert!(errors.iter().any(|e| e.field == "sources[0].type"));
        assert!(errors.iter().any(|e| e.field == "sink.type"));
    }

    // --- Glue config validation ---

    #[test]
    fn valid_glue_config_passes() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
            "role": "arn:aws:iam::123456789:role/GlueRole",
            "glue": {
                "worker_type": "G.2X",
                "number_of_workers": 10,
                "glue_version": "4.0",
                "timeout": 120,
                "max_retries": 1,
                "bookmark": "enabled",
                "connections": ["my-conn"],
                "default_arguments": {"--key": "value"}
            }
        });
        let errors = validate_job(&job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn invalid_worker_type() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
            "glue": { "worker_type": "G.99X" }
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "glue.worker_type"));
    }

    #[test]
    fn invalid_number_of_workers() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
            "glue": { "number_of_workers": 0 }
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "glue.number_of_workers"));
    }

    #[test]
    fn invalid_glue_version() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
            "glue": { "glue_version": "2.0" }
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "glue.glue_version"));
    }

    #[test]
    fn invalid_bookmark_value() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
            "glue": { "bookmark": "maybe" }
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "glue.bookmark"));
    }

    #[test]
    fn negative_timeout_rejected() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
            "glue": { "timeout": 0 }
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "glue.timeout"));
    }

    #[test]
    fn no_glue_block_is_fine() {
        let job = minimal_job();
        let errors = validate_job(&job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    // --- Glue role validation ---

    #[test]
    fn glue_job_missing_role() {
        let mut job = minimal_job();
        job.config = json!({"type": "glue"});
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "role"),
            "Expected role error, got: {:?}",
            errors
        );
    }

    #[test]
    fn glue_job_empty_role() {
        let mut job = minimal_job();
        job.config = json!({"type": "glue", "role": ""});
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "role"),
            "Expected role error, got: {:?}",
            errors
        );
    }

    #[test]
    fn glue_job_with_role_passes() {
        let job = minimal_job();
        let errors = validate_job(&job);
        assert!(
            !errors.iter().any(|e| e.field == "role"),
            "Unexpected role error: {:?}",
            errors
        );
    }

    // --- Python syntax validation ---

    #[test]
    fn valid_python_passes_syntax_check() {
        let script = "x = 1\nprint(x)\n";
        assert!(validate_python_syntax(script).is_none());
    }

    #[test]
    fn invalid_python_fails_syntax_check() {
        let script = "def foo(\n";
        let result = validate_python_syntax(script);
        assert!(result.is_some());
        assert!(
            result.as_ref().is_some_and(|m| m.contains("SyntaxError")),
            "Expected SyntaxError, got: {:?}",
            result
        );
    }

    #[test]
    fn validate_job_full_catches_bad_body() {
        let mut job = minimal_job();
        job.body = Some("def broken(\n".to_string());
        let errors = validate_job_full("bad_body", &job);
        assert!(
            errors.iter().any(|e| e.field == "script"),
            "Expected script error, got: {:?}",
            errors
        );
    }

    #[test]
    fn validate_job_full_passes_valid_job() {
        let job = valid_glue_job();
        let errors = validate_job_full("good_job", &job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn body_and_job_file_mutually_exclusive() {
        let mut job = minimal_job();
        job.body = Some("print('hi')".to_string());
        job.job_file = Some("./custom.py".to_string());
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.field == "job_file" && e.message.contains("cannot specify both")),
            "Expected mutual exclusion error, got: {:?}",
            errors
        );
    }

    #[test]
    fn validate_job_full_catches_bad_job_file() {
        let dir = std::env::temp_dir().join(format!("yard_vjf_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let bad_script = dir.join("bad.py");
        std::fs::write(&bad_script, "def broken(\n").unwrap();

        let mut job = minimal_job();
        job.job_file = Some(bad_script.to_string_lossy().to_string());

        let errors = validate_job_full("bad_file_job", &job);
        assert!(
            errors.iter().any(|e| e.field == "script"),
            "Expected script syntax error, got: {:?}",
            errors
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_job_full_passes_good_job_file() {
        let dir = std::env::temp_dir().join(format!("yard_vjfg_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let good_script = dir.join("good.py");
        std::fs::write(&good_script, "print('hello')\n").unwrap();

        let mut job = minimal_job();
        job.job_file = Some(good_script.to_string_lossy().to_string());

        let errors = validate_job_full("good_file_job", &job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- bash task type ---

    fn bash_job(command: Option<&str>) -> JobDefinition {
        let mut cfg = json!({"type": "bash"});
        if let Some(c) = command {
            cfg.as_object_mut()
                .expect("object")
                .insert("command".to_string(), json!(c));
        }
        JobDefinition {
            job_type: JobType::Bash,
            config: cfg,
            ..Default::default()
        }
    }

    #[test]
    fn bash_valid_with_command() {
        let job = bash_job(Some("echo hello"));
        let errors = validate_job(&job);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn bash_missing_command_errors() {
        let job = bash_job(None);
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "command"),
            "expected command error, got: {errors:?}"
        );
    }

    #[test]
    fn bash_empty_command_errors() {
        let job = bash_job(Some("   "));
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "command"));
    }

    #[test]
    fn bash_rejects_sources() {
        let mut job = bash_job(Some("echo hi"));
        job.sources = vec![Source {
            name: "s".to_string(),
            source_type: "s3".to_string(),
            format: None,
            path: Some("s3://b/x".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "sources"));
    }

    #[test]
    fn bash_rejects_sink() {
        let mut job = bash_job(Some("echo hi"));
        job.sink = Some(Sink {
            source: None,
            sink_type: "s3".to_string(),
            format: None,
            path: Some("s3://b/out".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "sink"));
    }

    #[test]
    fn bash_passes_validate_job_full() {
        // validate_job_full runs generate_python_script + validate_python_syntax.
        // Bash jobs return an empty script, which must parse as valid Python.
        let job = bash_job(Some("echo hello"));
        let errors = validate_job_full("bash_task", &job);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
    }

    #[test]
    fn bash_rejects_transforms_body_job_file() {
        let mut job = bash_job(Some("echo hi"));
        job.body = Some("pass".to_string());
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.field == "body"));
    }

    // --- aggregate ---

    #[test]
    fn aggregate_requires_group_by_and_aggs() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "aggregate".to_string(),
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires non-empty \"group_by\""))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires non-empty \"aggs\""))
        );
    }

    #[test]
    fn aggregate_valid() {
        let mut job = minimal_job();
        let mut aggs = HashMap::new();
        aggs.insert("total".to_string(), "sum(amount)".to_string());
        job.transforms = vec![Transform {
            transform_type: "aggregate".to_string(),
            group_by: vec!["region".to_string()],
            aggs,
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            !errors
                .iter()
                .any(|e| e.field.starts_with("transforms[0]")),
            "unexpected errors: {errors:?}"
        );
    }

    // --- window ---

    #[test]
    fn window_requires_name_expression_and_partition_or_order() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "window".to_string(),
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.message.contains("requires \"name\"")));
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("requires \"expression\""))
        );
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("\"partition_by\" or \"order_by\""))
        );
    }

    #[test]
    fn window_valid_with_partition_only() {
        let mut job = minimal_job();
        job.transforms = vec![Transform {
            transform_type: "window".to_string(),
            name: Some("row_num".to_string()),
            expression: Some("row_number()".to_string()),
            partition_by: vec!["customer_id".to_string()],
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            !errors
                .iter()
                .any(|e| e.field.starts_with("transforms[0]")),
            "unexpected errors: {errors:?}"
        );
    }

    // --- validate_dag_full (Phase 29 — TRIG-04..TRIG-06 trigger validation, D-23) ---

    fn dag_with(section: AirflowSection) -> ResolvedDag {
        // Construct a minimal ResolvedDag for validator unit tests.
        // ResolvedDag derives ONLY #[derive(Debug, Clone)] — NOT Default — so every
        // field is enumerated explicitly. validate_dag_full reads only `dag.name`
        // and `dag.config`; the other fields are populated with empty values.
        // AirflowSection DOES derive Default, so the caller may use
        // `..Default::default()` when constructing the `section` argument.
        ResolvedDag {
            name: "test_dag".to_string(),
            dir: PathBuf::new(),
            config: section,
            tasks: Vec::new(),
            depends_on: BTreeMap::new(),
        }
    }

    fn s3_src() -> SingleSource {
        SingleSource::S3(S3Trigger {
            bucket: "b".into(),
            ..Default::default()
        })
    }
    fn sqs_src() -> SingleSource {
        SingleSource::Sqs(SqsTrigger {
            queue_url: "q".into(),
            wait_time_seconds: None,
            max_messages: None,
            delete_message_on_reception: None,
        })
    }
    fn ds_src(uri: &str) -> SingleSource {
        SingleSource::Dataset(DatasetTrigger { uri: uri.into() })
    }

    #[test]
    fn validate_dag_full_passes_schedule_only() {
        let dag = dag_with(AirflowSection {
            schedule: Some("@daily".into()),
            trigger: None,
            ..Default::default()
        });
        assert!(validate_dag_full(&dag).is_empty());
    }

    #[test]
    fn validate_dag_full_passes_trigger_only() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::Single(SingleSource::Schedule(ScheduleTrigger {
                value: "@daily".into(),
            }))),
            ..Default::default()
        });
        assert!(validate_dag_full(&dag).is_empty());
    }

    #[test]
    fn validate_dag_full_passes_homogeneous_dataset_any() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::Any(vec![ds_src("a"), ds_src("b")])),
            ..Default::default()
        });
        assert!(validate_dag_full(&dag).is_empty());
    }

    #[test]
    fn validate_dag_full_passes_heterogeneous_all() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::All(vec![s3_src(), sqs_src()])),
            ..Default::default()
        });
        assert!(
            validate_dag_full(&dag).is_empty(),
            "heterogeneous all: must be allowed (DS-04)"
        );
    }

    #[test]
    fn validate_dag_full_rejects_schedule_and_trigger() {
        let dag = dag_with(AirflowSection {
            schedule: Some("@daily".into()),
            trigger: Some(Trigger::Single(ds_src("u"))),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "airflow.trigger");
        assert_eq!(
            errs[0].message,
            "cannot declare both top-level 'schedule:' and 'trigger:' — these are mutually exclusive. To schedule via the trigger block, use 'trigger: { schedule: \"<expr>\" }' and remove the top-level 'schedule:' field."
        );
    }

    #[test]
    fn validate_dag_full_rejects_empty_all() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::All(vec![])),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "airflow.trigger.all");
        assert_eq!(
            errs[0].message,
            "empty 'all: []' composite — must contain at least one trigger source."
        );
    }

    #[test]
    fn validate_dag_full_rejects_empty_any() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::Any(vec![])),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "airflow.trigger.any");
        assert_eq!(
            errs[0].message,
            "empty 'any: []' composite — must contain at least one trigger source."
        );
    }

    #[test]
    fn validate_dag_full_rejects_heterogeneous_any_single_source() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::Any(vec![s3_src()])),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "airflow.trigger.any");
        assert_eq!(
            errs[0].message,
            "'any:' supports only homogeneous Dataset sources (found: s3). Heterogeneous 'any:' has no clean Airflow primitive in v1.6 — split into separate DAGs per source, or use homogeneous Dataset triggers."
        );
    }

    #[test]
    fn validate_dag_full_rejects_heterogeneous_any_multi_kind() {
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::Any(vec![s3_src(), sqs_src()])),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "airflow.trigger.any");
        assert_eq!(
            errs[0].message,
            "'any:' supports only homogeneous Dataset sources (found: s3, sqs). Heterogeneous 'any:' has no clean Airflow primitive in v1.6 — split into separate DAGs per source, or use homogeneous Dataset triggers."
        );
    }

    #[test]
    fn validate_dag_full_rejects_heterogeneous_any_with_dataset() {
        // Per D-18 example: Dataset is the allowed source — excluded from {kinds}.
        let dag = dag_with(AirflowSection {
            schedule: None,
            trigger: Some(Trigger::Any(vec![s3_src(), ds_src("u")])),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].field, "airflow.trigger.any");
        assert_eq!(
            errs[0].message,
            "'any:' supports only homogeneous Dataset sources (found: s3). Heterogeneous 'any:' has no clean Airflow primitive in v1.6 — split into separate DAGs per source, or use homogeneous Dataset triggers."
        );
    }

    #[test]
    fn validate_dag_full_accumulates_multiple_errors() {
        // Per D-09 + D-23: schedule + trigger { any: [] } -> exactly two errors.
        let dag = dag_with(AirflowSection {
            schedule: Some("@daily".into()),
            trigger: Some(Trigger::Any(vec![])),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 2, "expected TRIG-04 + TRIG-05b, got: {errs:?}");
        assert_eq!(errs[0].field, "airflow.trigger"); // TRIG-04 first per D-08
        assert_eq!(errs[1].field, "airflow.trigger.any"); // TRIG-05b second
    }

    // --- Phase 30 plan 30-01: TRIG-08 + CONC-02 ---

    #[test]
    fn validate_dag_full_rejects_max_active_runs_zero() {
        let dag = dag_with(AirflowSection {
            max_active_runs: Some(0),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1, "expected exactly one CONC-02 error: {errs:?}");
        assert_eq!(errs[0].field, "airflow.max_active_runs");
        assert_eq!(errs[0].message, "must be >= 1");
    }

    #[test]
    fn validate_dag_full_passes_max_active_runs_one() {
        let dag = dag_with(AirflowSection {
            max_active_runs: Some(1),
            ..Default::default()
        });
        assert!(validate_dag_full(&dag).is_empty());
    }

    #[test]
    fn validate_dag_full_passes_max_active_runs_none() {
        let dag = dag_with(AirflowSection {
            max_active_runs: None,
            ..Default::default()
        });
        assert!(validate_dag_full(&dag).is_empty());
    }

    #[test]
    fn validate_dag_full_rejects_reserved_task_id_yard_wait_dataset() {
        let mut dag = dag_with(AirflowSection {
            trigger: Some(Trigger::Single(ds_src("s3://x"))),
            ..Default::default()
        });
        dag.tasks = vec!["_yard_wait_dataset".to_string()];
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1, "expected exactly one TRIG-08 error: {errs:?}");
        assert_eq!(errs[0].field, "airflow.tasks");
        assert_eq!(
            errs[0].message,
            "task_id '_yard_wait_dataset' is reserved for trigger codegen — rename your task"
        );
    }

    #[test]
    fn validate_dag_full_rejects_reserved_task_id_yard_join_for_heterogeneous_all() {
        let mut dag = dag_with(AirflowSection {
            trigger: Some(Trigger::All(vec![s3_src(), sqs_src()])),
            ..Default::default()
        });
        dag.tasks = vec!["_yard_join".to_string()];
        let errs = validate_dag_full(&dag);
        assert_eq!(errs.len(), 1, "expected exactly one TRIG-08 error: {errs:?}");
        assert_eq!(errs[0].field, "airflow.tasks");
        assert_eq!(
            errs[0].message,
            "task_id '_yard_join' is reserved for trigger codegen — rename your task"
        );
    }

    #[test]
    fn validate_dag_full_passes_when_no_trigger_present_with_reserved_id_named_task() {
        // TRIG-08 only fires when a trigger is present. Schedule-only DAGs
        // can name a task `_yard_wait_s3` without conflict (we don't emit
        // any reserved IDs in that codegen path).
        let mut dag = dag_with(AirflowSection {
            schedule: Some("@daily".into()),
            trigger: None,
            ..Default::default()
        });
        dag.tasks = vec!["_yard_wait_s3".to_string()];
        assert!(validate_dag_full(&dag).is_empty());
    }

    #[test]
    fn validate_dag_full_accumulates_phase29_and_phase30_errors() {
        // Sanity-check rule independence: TRIG-04 (mutual exclusion) AND
        // CONC-02 (max_active_runs >= 1) both fire — no short-circuit.
        let dag = dag_with(AirflowSection {
            schedule: Some("@daily".into()),
            trigger: Some(Trigger::Single(s3_src())),
            max_active_runs: Some(0),
            ..Default::default()
        });
        let errs = validate_dag_full(&dag);
        assert_eq!(
            errs.len(),
            2,
            "expected TRIG-04 + CONC-02, got: {errs:?}"
        );
        assert_eq!(errs[0].field, "airflow.trigger"); // TRIG-04 first per locked order
        assert_eq!(errs[1].field, "airflow.max_active_runs"); // CONC-02 last (Phase 30 appended rules)
        assert_eq!(errs[1].message, "must be >= 1");
    }
}
