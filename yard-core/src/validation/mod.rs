//! Job validation orchestration.
//!
//! This module is the public entry point for all validation in yard-core.
//! It re-exports the core validators:
//!
//! - [`validate_job_full`] -- schema validation (codegen-based syntax
//!   checking is now delegated to the plugin via `Provider::validate`)
//!
//! Sub-modules:
//! - `rules` -- per-job schema validation (sources, transforms, sinks)
//! - `syntax` -- Python syntax validation via `python3 ast.parse`

mod rules;
mod syntax;

use yard_structs::{JobDefinition, SchemaResponse, ValidationError};

// Re-export public API (preserves crate::validation::X paths)
pub use rules::validate_job;
pub use rules::validate_job_with_schema;
pub use syntax::validate_python_syntax;

/// Validate a job definition against yard's structural schema.
///
/// Runs schema validation. Script syntax checking is now delegated to the
/// plugin via `Provider::validate` and is not performed in core.
#[must_use]
pub fn validate_job_full(job_name: &str, job_def: &JobDefinition) -> Vec<ValidationError> {
    validate_job_full_with_schema(job_name, job_def, None)
}

/// Validate a job definition with optional provider schema for
/// schema-driven field validation (D-03, D-05).
///
/// Same as [`validate_job_full`] but passes the schema through to
/// [`validate_job_with_schema`] for source/sink type union and
/// schema-driven provider config field checking.
///
/// Script generation and syntax checking are no longer performed in core --
/// they are delegated to the plugin via `Provider::codegen` and
/// `Provider::validate`.
#[must_use]
pub fn validate_job_full_with_schema(
    _job_name: &str,
    job_def: &JobDefinition,
    schema: Option<&SchemaResponse>,
) -> Vec<ValidationError> {
    validate_job_with_schema(job_def, schema)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use yard_structs::{
        JdbcAuth, JobType, RdsIamAuth,
        Sink, Source, Transform,
    };

    fn valid_glue_job() -> JobDefinition {
        JobDefinition {
            job_type: JobType::Plugin("glue".to_string()),
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
            connection_type: None,
            auth: None,
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
            job_type: JobType::Plugin("glue".to_string()),
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

    fn rds_iam(username: Option<&str>) -> JdbcAuth {
        JdbcAuth::RdsIam(RdsIamAuth {
            username: username.map(|u| u.to_string()),
            host: "h.example.com".to_string(),
            port: 5432,
            region: "us-east-1".to_string(),
        })
    }

    fn jdbc_source(secret_id: Option<&str>, auth: Option<JdbcAuth>) -> Source {
        Source {
            name: "src".to_string(),
            source_type: "jdbc".to_string(),
            connection_url: Some("jdbc:postgresql://h:5432/db".to_string()),
            table: Some("public.t".to_string()),
            secret_id: secret_id.map(|s| s.to_string()),
            auth,
            ..Default::default()
        }
    }

    #[test]
    fn jdbc_auth_rds_iam_missing_username_without_secret_errors() {
        let mut job = minimal_job();
        job.sources = vec![jdbc_source(None, Some(rds_iam(None)))];
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.message.contains("\"auth.username\" is required")),
            "expected auth.username-required error, got: {errors:?}"
        );
    }

    #[test]
    fn jdbc_auth_rds_iam_username_and_secret_id_together_errors() {
        let mut job = minimal_job();
        job.sources = vec![jdbc_source(Some("rds-secret"), Some(rds_iam(Some("yard_app"))))];
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.message.contains("\"auth.username\" must not be set")),
            "expected auth.username-conflict error, got: {errors:?}"
        );
    }

    #[test]
    fn jdbc_auth_rds_iam_with_secret_id_and_no_username_ok() {
        let mut job = minimal_job();
        job.sources = vec![jdbc_source(Some("rds-secret"), Some(rds_iam(None)))];
        let errors = validate_job(&job);
        assert!(
            !errors.iter().any(|e| e.field.starts_with("sources[0].auth")),
            "auth-related errors should be empty, got: {errors:?}"
        );
    }

    #[test]
    fn jdbc_auth_rds_iam_alone_with_username_ok() {
        let mut job = minimal_job();
        job.sources = vec![jdbc_source(None, Some(rds_iam(Some("yard_app"))))];
        let errors = validate_job(&job);
        assert!(
            !errors.iter().any(|e| e.field.starts_with("sources[0].auth")),
            "auth-related errors should be empty, got: {errors:?}"
        );
    }

    #[test]
    fn auth_on_non_jdbc_source_errors() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "events".to_string(),
            source_type: "s3".to_string(),
            path: Some("s3://b/in".to_string()),
            auth: Some(rds_iam(Some("yard_app"))),
            ..Default::default()
        }];
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.message.contains("\"auth\" is only supported on jdbc")),
            "expected auth-on-non-jdbc error, got: {errors:?}"
        );
    }

    #[test]
    fn auth_on_non_jdbc_sink_errors() {
        let mut job = minimal_job();
        job.sink = Some(Sink {
            sink_type: "s3".to_string(),
            path: Some("s3://b/out".to_string()),
            auth: Some(rds_iam(Some("yard_app"))),
            ..Default::default()
        });
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "sink.auth" && e.message.contains("only supported on jdbc")),
            "expected sink-auth-on-non-jdbc error, got: {errors:?}"
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
            connection_type: None,
            auth: None,
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
            connection_type: None,
            auth: None,
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
            connection_type: None,
            auth: None,
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
            connection_type: None,
            auth: None,
        });
        let errors = validate_job(&job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    // --- Multiple errors collected ---

    #[test]
    fn collects_all_errors() {
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
            connection_type: None,
            auth: None,
        });
        let errors = validate_job(&job);
        assert!(errors.len() >= 2);
        assert!(errors.iter().any(|e| e.field == "sources[0].type"));
        assert!(errors.iter().any(|e| e.field == "sink.type"));
    }

    // --- body and job_file mutual exclusion ---

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

    // --- mask_pii validation ---

    #[test]
    fn mask_pii_bad_format_rejected() {
        let mut job = minimal_job();
        job.mask_pii = vec![
            "usa_ssn".into(),
            "123BAD".into(),
            "GOOD_ONE".into(),
        ];
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "mask_pii[0]"
                && e.message.contains("not valid SCREAMING_SNAKE_CASE")),
            "expected format error at index 0, got: {errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.field == "mask_pii[1]"
                && e.message.contains("not valid SCREAMING_SNAKE_CASE")),
            "expected format error at index 1, got: {errors:?}"
        );
        assert!(
            !errors.iter().any(|e| e.field == "mask_pii[2]"),
            "GOOD_ONE should pass format check, but got error at index 2: {errors:?}"
        );
    }

    #[test]
    fn mask_pii_empty_string_rejected() {
        let mut job = minimal_job();
        job.mask_pii = vec!["".into(), "USA_SSN".into()];
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "mask_pii[0]"
                && e.message.contains("must not be empty")),
            "expected empty-string error at index 0, got: {errors:?}"
        );
    }

    #[test]
    fn mask_pii_duplicates_rejected() {
        let mut job = minimal_job();
        job.mask_pii = vec![
            "USA_SSN".into(),
            "CREDIT_CARD".into(),
            "USA_SSN".into(),
        ];
        let errors = validate_job(&job);
        assert!(
            errors.iter().any(|e| e.field == "mask_pii"
                && e.message.contains("duplicate entity type 'USA_SSN'")),
            "expected duplicate error for USA_SSN, got: {errors:?}"
        );
    }

    // --- schema-driven provider config validation ---

    #[test]
    fn source_type_union_with_plugin_types() {
        let mut job = minimal_job();
        job.sources = vec![Source {
            name: "custom".to_string(),
            source_type: "databricks_table".to_string(),
            ..Default::default()
        }];
        let errors_no_schema = validate_job_with_schema(&job, None);
        assert!(
            errors_no_schema.iter().any(|e| e.field == "sources[0].type"),
            "expected source type error without schema, got: {errors_no_schema:?}"
        );

        let schema = yard_structs::SchemaResponse {
            fields: vec![],
            supported_source_types: Some(vec!["databricks_table".to_string()]),
            supported_sink_types: None,
        };
        let errors_with_schema = validate_job_with_schema(&job, Some(&schema));
        assert!(
            !errors_with_schema.iter().any(|e| e.field == "sources[0].type"),
            "expected no source type error with schema, got: {errors_with_schema:?}"
        );
    }

    #[test]
    fn sink_type_union_with_plugin_types() {
        let mut job = minimal_job();
        job.sink = Some(Sink {
            sink_type: "databricks_table".to_string(),
            ..Default::default()
        });
        let errors_no_schema = validate_job_with_schema(&job, None);
        assert!(
            errors_no_schema.iter().any(|e| e.field == "sink.type"),
            "expected sink type error without schema, got: {errors_no_schema:?}"
        );

        let schema = yard_structs::SchemaResponse {
            fields: vec![],
            supported_source_types: None,
            supported_sink_types: Some(vec!["databricks_table".to_string()]),
        };
        let errors_with_schema = validate_job_with_schema(&job, Some(&schema));
        assert!(
            !errors_with_schema.iter().any(|e| e.field == "sink.type"),
            "expected no sink type error with schema, got: {errors_with_schema:?}"
        );
    }
}
