use std::collections::HashSet;
use yard_structs::{JobDefinition, ValidationError};

const SUPPORTED_JOB_TYPES: &[&str] = &["glue"];
const SUPPORTED_SOURCE_TYPES: &[&str] = &["s3", "jdbc", "catalog"];
const SUPPORTED_SINK_TYPES: &[&str] = &["s3", "jdbc", "catalog"];
const SUPPORTED_TRANSFORM_TYPES: &[&str] = &[
    "filter",
    "sql",
    "join",
    "drop_columns",
    "select",
    "rename",
    "add_column",
];

fn err(field: &str, message: &str) -> ValidationError {
    ValidationError {
        field: field.to_string(),
        message: message.to_string(),
    }
}

pub fn validate_job(job: &JobDefinition) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    // Job type
    if !SUPPORTED_JOB_TYPES.contains(&job.job_type.as_str()) {
        errors.push(err(
            "type",
            &format!(
                "\"{}\" is not a supported job type (expected: {})",
                job.job_type,
                SUPPORTED_JOB_TYPES.join(", ")
            ),
        ));
    }

    // Track known df names for reference checking
    let mut known_names: HashSet<String> = HashSet::new();

    // Sources
    for (i, source) in job.sources.iter().enumerate() {
        let prefix = format!("sources[{}]", i);

        if !SUPPORTED_SOURCE_TYPES.contains(&source.source_type.as_str()) {
            errors.push(err(
                &format!("{prefix}.type"),
                &format!(
                    "\"{}\" is not a supported source type (expected: {})",
                    source.source_type,
                    SUPPORTED_SOURCE_TYPES.join(", ")
                ),
            ));
        }

        match source.source_type.as_str() {
            "s3" => {
                if source.path.is_none() {
                    errors.push(err(&format!("{prefix}"), "type \"s3\" requires \"path\""));
                }
            }
            "jdbc" => {
                if source.connection_url.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"jdbc\" requires \"connection_url\"",
                    ));
                }
                if source.table.is_none() {
                    errors.push(err(&format!("{prefix}"), "type \"jdbc\" requires \"table\""));
                }
            }
            "catalog" => {
                if source.database.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"catalog\" requires \"database\"",
                    ));
                }
                if source.table.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"catalog\" requires \"table\"",
                    ));
                }
            }
            _ => {}
        }

        known_names.insert(source.name.clone());
    }

    // Transforms
    for (i, transform) in job.transforms.iter().enumerate() {
        let prefix = format!("transforms[{}]", i);

        if !SUPPORTED_TRANSFORM_TYPES.contains(&transform.transform_type.as_str()) {
            errors.push(err(
                &format!("{prefix}.type"),
                &format!(
                    "\"{}\" is not a supported transform type (expected: {})",
                    transform.transform_type,
                    SUPPORTED_TRANSFORM_TYPES.join(", ")
                ),
            ));
        }

        // Reference checks
        if let Some(ref src) = transform.source {
            if !known_names.contains(src) {
                errors.push(err(
                    &format!("{prefix}.source"),
                    &format!("\"{}\" does not reference a known source or transform output", src),
                ));
            }
        }

        match transform.transform_type.as_str() {
            "filter" => {
                if transform.condition.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"filter\" requires \"condition\"",
                    ));
                }
            }
            "sql" => {
                if transform.query.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"sql\" requires \"query\"",
                    ));
                }
            }
            "join" => {
                if transform.left.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"join\" requires \"left\"",
                    ));
                } else if !known_names.contains(transform.left.as_ref().unwrap()) {
                    errors.push(err(
                        &format!("{prefix}.left"),
                        &format!(
                            "\"{}\" does not reference a known source or transform output",
                            transform.left.as_ref().unwrap()
                        ),
                    ));
                }
                if transform.right.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"join\" requires \"right\"",
                    ));
                } else if !known_names.contains(transform.right.as_ref().unwrap()) {
                    errors.push(err(
                        &format!("{prefix}.right"),
                        &format!(
                            "\"{}\" does not reference a known source or transform output",
                            transform.right.as_ref().unwrap()
                        ),
                    ));
                }
                if transform.on.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"join\" requires \"on\"",
                    ));
                }
            }
            "drop_columns" | "select" => {
                if transform.columns.is_empty() {
                    errors.push(err(
                        &format!("{prefix}"),
                        &format!(
                            "type \"{}\" requires non-empty \"columns\"",
                            transform.transform_type
                        ),
                    ));
                }
            }
            "rename" => {
                if transform.mapping.is_empty() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"rename\" requires non-empty \"mapping\"",
                    ));
                }
            }
            "add_column" => {
                if transform.name.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"add_column\" requires \"name\"",
                    ));
                }
                if transform.expression.is_none() {
                    errors.push(err(
                        &format!("{prefix}"),
                        "type \"add_column\" requires \"expression\"",
                    ));
                }
            }
            _ => {}
        }

        // Register output name for downstream reference checking
        if let Some(ref output) = transform.output {
            known_names.insert(output.clone());
        } else if let Some(ref src) = transform.source {
            // If no output, the transform overwrites its source — already known
            known_names.insert(src.clone());
        }
    }

    // Sink
    if let Some(ref sink) = job.sink {
        if !SUPPORTED_SINK_TYPES.contains(&sink.sink_type.as_str()) {
            errors.push(err(
                "sink.type",
                &format!(
                    "\"{}\" is not a supported sink type (expected: {})",
                    sink.sink_type,
                    SUPPORTED_SINK_TYPES.join(", ")
                ),
            ));
        }

        if let Some(ref src) = sink.source {
            if !known_names.contains(src) {
                errors.push(err(
                    "sink.source",
                    &format!(
                        "\"{}\" does not reference a known source or transform output",
                        src
                    ),
                ));
            }
        }

        match sink.sink_type.as_str() {
            "s3" => {
                if sink.path.is_none() {
                    errors.push(err("sink", "type \"s3\" requires \"path\""));
                }
            }
            "jdbc" => {
                if sink.connection_url.is_none() {
                    errors.push(err("sink", "type \"jdbc\" requires \"connection_url\""));
                }
                if sink.table.is_none() {
                    errors.push(err("sink", "type \"jdbc\" requires \"table\""));
                }
            }
            "catalog" => {
                if sink.database.is_none() {
                    errors.push(err("sink", "type \"catalog\" requires \"database\""));
                }
                if sink.table.is_none() {
                    errors.push(err("sink", "type \"catalog\" requires \"table\""));
                }
            }
            _ => {}
        }
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    use yard_structs::{Sink, Source, Transform};

    fn valid_glue_job() -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
            imports: vec![],
            body: None,
            sources: vec![Source {
                name: "events".to_string(),
                source_type: "s3".to_string(),
                format: Some("parquet".to_string()),
                path: Some("s3://bucket/in/".to_string()),
                connection_url: None,
                table: None,
                database: None,
                secret_id: None,
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
            }],
            config: json!({"type": "glue"}),
        }
    }

    fn minimal_job() -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
            imports: vec![],
            body: None,
            sources: vec![],
            sink: None,
            transforms: vec![],
            config: json!({"type": "glue"}),
        }
    }

    // --- Valid job passes ---

    #[test]
    fn valid_job_has_no_errors() {
        let errors = validate_job(&valid_glue_job());
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    // --- Job type ---

    #[test]
    fn invalid_job_type() {
        let mut job = minimal_job();
        job.job_type = "spark_streaming".to_string();
        let errors = validate_job(&job);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].field == "type");
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
        }];
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.message.contains("requires \"path\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"connection_url\"")));
        assert!(errors.iter().any(|e| e.message.contains("requires \"table\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"database\"")));
        assert!(errors.iter().any(|e| e.message.contains("requires \"table\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"condition\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"query\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"left\"")));
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"right\"")));
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"on\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires non-empty \"columns\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires non-empty \"mapping\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"name\"")));
        assert!(errors
            .iter()
            .any(|e| e.message.contains("requires \"expression\"")));
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
        });
        let errors = validate_job(&job);
        assert!(errors.iter().any(|e| e.message.contains("requires \"path\"")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.field == "transforms[0].source"
                && e.message.contains("nonexistent")));
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
        });
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.field == "sink.source" && e.message.contains("nonexistent")));
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
        }];
        let errors = validate_job(&job);
        assert!(errors
            .iter()
            .any(|e| e.field == "transforms[0].right" && e.message.contains("ghost")));
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
        });
        let errors = validate_job(&job);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    // --- Multiple errors collected ---

    #[test]
    fn collects_all_errors() {
        let mut job = minimal_job();
        job.job_type = "unknown".to_string();
        job.sources = vec![Source {
            name: "src".to_string(),
            source_type: "gcs".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
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
        });
        let errors = validate_job(&job);
        assert!(errors.len() >= 3);
        assert!(errors.iter().any(|e| e.field == "type"));
        assert!(errors.iter().any(|e| e.field == "sources[0].type"));
        assert!(errors.iter().any(|e| e.field == "sink.type"));
    }
}
