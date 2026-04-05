use std::collections::HashSet;
use std::process::Command;
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

    // body and job_file are mutually exclusive
    if job.body.is_some() && job.job_file.is_some() {
        errors.push(err(
            "job_file",
            "cannot specify both \"body\" and \"job_file\"",
        ));
    }

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
                    errors.push(err(&prefix.to_string(), "type \"s3\" requires \"path\""));
                }
            }
            "jdbc" => {
                if source.connection_url.is_none() {
                    errors.push(err(
                        &prefix.to_string(),
                        "type \"jdbc\" requires \"connection_url\"",
                    ));
                }
                if source.table.is_none() {
                    errors.push(err(&prefix.to_string(), "type \"jdbc\" requires \"table\""));
                }
            }
            "catalog" => {
                if source.database.is_none() {
                    errors.push(err(
                        &prefix.to_string(),
                        "type \"catalog\" requires \"database\"",
                    ));
                }
                if source.table.is_none() {
                    errors.push(err(
                        &prefix.to_string(),
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
        if let Some(ref src) = transform.source
            && !known_names.contains(src)
        {
            errors.push(err(
                &format!("{prefix}.source"),
                &format!(
                    "\"{}\" does not reference a known source or transform output",
                    src
                ),
            ));
        }

        match transform.transform_type.as_str() {
            "filter" => {
                if transform.condition.is_none() {
                    errors.push(err(
                        &prefix.to_string(),
                        "type \"filter\" requires \"condition\"",
                    ));
                }
            }
            "sql" => {
                if transform.query.is_none() {
                    errors.push(err(&prefix.to_string(), "type \"sql\" requires \"query\""));
                }
            }
            "join" => {
                if let Some(left) = &transform.left {
                    if !known_names.contains(left) {
                        errors.push(err(
                            &format!("{prefix}.left"),
                            &format!(
                                "\"{}\" does not reference a known source or transform output",
                                left
                            ),
                        ));
                    }
                } else {
                    errors.push(err(&prefix.to_string(), "type \"join\" requires \"left\""));
                }
                if let Some(right) = &transform.right {
                    if !known_names.contains(right) {
                        errors.push(err(
                            &format!("{prefix}.right"),
                            &format!(
                                "\"{}\" does not reference a known source or transform output",
                                right
                            ),
                        ));
                    }
                } else {
                    errors.push(err(&prefix.to_string(), "type \"join\" requires \"right\""));
                }
                if transform.on.is_none() {
                    errors.push(err(&prefix.to_string(), "type \"join\" requires \"on\""));
                }
            }
            "drop_columns" | "select" => {
                if transform.columns.is_empty() {
                    errors.push(err(
                        &prefix.to_string(),
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
                        &prefix.to_string(),
                        "type \"rename\" requires non-empty \"mapping\"",
                    ));
                }
            }
            "add_column" => {
                if transform.name.is_none() {
                    errors.push(err(
                        &prefix.to_string(),
                        "type \"add_column\" requires \"name\"",
                    ));
                }
                if transform.expression.is_none() {
                    errors.push(err(
                        &prefix.to_string(),
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

        if let Some(ref src) = sink.source
            && !known_names.contains(src)
        {
            errors.push(err(
                "sink.source",
                &format!(
                    "\"{}\" does not reference a known source or transform output",
                    src
                ),
            ));
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

    // Provider-specific config validation
    if job.job_type == "glue"
        && let Some(glue_config) = job.config.get("glue")
    {
        validate_glue_config(glue_config, &mut errors);
    }

    errors
}

const VALID_WORKER_TYPES: &[&str] = &["G.025X", "G.1X", "G.2X", "G.4X", "G.8X", "Z.2X"];
const VALID_BOOKMARK_VALUES: &[&str] = &["enabled", "disabled"];

fn validate_glue_config(config: &serde_json::Value, errors: &mut Vec<ValidationError>) {
    if let Some(wt) = config.get("worker_type").and_then(|v| v.as_str())
        && !VALID_WORKER_TYPES.contains(&wt)
    {
        errors.push(err(
            "glue.worker_type",
            &format!(
                "\"{}\" is not a valid worker type (expected: {})",
                wt,
                VALID_WORKER_TYPES.join(", ")
            ),
        ));
    }

    if let Some(n) = config.get("number_of_workers").and_then(|v| v.as_i64())
        && n < 1
    {
        errors.push(err("glue.number_of_workers", "must be at least 1"));
    }

    if let Some(v) = config.get("glue_version").and_then(|v| v.as_str())
        && !["3.0", "4.0"].contains(&v)
    {
        errors.push(err(
            "glue.glue_version",
            &format!(
                "\"{}\" is not a supported Glue version (expected: 3.0, 4.0)",
                v
            ),
        ));
    }

    if let Some(t) = config.get("timeout").and_then(|v| v.as_i64())
        && t < 1
    {
        errors.push(err("glue.timeout", "must be at least 1 (minutes)"));
    }

    if let Some(r) = config.get("max_retries").and_then(|v| v.as_i64())
        && r < 0
    {
        errors.push(err("glue.max_retries", "cannot be negative"));
    }

    if let Some(b) = config.get("bookmark").and_then(|v| v.as_str())
        && !VALID_BOOKMARK_VALUES.contains(&b)
    {
        errors.push(err(
            "glue.bookmark",
            &format!(
                "\"{}\" is not valid (expected: {})",
                b,
                VALID_BOOKMARK_VALUES.join(", ")
            ),
        ));
    }

    if let Some(conns) = config.get("connections")
        && !conns.is_array()
    {
        errors.push(err("glue.connections", "must be an array of strings"));
    }

    if let Some(args) = config.get("default_arguments")
        && !args.is_object()
    {
        errors.push(err(
            "glue.default_arguments",
            "must be a map of string keys to string values",
        ));
    }
}

/// Validate that a generated Python script is syntactically valid.
/// Shells out to `python3` using `ast.parse`. Returns None if valid,
/// or Some(error message) if the script has a syntax error.
pub fn validate_python_syntax(script: &str) -> Option<String> {
    let result = Command::new("python3")
        .args(["-c", "import ast, sys; ast.parse(sys.stdin.read())"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn();

    let mut child = match result {
        Ok(child) => child,
        Err(e) => {
            return Some(format!("Failed to run python3: {e}. Is python3 installed?"));
        }
    };

    // Write script to stdin
    if let Some(ref mut stdin) = child.stdin {
        use std::io::Write;
        let _ = stdin.write_all(script.as_bytes());
    }
    // Drop stdin to close the pipe so python reads EOF
    child.stdin.take();

    let output = match child.wait_with_output() {
        Ok(o) => o,
        Err(e) => return Some(format!("Failed to wait on python3: {e}")),
    };

    if output.status.success() {
        None
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Extract just the meaningful part of the syntax error
        let msg = stderr
            .lines()
            .rev()
            .find(|l| l.contains("SyntaxError") || l.contains("Error"))
            .map(|l| l.trim().to_string())
            .unwrap_or_else(|| "Unknown syntax error".to_string());
        Some(msg)
    }
}

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
            job_file: None,
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

    // --- Glue config validation ---

    #[test]
    fn valid_glue_config_passes() {
        let mut job = minimal_job();
        job.config = json!({
            "type": "glue",
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
}
