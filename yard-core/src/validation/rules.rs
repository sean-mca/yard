//! Per-job schema validation rules.
//!
//! This module validates individual [`JobDefinition`] structs against
//! yard's schema rules -- checking source types, transform field
//! requirements, sink configuration, Iceberg partitioning, and
//! provider-specific config. Errors are collected (never short-circuit)
//! so users see every violation in a single pass.

use regex::Regex;
use std::collections::HashSet;
use std::sync::LazyLock;
use yard_structs::{JdbcAuth, JobDefinition, SchemaResponse, ValidationError};

/// Supported source types for Spark jobs.
const SUPPORTED_SOURCE_TYPES: &[&str] = &["s3", "jdbc", "catalog", "kafka", "api"];
/// Valid engine values for JDBC sources.
const VALID_ENGINES: &[&str] = &["spark", "glue"];
/// Supported sink types for Spark jobs.
const SUPPORTED_SINK_TYPES: &[&str] = &["s3", "jdbc", "catalog", "iceberg"];
/// Valid time-based partition units for Iceberg.
const VALID_PARTITION_UNITS: &[&str] = &["year", "month", "day"];
/// Valid write modes for Iceberg sinks.
const VALID_ICEBERG_MODES: &[&str] = &["append", "overwrite"];
/// Supported transform types.
const SUPPORTED_TRANSFORM_TYPES: &[&str] = &[
    "filter",
    "sql",
    "join",
    "drop_columns",
    "select",
    "rename",
    "add_column",
    "aggregate",
    "window",
];

/// Regex for SCREAMING_SNAKE_CASE entity types (D-02).
///
/// Alpha-first, no leading/trailing/double underscores, no digits in first
/// position. Examples: `USA_SSN`, `CREDIT_CARD`, `EMAIL`.
static SCREAMING_SNAKE_RE: LazyLock<Regex> = LazyLock::new(|| {
    // SAFETY: this is a compile-time constant pattern that always succeeds.
    #[allow(clippy::expect_used)]
    Regex::new(r"^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$").expect("static regex")
});

/// Single canonical [`ValidationError`] constructor, re-exported from
/// [`crate::providers::validation_err`] so call sites in this module stay terse.
pub use crate::providers::validation_err as err;

/// Validate a job definition against yard's schema rules.
///
/// Delegates to [`validate_job_with_schema`] with `None` schema, preserving
/// backward compatibility for call sites that don't have a schema cache.
#[must_use]
pub fn validate_job(job: &JobDefinition) -> Vec<ValidationError> {
    validate_job_with_schema(job, None)
}

/// Validate a job definition against yard's schema rules with optional
/// provider schema for field checking and source/sink type union (D-03).
///
/// Returns a (possibly empty) list of validation errors. Errors are
/// collected without short-circuiting so every violation is reported.
///
/// When `schema` is `Some`, source/sink type checks union the built-in
/// types with the schema's declared types, and provider config validation
/// uses schema-driven field checking.
#[must_use]
pub fn validate_job_with_schema(
    job: &JobDefinition,
    schema: Option<&SchemaResponse>,
) -> Vec<ValidationError> {
    let mut errors = Vec::with_capacity(4);

    // Note: job-type validity (one of `glue`, `emr`, `bash`) is now enforced
    // by serde at deserialize time via the `JobType` enum's `rename_all =
    // "lowercase"` attribute. Unknown wire strings are rejected at parse time
    // with serde's "unknown variant" error, so the previous
    // `SUPPORTED_JOB_TYPES.contains(...)` arm is no longer reachable here.

    // mask_pii validation (VAL-01/VAL-02/VAL-03).
    //
    // All three checks run independently per D-07 — no short-circuit.
    // In v2.0, mask_pii is validated at the structural level here. The
    // provider-specific job-type restriction (e.g. "glue only") is now the
    // plugin's responsibility via Provider::validate.
    if !job.mask_pii.is_empty() {
        // VAL-02 (D-03/D-16/D-18/D-19): per-element format validation
        for (i, entity) in job.mask_pii.iter().enumerate() {
            if entity.is_empty() {
                errors.push(err(
                    &format!("mask_pii[{i}]"),
                    "entity type must not be empty",
                ));
            } else if !SCREAMING_SNAKE_RE.is_match(entity) {
                errors.push(err(
                    &format!("mask_pii[{i}]"),
                    &format!(
                        "'{entity}' is not valid SCREAMING_SNAKE_CASE (e.g. USA_SSN, CREDIT_CARD)"
                    ),
                ));
            }
        }

        // VAL-03 (D-04/D-17): duplicate detection — exact match, all elements
        // (not just format-valid ones, per Pitfall 5/D-07)
        let mut seen = HashSet::with_capacity(job.mask_pii.len());
        for entity in &job.mask_pii {
            if !seen.insert(entity.as_str()) {
                errors.push(err(
                    "mask_pii",
                    &format!("duplicate entity type '{entity}'"),
                ));
            }
        }
    }

    // body and job_file are mutually exclusive (only relevant for Spark jobs)
    if job.body.is_some() && job.job_file.is_some() {
        errors.push(err(
            "job_file",
            "cannot specify both \"body\" and \"job_file\"",
        ));
    }

    // Build effective source type list — union built-in types with plugin-declared types (D-03)
    let effective_source_types = effective_type_list(SUPPORTED_SOURCE_TYPES, schema.and_then(|s| s.supported_source_types.as_deref()));
    let effective_sink_types = effective_type_list(SUPPORTED_SINK_TYPES, schema.and_then(|s| s.supported_sink_types.as_deref()));

    // Track known df names for reference checking
    let mut known_names: HashSet<String> = HashSet::new();

    // Sources
    for (i, source) in job.sources.iter().enumerate() {
        let prefix = format!("sources[{}]", i);

        if !effective_source_types.iter().any(|t| t == &source.source_type) {
            errors.push(err(
                &format!("{prefix}.type"),
                &format!(
                    "\"{}\" is not a supported source type (expected: {})",
                    source.source_type,
                    effective_source_types.join(", ")
                ),
            ));
        }

        if source.auth.is_some() && source.source_type != "jdbc" {
            errors.push(err(
                &format!("{prefix}.auth"),
                "\"auth\" is only supported on jdbc sources",
            ));
        }

        match source.source_type.as_str() {
            "s3" => {
                if source.path.is_none() {
                    errors.push(err(&prefix, "type \"s3\" requires \"path\""));
                }
            }
            "jdbc" => {
                if source.connection_url.is_none() && source.auth.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"jdbc\" requires \"connection_url\" or \"auth\" (to derive the URL)",
                    ));
                }
                if source.connection_url.is_none() && source.auth.is_some() {
                    if source.connection_type.is_none() {
                        errors.push(err(
                            &prefix,
                            "type \"jdbc\" requires \"connection_type\" when \"connection_url\" is not set",
                        ));
                    }
                    if source.database.is_none() {
                        errors.push(err(
                            &prefix,
                            "type \"jdbc\" requires \"database\" when \"connection_url\" is not set",
                        ));
                    }
                }
                if source.table.is_none() {
                    errors.push(err(&prefix, "type \"jdbc\" requires \"table\""));
                }
                if source.engine.as_deref() == Some("glue")
                    && source.connection_type.is_none()
                {
                    errors.push(err(
                        &prefix,
                        "type \"jdbc\" with engine \"glue\" requires \"connection_type\" (mysql, postgresql, sqlserver, oracle, redshift)",
                    ));
                }
                errors.extend(validate_jdbc_auth(
                    &prefix,
                    source.secret_id.as_deref(),
                    source.auth.as_ref(),
                ));
            }
            "kafka" => {
                if source.connection_url.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"kafka\" requires \"connection_url\" (bootstrap servers)",
                    ));
                }
                if source.topic.is_none() {
                    errors.push(err(&prefix, "type \"kafka\" requires \"topic\""));
                }
            }
            "api" => {
                if source.url.is_none() {
                    errors.push(err(&prefix, "type \"api\" requires \"url\""));
                }
            }
            "catalog" => {
                if source.database.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"catalog\" requires \"database\"",
                    ));
                }
                if source.table.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"catalog\" requires \"table\"",
                    ));
                }
            }
            _ => {}
        }

        if let Some(engine) = source.engine.as_deref()
            && !VALID_ENGINES.contains(&engine)
        {
            errors.push(err(
                &format!("{prefix}.engine"),
                &format!(
                    "\"{}\" is not a valid engine (expected: {})",
                    engine,
                    VALID_ENGINES.join(", ")
                ),
            ));
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
                        &prefix,
                        "type \"filter\" requires \"condition\"",
                    ));
                }
            }
            "sql" => {
                if transform.query.is_none() {
                    errors.push(err(&prefix, "type \"sql\" requires \"query\""));
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
                    errors.push(err(&prefix, "type \"join\" requires \"left\""));
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
                    errors.push(err(&prefix, "type \"join\" requires \"right\""));
                }
                if transform.on.is_none() {
                    errors.push(err(&prefix, "type \"join\" requires \"on\""));
                }
            }
            "drop_columns" | "select" => {
                if transform.columns.is_empty() {
                    errors.push(err(
                        &prefix,
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
                        &prefix,
                        "type \"rename\" requires non-empty \"mapping\"",
                    ));
                }
            }
            "add_column" => {
                if transform.name.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"add_column\" requires \"name\"",
                    ));
                }
                if transform.expression.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"add_column\" requires \"expression\"",
                    ));
                }
            }
            "aggregate" => {
                if transform.group_by.is_empty() {
                    errors.push(err(
                        &prefix,
                        "type \"aggregate\" requires non-empty \"group_by\"",
                    ));
                }
                if transform.aggs.is_empty() {
                    errors.push(err(
                        &prefix,
                        "type \"aggregate\" requires non-empty \"aggs\"",
                    ));
                }
            }
            "window" => {
                if transform.name.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"window\" requires \"name\" (new column name)",
                    ));
                }
                if transform.expression.is_none() {
                    errors.push(err(
                        &prefix,
                        "type \"window\" requires \"expression\"",
                    ));
                }
                if transform.partition_by.is_empty() && transform.order_by.is_empty() {
                    errors.push(err(
                        &prefix,
                        "type \"window\" requires at least one of \"partition_by\" or \"order_by\"",
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
        if !effective_sink_types.iter().any(|t| t == &sink.sink_type) {
            errors.push(err(
                "sink.type",
                &format!(
                    "\"{}\" is not a supported sink type (expected: {})",
                    sink.sink_type,
                    effective_sink_types.join(", ")
                ),
            ));
        }

        if sink.auth.is_some() && sink.sink_type != "jdbc" {
            errors.push(err(
                "sink.auth",
                "\"auth\" is only supported on jdbc sinks",
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
                if sink.connection_url.is_none() && sink.auth.is_none() {
                    errors.push(err("sink", "type \"jdbc\" requires \"connection_url\" or \"auth\" (to derive the URL)"));
                }
                if sink.connection_url.is_none() && sink.auth.is_some() {
                    if sink.connection_type.is_none() {
                        errors.push(err("sink", "type \"jdbc\" requires \"connection_type\" when \"connection_url\" is not set"));
                    }
                    if sink.database.is_none() {
                        errors.push(err("sink", "type \"jdbc\" requires \"database\" when \"connection_url\" is not set"));
                    }
                }
                if sink.table.is_none() {
                    errors.push(err("sink", "type \"jdbc\" requires \"table\""));
                }
                errors.extend(validate_jdbc_auth(
                    "sink",
                    sink.secret_id.as_deref(),
                    sink.auth.as_ref(),
                ));
            }
            "catalog" => {
                if sink.database.is_none() {
                    errors.push(err("sink", "type \"catalog\" requires \"database\""));
                }
                if sink.table.is_none() {
                    errors.push(err("sink", "type \"catalog\" requires \"table\""));
                }
            }
            "iceberg" => {
                if sink.database.is_none() {
                    errors.push(err("sink", "type \"iceberg\" requires \"database\""));
                }
                if sink.table.is_none() {
                    errors.push(err("sink", "type \"iceberg\" requires \"table\""));
                }
                if let Some(mode) = sink.mode.as_deref()
                    && !VALID_ICEBERG_MODES.contains(&mode)
                {
                    errors.push(err(
                        "sink.mode",
                        &format!(
                            "\"{}\" is not valid for iceberg (expected: {})",
                            mode,
                            VALID_ICEBERG_MODES.join(", ")
                        ),
                    ));
                }
            }
            _ => {}
        }
    }

    // Job-level partition_by (Iceberg only)
    if !job.partition_by.is_empty() {
        for p in &job.partition_by {
            if !VALID_PARTITION_UNITS.contains(&p.as_str()) {
                errors.push(err(
                    "partition_by",
                    &format!(
                        "\"{}\" is not a valid partition unit (expected: {})",
                        p,
                        VALID_PARTITION_UNITS.join(", ")
                    ),
                ));
            }
        }

        let sink_is_iceberg = job
            .sink
            .as_ref()
            .is_some_and(|s| s.sink_type == "iceberg");
        if !sink_is_iceberg {
            errors.push(err(
                "partition_by",
                "job-level \"partition_by\" requires an iceberg sink",
            ));
        }

        match (job.create_timestamp, &job.partition_timestamp_column) {
            (true, Some(_)) => errors.push(err(
                "create_timestamp",
                "cannot set both \"create_timestamp\" and \"partition_timestamp_column\"",
            )),
            (false, None) => errors.push(err(
                "partition_by",
                "requires one of \"create_timestamp: true\" or \"partition_timestamp_column\"",
            )),
            _ => {}
        }
    } else if job.create_timestamp || job.partition_timestamp_column.is_some() {
        errors.push(err(
            "partition_by",
            "\"create_timestamp\" / \"partition_timestamp_column\" require non-empty \"partition_by\"",
        ));
    }

    // Provider-specific config validation is now the plugin's responsibility
    // via Provider::validate. Core validation covers only structural schema.

    errors
}

/// Build an effective type list by unioning built-in types with optional
/// plugin-declared types (D-03). Used for source and sink type validation.
fn effective_type_list(builtin: &[&str], plugin_types: Option<&[String]>) -> Vec<String> {
    let mut types: Vec<String> = builtin.iter().map(|s| (*s).to_string()).collect();
    if let Some(extras) = plugin_types {
        for t in extras {
            if !types.iter().any(|existing| existing == t) {
                types.push(t.clone());
            }
        }
    }
    types
}

/// Validate the interplay between `secret_id` and `auth` on a jdbc source/sink.
///
/// Caller has already confirmed `source_type`/`sink_type` is `"jdbc"`.
/// At most one error is returned (mutual exclusion or missing username).
fn validate_jdbc_auth(
    prefix: &str,
    secret_id: Option<&str>,
    auth: Option<&JdbcAuth>,
) -> Vec<ValidationError> {
    let mut errors = Vec::with_capacity(1);
    if let Some(JdbcAuth::RdsIam(rds)) = auth {
        match (secret_id.is_some(), rds.username.is_some()) {
            (true, true) => errors.push(err(
                &format!("{prefix}.auth.username"),
                "\"auth.username\" must not be set when \"secret_id\" is also set; the username is read from the secret",
            )),
            (false, false) => errors.push(err(
                &format!("{prefix}.auth.username"),
                "\"auth.username\" is required for kind \"rds_iam\" when no \"secret_id\" is set",
            )),
            _ => {}
        }
    }
    errors
}
