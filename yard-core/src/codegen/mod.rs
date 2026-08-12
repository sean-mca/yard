//! PySpark/Glue script codegen from yard job definitions.
//!
//! This module generates complete Python scripts for AWS Glue and EMR
//! Serverless jobs by combining Tera templates with dynamic source,
//! transform, and sink rendering. The generated scripts include import
//! management, Spark session setup, null-handling helpers for Iceberg
//! sinks, and proper Glue `job.commit()` teardown.
//!
//! Sub-modules handle distinct rendering concerns:
//! - `helpers` -- shared utilities (import rendering, Spark options, JDBC auth, partitions)
//! - `source` -- reader calls per source type (S3, JDBC, Catalog, Kafka, API)
//! - `sink` -- writer calls per sink type (S3, JDBC, Catalog, Iceberg)
//! - `transform` -- transform operations (filter, SQL, join, aggregate, window, etc.)

mod helpers;
mod pii;
mod sink;
mod source;
mod transform;

use anyhow::{Context as AnyhowContext, Result, anyhow};
use tera::{Context, Tera};
use yard_structs::{JobDefinition, JobType};

// Re-import sub-module items so they are in scope for generate_python_script
// and for `use super::*` in the test module
use helpers::*;
use pii::render_pii;
use sink::render_sink;
use source::render_sources;
use transform::render_transforms;

/// Tera template for AWS Glue PySpark jobs.
const GLUE_TEMPLATE: &str = include_str!("../templates/glue.py.tera");

/// Tera template for EMR Serverless PySpark jobs.
const EMR_TEMPLATE: &str = include_str!("../templates/emr.py.tera");

/// Emitted inline at module scope when an iceberg sink is writing with
/// `fill_nulls` enabled. Provides a single schema-conform path (Spark 3.5 /
/// Glue 5): voids are dropped recursively (never invented), the batch is
/// reconciled to a target schema via `DataFrame.to(target)`, and existing
/// tables auto-evolve by merging the live schema with the void-free batch.
/// A fail-fast guard refuses writes where the source kind diverges from the
/// table kind (struct vs list/map). Opt-out via `fill_nulls: false`.
const ICEBERG_FILL_NULLS_HELPERS: &str = r#"def _yard_has_void(dt):
    if isinstance(dt, NullType):
        return True
    if isinstance(dt, StructType):
        if len(dt.fields) == 0:
            return True
        return any(_yard_has_void(f.dataType) for f in dt.fields)
    if isinstance(dt, ArrayType):
        return _yard_has_void(dt.elementType)
    if isinstance(dt, MapType):
        return _yard_has_void(dt.keyType) or _yard_has_void(dt.valueType)
    return False


def _yard_void_free_ddl(dt):
    if isinstance(dt, NullType):
        return None
    if isinstance(dt, StructType):
        if len(dt.fields) == 0:
            return "struct<>"
        parts = []
        for f in dt.fields:
            sub = _yard_void_free_ddl(f.dataType)
            if sub is not None:
                parts.append("`" + f.name.replace("`", "``") + "`:" + sub)
        if not parts:
            return "struct<>"
        return "struct<" + ",".join(parts) + ">"
    if isinstance(dt, ArrayType):
        inner = _yard_void_free_ddl(dt.elementType)
        if inner is None:
            return None
        return "array<" + inner + ">"
    if isinstance(dt, MapType):
        k = _yard_void_free_ddl(dt.keyType)
        v = _yard_void_free_ddl(dt.valueType)
        if k is None or v is None:
            return None
        return "map<" + k + "," + v + ">"
    return dt.simpleString()


def _yard_void_free_dt(dt):
    # Recursively drop void leaves, empty structs, array<void>, map<..void..> at
    # any depth. Returns a cleaned DataType, or None when the type collapses
    # entirely (caller drops the field). Voids are dropped, never invented: a
    # NullType column carries zero inferable type, so the only honest move is to
    # omit it until a real value re-introduces it via schema evolution.
    if isinstance(dt, NullType):
        return None
    if isinstance(dt, StructType):
        fields = []
        for f in dt.fields:
            sub = _yard_void_free_dt(f.dataType)
            if sub is not None:
                fields.append(StructField(f.name, sub, True))
        if not fields:
            return None
        return StructType(fields)
    if isinstance(dt, ArrayType):
        inner = _yard_void_free_dt(dt.elementType)
        if inner is None:
            return None
        return ArrayType(inner, True)
    if isinstance(dt, MapType):
        k = _yard_void_free_dt(dt.keyType)
        v = _yard_void_free_dt(dt.valueType)
        if k is None or v is None:
            return None
        return MapType(k, v, True)
    return dt


def _yard_void_free_schema(schema):
    fields = []
    for f in schema.fields:
        sub = _yard_void_free_dt(f.dataType)
        if sub is not None:
            fields.append(StructField(f.name, sub, True))
    return StructType(fields)


def _yard_kind(dt):
    if isinstance(dt, StructType):
        return "struct"
    if isinstance(dt, ArrayType):
        return "array"
    if isinstance(dt, MapType):
        return "map"
    return "scalar"


def _yard_kind_mismatch(src_dt, tgt_dt):
    # True when the source-inferred kind diverges from the target kind in a way
    # df.to() cannot reconcile (struct vs list/map, or scalar vs container).
    # Scalar-vs-scalar is left to df.to()'s safe up-cast.
    ks, kt = _yard_kind(src_dt), _yard_kind(tgt_dt)
    if ks == kt:
        return False
    return ks != "scalar" or kt != "scalar"


def _yard_merge_dt(live_dt, batch_dt):
    if isinstance(live_dt, StructType) and isinstance(batch_dt, StructType):
        return _yard_merge_schema(live_dt, batch_dt)
    if isinstance(live_dt, ArrayType) and isinstance(batch_dt, ArrayType):
        return ArrayType(_yard_merge_dt(live_dt.elementType, batch_dt.elementType), True)
    if isinstance(live_dt, MapType) and isinstance(batch_dt, MapType):
        return MapType(live_dt.keyType, _yard_merge_dt(live_dt.valueType, batch_dt.valueType), True)
    return live_dt


def _yard_merge_schema(live, batch):
    # Union: live field types win; genuinely-new typed fields from the batch are
    # added (including nested); nested structs merge field-by-field. The result
    # is the auto-evolve target the dataframe is conformed to before writing.
    batch_map = {f.name: f.dataType for f in batch.fields}
    out = []
    seen = set()
    for f in live.fields:
        seen.add(f.name)
        if f.name in batch_map:
            out.append(StructField(f.name, _yard_merge_dt(f.dataType, batch_map[f.name]), True))
        else:
            out.append(StructField(f.name, f.dataType, True))
    for f in batch.fields:
        if f.name not in seen:
            out.append(StructField(f.name, f.dataType, True))
    return StructType(out)


def _yard_read_iceberg_schema(spark, tbl):
    return spark.read.format("iceberg").load(tbl).schema


def _yard_conform(df, target_schema):
    return df.to(target_schema)
"#;

/// Generate a complete PySpark script for the given job definition.
///
/// For Glue and EMR job types, renders sources, transforms, sink, and
/// import management into a Tera template. Task-only types (Bash) return
/// an empty string. If `job_file` is set, returns the external file
/// contents verbatim.
///
/// # Errors
///
/// Returns an error when:
/// - Required source/sink fields are missing
/// - The Tera template fails to render
/// - An external `job_file` cannot be read
/// - The job type has no codegen template (e.g. Bash)
pub fn generate_python_script(job_name: &str, job_def: &JobDefinition) -> Result<String> {
    // Task-only job types (bash, ...) don't produce a standalone PySpark script;
    // they participate in Airflow DAG codegen instead. Return an empty string so
    // callers that blindly hash/write the script output continue to work -- the
    // apply path skips deploy for these types via `is_task_only`.
    if crate::is_task_only(job_def.job_type) {
        return Ok(String::new());
    }

    // If job_file is specified, use the external file as the complete script
    if let Some(ref path) = job_def.job_file {
        return std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read job_file: {path}"));
    }

    let template = match job_def.job_type {
        JobType::Glue => GLUE_TEMPLATE,
        JobType::Emr => EMR_TEMPLATE,
        JobType::Bash => {
            return Err(anyhow!(
                "bash jobs are task-only and have no Spark template"
            ));
        }
        _ => {
            return Err(anyhow!(
                "unsupported job type for codegen: {}", job_def.job_type
            ));
        }
    };

    let mut tera = Tera::default();
    tera.add_raw_template("script", template)?;

    // Determine the default source name (first source, or "source")
    let default_source = job_def
        .sources
        .first()
        .map(|s| s.name.as_str())
        .unwrap_or("source");

    let all_source_names: Vec<String> = job_def.sources.iter().map(|s| s.name.clone()).collect();

    let default_engine = default_engine_for(job_def);

    // Build extra imports needed by template features (at most ~8 entries)
    let mut extra_imports = Vec::with_capacity(8);
    let iceberg = has_iceberg_sink(job_def);
    let partitioning = !job_def.partition_by.is_empty();
    if needs_secrets_imports(job_def) || needs_jdbc_auth_imports(job_def) || iceberg {
        extra_imports.push("import boto3".to_string());
        if needs_secrets_imports(job_def) {
            extra_imports.push("import json".to_string());
        }
    }
    if needs_requests_import(job_def) {
        extra_imports.push("import requests".to_string());
    }
    if needs_dynamic_frame_import(job_def, &default_engine) {
        extra_imports.push("from awsglue.dynamicframe import DynamicFrame".to_string());
    }
    let fill_nulls = should_fill_nulls(job_def);
    if needs_functions_import(job_def) || partitioning || fill_nulls {
        extra_imports.push("from pyspark.sql import functions as F".to_string());
    }
    if fill_nulls {
        extra_imports.push(
            "from pyspark.sql.types import (StructType, StructField, ArrayType, MapType, DoubleType, \
             FloatType, IntegerType, LongType, ShortType, ByteType, TimestampType, DateType, \
             DecimalType, BinaryType, BooleanType, NullType)"
                .to_string(),
        );
    }
    if needs_window_import(job_def) {
        extra_imports.push("from pyspark.sql.window import Window".to_string());
    }
    if needs_pii_imports(job_def) {
        extra_imports.push("from awsglueml.transforms import EntityDetector".to_string());
    }

    let user_imports = render_imports(&job_def.imports);
    let mut all_imports = if extra_imports.is_empty() {
        user_imports
    } else if user_imports.is_empty() {
        extra_imports.join("\n")
    } else {
        format!("{}\n{}", user_imports, extra_imports.join("\n"))
    };
    if fill_nulls {
        // Append the helpers at module scope (after the imports block, before
        // the Glue/Spark setup). The template inlines `imports_block` verbatim.
        all_imports.push_str("\n\n\n");
        all_imports.push_str(ICEBERG_FILL_NULLS_HELPERS);
    }

    // Build the run() body
    let run_body = if let Some(body) = &job_def.body {
        indent_body(body)
    } else {
        let mut parts = Vec::with_capacity(3);
        if !job_def.sources.is_empty() {
            parts.push(format!(
                "    # --- Sources ---\n{}",
                render_sources(&job_def.sources, &default_engine)?
            ));
        }
        if !job_def.transforms.is_empty() {
            parts.push(format!(
                "    # --- Transforms ---\n{}",
                render_transforms(&job_def.transforms, default_source, &all_source_names)?
            ));
        }
        if let Some(sink) = &job_def.sink {
            let sink_source = sink.source.as_deref().unwrap_or(default_source);
            if let Some(deriv) = render_partition_derivation(job_def, sink_source) {
                parts.push(deriv);
            }
            if !job_def.mask_pii.is_empty() {
                let pii_source = format!("df_{sink_source}");
                parts.push(format!(
                    "    # --- PII Masking ---\n{}",
                    render_pii(&job_def.mask_pii, &pii_source)
                ));
            }
            // Mirror job-level partition_by onto the iceberg sink so writeTo
            // emits `.partitionedBy(...)` on first table creation.
            let effective_sink = if sink.sink_type == "iceberg" && !job_def.partition_by.is_empty()
            {
                let mut s = sink.clone();
                s.partition_by = job_def.partition_by.clone();
                std::borrow::Cow::Owned(s)
            } else {
                std::borrow::Cow::Borrowed(sink)
            };
            parts.push(format!(
                "    # --- Sink ---\n{}",
                render_sink(&effective_sink, default_source, fill_nulls, job_def.config.get("glue").and_then(|g| g.get("catalog_id")).and_then(|v| v.as_str()))?
            ));
        }
        if parts.is_empty() {
            "    pass".to_string()
        } else {
            parts.join("\n\n")
        }
    };

    let mut context = Context::new();
    context.insert("job_name", job_name);
    context.insert("job_type", &job_def.job_type);
    context.insert("imports_block", &all_imports);
    context.insert("body", &run_body);

    // Iceberg warehouse for the glue_catalog Spark catalog, read from merged
    // provider config (`providers.glue.warehouse`, flowed into job.config.glue).
    let glue_cfg = job_def.config.get("glue");
    let warehouse = glue_cfg
        .and_then(|g| g.get("warehouse"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    context.insert("iceberg_warehouse", warehouse);

    let catalog_id = glue_cfg
        .and_then(|g| g.get("catalog_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    context.insert("catalog_id", catalog_id);

    let rendered = tera
        .render("script", &context)
        .context("Failed to render Python template")?;

    Ok(rendered)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn base_job() -> JobDefinition {
        JobDefinition {
            job_type: JobType::Glue,
            config: json!({"type": "glue"}),
            ..Default::default()
        }
    }

    fn s3_source(name: &str, path: &str) -> yard_structs::Source {
        yard_structs::Source {
            name: name.to_string(),
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some(path.to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }
    }

    // --- Routing ---

    // The `unsupported_job_type_errors` test was deleted in Phase 21 plan 21-01.
    // Unknown job-type wire strings are now rejected at deserialize time by
    // serde via JobType's `unknown variant` error (see
    // `yard_structs::config::tests::job_type_deserialize_unknown_rejects`).
    // Constructing a JobDefinition with an invalid job_type is no longer
    // expressible — JobType is a closed three-variant enum.

    // --- Template basics ---

    #[test]
    fn generates_header() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("Generated by YARD for job: test_job"));
    }

    #[test]
    fn glue_setup_and_teardown() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("from awsglue.utils import getResolvedOptions"));
        assert!(script.contains("SparkSession.builder"));
        assert!(script.contains("spark.sparkContext"));
        assert!(script.contains("job.commit()"));
    }

    #[test]
    fn default_body_is_pass() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("    pass"));
    }

    // --- Named sources ---

    #[test]
    fn single_source_named() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://bucket/events/")];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(
            script.contains(
                "df_events = spark.read.format(\"parquet\").load(\"s3://bucket/events/\")"
            )
        );
    }

    #[test]
    fn multiple_sources_named() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://bucket/orders/"),
            s3_source("customers", "s3://bucket/customers/"),
        ];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_orders = spark.read"));
        assert!(script.contains("df_customers = spark.read"));
    }

    // --- Transforms with named dfs ---

    #[test]
    fn transform_defaults_to_first_source() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.transforms = vec![yard_structs::Transform {
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
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_events = df_events.filter(col('active'))"));
    }

    #[test]
    fn transform_explicit_source_and_output() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://b/orders"),
            s3_source("customers", "s3://b/customers"),
        ];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "filter".to_string(),
            source: Some("customers".to_string()),
            output: Some("active_customers".to_string()),
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
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_active_customers = df_customers.filter(col('active'))"));
    }

    // --- Join ---

    #[test]
    fn join_transform() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://b/orders"),
            s3_source("customers", "s3://b/customers"),
        ];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "join".to_string(),
            source: None,
            output: Some("enriched".to_string()),
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: Some("orders".to_string()),
            right: Some("customers".to_string()),
            on: Some("customer_id".to_string()),
            how: Some("left".to_string()),
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains(
            "df_enriched = df_orders.join(df_customers, on=\"customer_id\", how=\"left\")"
        ));
    }

    // --- SQL with named sources ---

    #[test]
    fn sql_registers_all_sources_as_views() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://b/orders"),
            s3_source("customers", "s3://b/customers"),
        ];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "sql".to_string(),
            source: None,
            output: Some("enriched".to_string()),
            condition: None,
            query: Some(
                "SELECT o.*, c.name FROM orders o JOIN customers c ON o.cid = c.id".to_string(),
            ),
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
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_orders.createOrReplaceTempView(\"orders\")"));
        assert!(script.contains("df_customers.createOrReplaceTempView(\"customers\")"));
        assert!(script.contains("df_enriched = spark.sql("));
    }

    // --- Sink with named source ---

    #[test]
    fn sink_defaults_to_first_source() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out/".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: Some("overwrite".to_string()),
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_events.write.format(\"parquet\")"));
    }

    #[test]
    fn sink_explicit_source() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://b/orders"),
            s3_source("customers", "s3://b/customers"),
        ];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "join".to_string(),
            source: None,
            output: Some("enriched".to_string()),
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: Some("orders".to_string()),
            right: Some("customers".to_string()),
            on: Some("customer_id".to_string()),
            how: Some("inner".to_string()),
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        job.sink = Some(yard_structs::Sink {
            source: Some("enriched".to_string()),
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out/".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: Some("overwrite".to_string()),
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_enriched.write.format(\"parquet\")"));
    }

    // --- Iceberg sink ---

    fn iceberg_sink(database: &str, table: &str, path: Option<&str>) -> yard_structs::Sink {
        yard_structs::Sink {
            source: None,
            sink_type: "iceberg".to_string(),
            database: Some(database.to_string()),
            table: Some(table.to_string()),
            path: path.map(str::to_string),
            mode: Some("append".to_string()),
            ..Default::default()
        }
    }

    /// Render the canonical single-source iceberg job used by the conform tests.
    fn iceberg_script() -> String {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        generate_python_script("test_job", &job).unwrap()
    }

    /// Split a rendered iceberg script into its (new-table, existing-table)
    /// branches around the `tableExists` `if`/`else`.
    fn split_branches(script: &str) -> (String, String) {
        let if_idx = script
            .find("if not spark.catalog.tableExists(_tbl):")
            .expect("if/tableExists block must be present");
        let else_idx = script[if_idx..]
            .find("\n    else:\n")
            .map(|o| if_idx + o)
            .expect("else: branch must follow the tableExists check");
        (
            script[if_idx..else_idx].to_string(),
            script[else_idx..].to_string(),
        )
    }

    #[test]
    fn iceberg_sink_without_path_omits_location() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("\"glue_catalog.analytics.events\""));
        assert!(!script.contains(".tableProperty(\"location\""));
    }

    #[test]
    fn iceberg_sink_with_path_emits_location_property() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink(
            "analytics",
            "events",
            Some("s3://my-warehouse/analytics/events/"),
        ));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains(
            ".tableProperty(\"location\", \"s3://my-warehouse/analytics/events/\")"
        ));
        // Location only applies on create; unchanged for non-create branch.
        assert!(script.contains(".writeTo(_tbl).option(\"merge-schema\", \"true\").append()"));
    }

    #[test]
    fn iceberg_sink_empty_path_omits_location() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", Some("")));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(!script.contains(".tableProperty(\"location\""));
    }

    // --- iceberg schema-conform: emitted helper shape (void-free + df.to) ---

    #[test]
    fn conform_uses_exact_null_type_check() {
        let script = iceberg_script();
        // Void detection stays exact isinstance, never the old substring match.
        assert!(script.contains("isinstance(dt, NullType)"));
        assert!(!script.contains("\"void\" in dt.simpleString()"));
    }

    #[test]
    fn dual_arm_machinery_is_gone() {
        // Goal-backward: the broken when(isNull, default).otherwise(coerced)
        // struct reconstruction (the source of the DATA_DIFF_TYPES crash) must
        // be fully absent. The deleted dual-arm helpers were the only emitters
        // of these expressions, so their absence proves the machinery is gone
        // — without re-spelling the dead identifiers into this file.
        let script = iceberg_script();
        assert!(!script.contains(".otherwise("));
        assert!(!script.contains("F.when("));
        // The single conform pass replaces all of it.
        assert!(script.contains("_yard_conform("));
    }

    #[test]
    fn voids_are_dropped_never_invented() {
        // D-3: a void column carries no inferable type — drop it, never fabricate
        // 0 / "" / False / default structs (the source of Bug 1).
        let script = iceberg_script();
        assert!(!script.contains("F.lit(0)"));
        assert!(!script.contains("F.lit(0.0)"));
        assert!(!script.contains("F.lit(False)"));
        assert!(!script.contains("F.lit(\"\")"));
        assert!(!script.contains("F.coalesce("));
    }

    #[test]
    fn conform_helpers_defined_in_prelude() {
        let script = iceberg_script();
        // The new schema-conform helpers.
        assert!(script.contains("def _yard_void_free_schema(schema):"));
        assert!(script.contains("def _yard_merge_schema(live, batch):"));
        assert!(script.contains("def _yard_kind_mismatch(src_dt, tgt_dt):"));
        assert!(script.contains("def _yard_conform(df, target_schema):"));
        assert!(script.contains("def _yard_read_iceberg_schema(spark, tbl):"));
        // The single-pass conform delegates to Spark 3.5 DataFrame.to().
        assert!(script.contains("return df.to(target_schema)"));
    }

    #[test]
    fn recursive_void_helpers_kept() {
        // D-7: _yard_has_void and _yard_void_free_ddl are correct and retained
        // (the latter backs the documented per-column cast fallback).
        let script = iceberg_script();
        assert!(script.contains("def _yard_has_void(dt):"));
        assert!(script.contains("def _yard_void_free_ddl(dt):"));
        // _yard_has_void still recognizes every container kind.
        assert!(script.contains("isinstance(dt, StructType)"));
        assert!(script.contains("isinstance(dt, ArrayType)"));
        assert!(script.contains("isinstance(dt, MapType)"));
    }

    #[test]
    fn void_free_schema_drops_void_leaves_at_any_depth() {
        let script = iceberg_script();
        // _yard_void_free_dt returns None for a void leaf (signals drop)…
        assert!(script.contains("if isinstance(dt, NullType):\n        return None"));
        // …and rebuilds clean container types from surviving children.
        assert!(script.contains("fields.append(StructField(f.name, sub, True))"));
        assert!(script.contains("return ArrayType(inner, True)"));
        assert!(script.contains("return MapType(k, v, True)"));
        // Top-level driver builds a StructType from non-collapsed fields.
        assert!(script.contains("def _yard_void_free_schema(schema):"));
        assert!(script.contains("return StructType(fields)"));
        // StructField is imported for the schema rebuild.
        assert!(script.contains("StructType, StructField, ArrayType, MapType"));
    }

    #[test]
    fn merge_schema_unions_live_and_batch() {
        let script = iceberg_script();
        // Live field types win; new typed fields are appended (auto-evolve).
        assert!(script.contains("def _yard_merge_schema(live, batch):"));
        assert!(script.contains("batch_map = {f.name: f.dataType for f in batch.fields}"));
        assert!(script.contains("if f.name not in seen:"));
        // Nested structs merge field-by-field via _yard_merge_dt recursion.
        assert!(script.contains("def _yard_merge_dt(live_dt, batch_dt):"));
        assert!(script.contains("return _yard_merge_schema(live_dt, batch_dt)"));
    }

    #[test]
    fn kind_mismatch_predicate_distinguishes_containers() {
        let script = iceberg_script();
        // Scalar-vs-scalar is not a mismatch (df.to handles safe up-cast);
        // any struct/list/map divergence is.
        assert!(script.contains("def _yard_kind_mismatch(src_dt, tgt_dt):"));
        assert!(script.contains("return ks != \"scalar\" or kt != \"scalar\""));
    }

    // --- iceberg per-branch: new-table conform vs existing-table merge+evolve ---

    #[test]
    fn new_table_branch_conforms_via_void_free_schema() {
        let script = iceberg_script();
        let (new_table, _existing) = split_branches(&script);
        assert!(new_table.contains("_target = _yard_void_free_schema(df_events.schema)"));
        assert!(new_table.contains("df_events = _yard_conform(df_events, _target)"));
        assert!(new_table.contains(".create())"));
        // df.to() is the single conform pass.
        assert!(script.contains("return df.to(target_schema)"));
    }

    #[test]
    fn new_table_branch_does_not_read_live_schema() {
        // Regression: reading live Iceberg metadata before the table exists errors.
        // The void-free target is derived purely from the first batch.
        let script = iceberg_script();
        let (new_table, _existing) = split_branches(&script);
        assert!(!new_table.contains("_yard_read_iceberg_schema"));
        assert!(!new_table.contains("_yard_merge_schema"));
    }

    #[test]
    fn existing_table_branch_merges_and_evolves_append() {
        let script = iceberg_script();
        let (_new_table, existing) = split_branches(&script);
        // Live schema is the contract; target = merge(live, void_free(batch)).
        assert!(existing.contains("_live = _yard_read_iceberg_schema(spark, _tbl)"));
        assert!(existing.contains("_batch = _yard_void_free_schema(df_events.schema)"));
        assert!(existing.contains("_target = _yard_merge_schema(_live, _batch)"));
        assert!(existing.contains("df_events = _yard_conform(df_events, _target)"));
        // Auto-evolve on append via the merge-schema write option.
        assert!(existing.contains("df_events.writeTo(_tbl).option(\"merge-schema\", \"true\").append()"));
    }

    #[test]
    fn existing_table_branch_reorders_columns_to_match_table() {
        let script = iceberg_script();
        let (new_table, existing) = split_branches(&script);
        assert!(existing.contains("_existing_cols = spark.table(_tbl).columns"));
        assert!(existing.contains("_ordered = [_c for _c in _existing_cols if _c in df_events.columns]"));
        assert!(existing.contains("_new = [_c for _c in df_events.columns if _c not in _existing_cols]"));
        assert!(existing.contains("df_events = df_events.select(_ordered + _new)"));
        assert!(!new_table.contains("_existing_cols"));
    }

    #[test]
    fn column_reorder_present_when_fill_nulls_false() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        if let Some(s) = job.sink.as_mut() {
            s.fill_nulls = Some(false);
        }
        let script = generate_python_script("test_job", &job).unwrap();
        let (_new_table, existing) = split_branches(&script);
        assert!(existing.contains("_existing_cols = spark.table(_tbl).columns"));
        assert!(existing.contains("df_events = df_events.select(_ordered + _new)"));
    }

    #[test]
    fn existing_table_branch_overwrite_maps_to_overwrite_partitions() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        if let Some(s) = job.sink.as_mut() {
            s.mode = Some("overwrite".to_string());
        }
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("_target = _yard_merge_schema(_live, _batch)"));
        // Overwrite mode maps to overwritePartitions, not append.
        assert!(script.contains("df_events.writeTo(_tbl).option(\"merge-schema\", \"true\").overwritePartitions()"));
    }

    #[test]
    fn existing_table_branch_emits_kind_mismatch_guard() {
        // D-6 fail-fast: refuse the write when an inferred column kind diverges
        // from the table kind (struct vs list/map) — stops Bug 2 at the source
        // rather than emitting an expression that detonates in Spark.
        let script = iceberg_script();
        let (new_table, existing) = split_branches(&script);
        assert!(existing.contains("_yard_kind_mismatch(_f.dataType, _live_types[_f.name])"));
        assert!(
            existing.contains("raise ValueError(\"yard: schema kind mismatch for column "),
            "existing-table branch must emit a clear fail-fast guard message"
        );
        // The guard is existing-table only (a new table has no prior kinds).
        assert!(!new_table.contains("_yard_kind_mismatch"));
    }

    #[test]
    fn no_try_except_around_write() {
        let script = iceberg_script();
        // No try/except wraps the Iceberg writeTo on either branch. Two unrelated
        // try/except blocks exist in the rendered script — the Glue create-database
        // guard (`except _glue.exceptions.EntityNotFoundException`) and the
        // template's `__main__` wrapper — neither touches the write call-site.
        assert!(
            !script.contains("try:\n        df_events.writeTo(_tbl)"),
            "writeTo(_tbl) on the existing-table branch must not be wrapped in try:"
        );
        assert!(
            !script.contains(".append()\n    except"),
            "no exception handler wraps .append() on the existing-table write"
        );
        assert!(
            !script.contains(".overwritePartitions()\n    except"),
            "no exception handler wraps .overwritePartitions() on the existing-table write"
        );
        assert!(
            !script.contains(".create())\n    except"),
            "no exception handler wraps .create()) on the new-table write"
        );
    }

    #[test]
    fn fill_nulls_false_emits_neither_path() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        if let Some(s) = job.sink.as_mut() {
            s.fill_nulls = Some(false);
        }
        let script = generate_python_script("test_job", &job).unwrap();
        // D-8: opt-out emits neither conform path and injects no helpers at all.
        assert!(!script.contains("_yard_conform("));
        assert!(!script.contains("_yard_void_free_schema("));
        assert!(!script.contains("_yard_merge_schema("));
        assert!(!script.contains("def _yard_"));
        // Structural shape of the sink block is otherwise intact.
        assert!(script.contains("if not spark.catalog.tableExists(_tbl):"));
        assert!(script.contains("df_events.writeTo(_tbl).option(\"merge-schema\", \"true\").append()"));
    }

    #[test]
    fn non_iceberg_sink_output_unchanged() {
        // Regression golden: the iceberg rewrite must not alter non-iceberg
        // codegen. An s3/parquet sink emits its plain writer and zero schema
        // -conform machinery or `_yard_` helpers.
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains(
            "    # --- Sink ---\n    df_events.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://b/out\")"
        ));
        // No iceberg helpers, type imports, or conform calls leak in.
        assert!(!script.contains("_yard_"));
        assert!(!script.contains("df.to("));
        assert!(!script.contains("from pyspark.sql.types import"));
        assert!(!script.contains("glue_catalog"));
    }

    // --- Body override still works ---

    #[test]
    fn body_override_skips_source_sink() {
        let mut job = base_job();
        job.body = Some("print('custom')".to_string());
        job.sources = vec![s3_source("events", "s3://b/in")];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("    print('custom')"));
        assert!(!script.contains("spark.read"));
    }

    // --- JDBC with secrets uses named vars ---

    #[test]
    fn jdbc_source_secret_named() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "users".to_string(),
            source_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: Some("jdbc:postgresql://host:5432/db".to_string()),
            table: Some("public.users".to_string()),
            database: None,
            secret_id: Some("my-rds-secret".to_string()),
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("import boto3"));
        assert!(script.contains("get_secret_value(SecretId=\"my-rds-secret\")"));
        assert!(script.contains("users_secret[\"username\"]"));
        assert!(script.contains("df_users = spark.read.format(\"jdbc\")"));
    }

    // --- JDBC RDS IAM auth ---

    fn rds_iam_auth(username: Option<&str>) -> yard_structs::JdbcAuth {
        yard_structs::JdbcAuth::RdsIam(yard_structs::RdsIamAuth {
            username: username.map(|u| u.to_string()),
            host: "orders.cluster-abc.us-east-1.rds.amazonaws.com".to_string(),
            port: 5432,
            region: "us-east-1".to_string(),
        })
    }

    #[test]
    fn jdbc_source_rds_iam_auth_alone_uses_config_username() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "orders".to_string(),
            source_type: "jdbc".to_string(),
            connection_url: Some("jdbc:postgresql://h:5432/db".to_string()),
            table: Some("public.orders".to_string()),
            auth: Some(rds_iam_auth(Some("yard_app"))),
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("import boto3"));
        assert!(script.contains("_orders_rds = boto3.client(\"rds\", region_name=\"us-east-1\")"));
        assert!(script.contains("orders_token = _orders_rds.generate_db_auth_token("));
        assert!(script.contains("DBHostname=\"orders.cluster-abc.us-east-1.rds.amazonaws.com\","));
        assert!(script.contains("Port=5432,"));
        assert!(script.contains("DBUsername=\"yard_app\","));
        assert!(script.contains("Region=\"us-east-1\","));
        assert!(script.contains(".option(\"user\", \"yard_app\").option(\"password\", orders_token)"));
        assert!(!script.contains("get_secret_value"));
    }

    #[test]
    fn jdbc_source_rds_iam_auth_with_secret_uses_secret_username() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "orders".to_string(),
            source_type: "jdbc".to_string(),
            connection_url: Some("jdbc:postgresql://h:5432/db".to_string()),
            table: Some("public.orders".to_string()),
            secret_id: Some("rds-secret".to_string()),
            auth: Some(rds_iam_auth(None)),
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("get_secret_value(SecretId=\"rds-secret\")"));
        assert!(script.contains("orders_token = _orders_rds.generate_db_auth_token("));
        assert!(script.contains("DBUsername=orders_secret[\"username\"],"));
        assert!(script.contains(".option(\"user\", orders_secret[\"username\"]).option(\"password\", orders_token)"));
    }

    #[test]
    fn jdbc_source_rds_iam_auth_glue_engine() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "orders".to_string(),
            source_type: "jdbc".to_string(),
            connection_url: Some("jdbc:postgresql://h:5432/db".to_string()),
            table: Some("public.orders".to_string()),
            engine: Some("glue".to_string()),
            connection_type: Some("postgresql".to_string()),
            auth: Some(rds_iam_auth(Some("yard_app"))),
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("orders_token = _orders_rds.generate_db_auth_token("));
        assert!(script.contains("\"user\": \"yard_app\", \"password\": orders_token"));
        assert!(script.contains("create_dynamic_frame.from_options"));
    }

    #[test]
    fn jdbc_source_rds_iam_derives_url_from_auth() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "orders".to_string(),
            source_type: "jdbc".to_string(),
            database: Some("orders".to_string()),
            table: Some("public.orders".to_string()),
            connection_type: Some("postgresql".to_string()),
            auth: Some(rds_iam_auth(Some("yard_app"))),
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("jdbc:postgresql://orders.cluster-abc.us-east-1.rds.amazonaws.com:5432/orders"));
        assert!(script.contains("orders_token = _orders_rds.generate_db_auth_token("));
    }

    #[test]
    fn jdbc_sink_rds_iam_auth_alone() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            sink_type: "jdbc".to_string(),
            connection_url: Some("jdbc:postgresql://h:5432/db".to_string()),
            table: Some("public.events".to_string()),
            mode: Some("append".to_string()),
            auth: Some(rds_iam_auth(Some("yard_app"))),
            ..Default::default()
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("_sink_rds = boto3.client(\"rds\", region_name=\"us-east-1\")"));
        assert!(script.contains("sink_token = _sink_rds.generate_db_auth_token("));
        assert!(script.contains("DBUsername=\"yard_app\","));
        assert!(script.contains(".option(\"user\", \"yard_app\").option(\"password\", sink_token)"));
    }

    // --- Full pipeline with join ---

    #[test]
    fn full_pipeline_with_join() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://raw/orders/"),
            s3_source("customers", "s3://raw/customers/"),
        ];
        job.transforms = vec![
            yard_structs::Transform {
                transform_type: "filter".to_string(),
                source: Some("orders".to_string()),
                output: None,
                condition: Some("col('status') != 'cancelled'".to_string()),
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
            yard_structs::Transform {
                transform_type: "join".to_string(),
                source: None,
                output: Some("enriched".to_string()),
                condition: None,
                query: None,
                columns: vec![],
                mapping: HashMap::new(),
                name: None,
                expression: None,
                left: Some("orders".to_string()),
                right: Some("customers".to_string()),
                on: Some("customer_id".to_string()),
                how: Some("left".to_string()),
                group_by: vec![],
                aggs: std::collections::HashMap::new(),
                partition_by: vec![],
                order_by: vec![],
            },
            yard_structs::Transform {
                transform_type: "drop_columns".to_string(),
                source: Some("enriched".to_string()),
                output: None,
                condition: None,
                query: None,
                columns: vec!["debug".to_string()],
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
        job.sink = Some(yard_structs::Sink {
            source: Some("enriched".to_string()),
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://curated/enriched_orders/".to_string()),
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            mode: Some("overwrite".to_string()),
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_orders = spark.read"));
        assert!(script.contains("df_customers = spark.read"));
        assert!(script.contains("df_orders = df_orders.filter("));
        assert!(script.contains("df_enriched = df_orders.join(df_customers"));
        assert!(script.contains("df_enriched = df_enriched.drop("));
        assert!(script.contains("df_enriched.write.format(\"parquet\")"));
    }

    #[test]
    fn different_jobs_produce_different_scripts() {
        let a = generate_python_script("job_a", &base_job()).unwrap();
        let b = generate_python_script("job_b", &base_job()).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn job_file_uses_external_script() {
        let dir = std::env::temp_dir().join(format!("yard_jf_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let script_path = dir.join("custom.py");
        std::fs::write(&script_path, "print('custom script')\n").unwrap();

        let mut job = base_job();
        job.job_file = Some(script_path.to_string_lossy().to_string());

        let result = generate_python_script("test_job", &job).unwrap();
        assert_eq!(result, "print('custom script')\n");

        // Should NOT contain Glue boilerplate
        assert!(!result.contains("GlueContext"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn job_file_missing_file_errors() {
        let mut job = base_job();
        job.job_file = Some("/tmp/nonexistent_yard_test.py".to_string());

        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
    }

    // --- Missing required fields return errors ---

    #[test]
    fn s3_source_missing_path_errors() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "events".to_string(),
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: None,
            connection_url: None,
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("path"), "error should mention 'path': {msg}");
    }

    #[test]
    fn jdbc_source_missing_connection_url_errors() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "users".to_string(),
            source_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: Some("public.users".to_string()),
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("connection_url"), "error should mention 'connection_url': {msg}");
    }

    #[test]
    fn jdbc_source_missing_table_errors() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "users".to_string(),
            source_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: Some("jdbc:postgresql://host/db".to_string()),
            table: None,
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("table"), "error should mention 'table': {msg}");
    }

    #[test]
    fn catalog_source_missing_database_errors() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "catalog_src".to_string(),
            source_type: "catalog".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: Some("my_table".to_string()),
            database: None,
            secret_id: None,
            ..Default::default()
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("database"), "error should mention 'database': {msg}");
    }

    #[test]
    fn catalog_source_missing_table_errors() {
        let mut job = base_job();
        job.sources = vec![yard_structs::Source {
            name: "catalog_src".to_string(),
            source_type: "catalog".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: Some("my_db".to_string()),
            secret_id: None,
            ..Default::default()
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("table"), "error should mention 'table': {msg}");
    }

    #[test]
    fn join_missing_right_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("orders", "s3://b/orders")];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "join".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: Some("orders".to_string()),
            right: None,
            on: Some("id".to_string()),
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("right"), "error should mention 'right': {msg}");
    }

    #[test]
    fn join_missing_on_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("orders", "s3://b/orders")];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "join".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: None,
            left: Some("orders".to_string()),
            right: Some("customers".to_string()),
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("on"), "error should mention 'on': {msg}");
    }

    #[test]
    fn add_column_missing_name_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "add_column".to_string(),
            source: None,
            output: None,
            condition: None,
            query: None,
            columns: vec![],
            mapping: HashMap::new(),
            name: None,
            expression: Some("lit(1)".to_string()),
            left: None,
            right: None,
            on: None,
            how: None,
            group_by: vec![],
            aggs: std::collections::HashMap::new(),
            partition_by: vec![],
            order_by: vec![],
        }];
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("name"), "error should mention 'name': {msg}");
    }

    #[test]
    fn s3_sink_missing_path_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
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
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("path"), "error should mention 'path': {msg}");
    }

    #[test]
    fn jdbc_sink_missing_connection_url_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: Some("output_table".to_string()),
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("connection_url"), "error should mention 'connection_url': {msg}");
    }

    #[test]
    fn jdbc_sink_missing_table_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: Some("jdbc:postgresql://host/db".to_string()),
            table: None,
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("table"), "error should mention 'table': {msg}");
    }

    #[test]
    fn catalog_sink_missing_database_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "catalog".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: Some("my_table".to_string()),
            database: None,
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("database"), "error should mention 'database': {msg}");
    }

    #[test]
    fn catalog_sink_missing_table_errors() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            source: None,
            sink_type: "catalog".to_string(),
            format: None,
            path: None,
            connection_url: None,
            table: None,
            database: Some("my_db".to_string()),
            secret_id: None,
            mode: None,
            partition_by: vec![],
            fill_nulls: None,
            connection_type: None,
            auth: None,
        });
        let result = generate_python_script("test_job", &job);
        assert!(result.is_err());
        let msg = format!("{}", result.expect_err("expected error"));
        assert!(msg.contains("table"), "error should mention 'table': {msg}");
    }

    // --- aggregate ---

    #[test]
    fn aggregate_transform_basic() {
        let mut job = base_job();
        job.sources = vec![s3_source("orders", "s3://b/orders")];
        let mut aggs = HashMap::new();
        aggs.insert("total".to_string(), "sum(amount)".to_string());
        aggs.insert("n".to_string(), "count(*)".to_string());
        job.transforms = vec![yard_structs::Transform {
            transform_type: "aggregate".to_string(),
            source: Some("orders".to_string()),
            output: Some("daily".to_string()),
            group_by: vec!["region".to_string(), "day".to_string()],
            aggs,
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("from pyspark.sql import functions as F"));
        assert!(script.contains(
            "df_daily = df_orders.groupBy(\"region\", \"day\").agg(F.expr(\"count(*)\").alias(\"n\"), F.expr(\"sum(amount)\").alias(\"total\"))"
        ));
    }

    #[test]
    fn aggregate_defaults_to_first_source() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/events")];
        let mut aggs = HashMap::new();
        aggs.insert("n".to_string(), "count(*)".to_string());
        job.transforms = vec![yard_structs::Transform {
            transform_type: "aggregate".to_string(),
            group_by: vec!["user".to_string()],
            aggs,
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains(
            "df_events = df_events.groupBy(\"user\").agg(F.expr(\"count(*)\").alias(\"n\"))"
        ));
    }

    // --- window ---

    #[test]
    fn window_transform_partition_and_order() {
        let mut job = base_job();
        job.sources = vec![s3_source("orders", "s3://b/orders")];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "window".to_string(),
            source: Some("orders".to_string()),
            output: Some("ranked".to_string()),
            name: Some("row_num".to_string()),
            expression: Some("row_number()".to_string()),
            partition_by: vec!["customer_id".to_string()],
            order_by: vec![
                yard_structs::OrderBySpec {
                    column: "created_at".to_string(),
                    desc: true,
                },
                yard_structs::OrderBySpec {
                    column: "id".to_string(),
                    desc: false,
                },
            ],
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("from pyspark.sql import functions as F"));
        assert!(script.contains("from pyspark.sql.window import Window"));
        assert!(script.contains(
            "_w_row_num = Window.partitionBy(\"customer_id\").orderBy(F.col(\"created_at\").desc(), F.col(\"id\").asc())"
        ));
        assert!(script.contains(
            "df_ranked = df_orders.withColumn(\"row_num\", F.expr(\"row_number()\").over(_w_row_num))"
        ));
    }

    #[test]
    fn window_transform_partition_only() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/events")];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "window".to_string(),
            name: Some("cnt".to_string()),
            expression: Some("count(*)".to_string()),
            partition_by: vec!["user".to_string()],
            ..Default::default()
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("_w_cnt = Window.partitionBy(\"user\")"));
        assert!(!script.contains(".orderBy("));
    }

    #[test]
    fn bash_job_generates_empty_script() {
        let job = JobDefinition {
            job_type: JobType::Bash,
            config: json!({"type": "bash", "command": "echo hi"}),
            ..Default::default()
        };
        let script = generate_python_script("t", &job).unwrap();
        assert!(
            script.is_empty(),
            "expected empty script for task-only type, got: {script}"
        );
    }

    #[test]
    fn aggregate_and_window_share_functions_import() {
        let mut job = base_job();
        job.sources = vec![s3_source("orders", "s3://b/orders")];
        let mut aggs = HashMap::new();
        aggs.insert("total".to_string(), "sum(amount)".to_string());
        job.transforms = vec![
            yard_structs::Transform {
                transform_type: "aggregate".to_string(),
                output: Some("totals".to_string()),
                group_by: vec!["region".to_string()],
                aggs,
                ..Default::default()
            },
            yard_structs::Transform {
                transform_type: "window".to_string(),
                source: Some("totals".to_string()),
                output: Some("ranked".to_string()),
                name: Some("rank".to_string()),
                expression: Some("rank()".to_string()),
                partition_by: vec!["region".to_string()],
                ..Default::default()
            },
        ];
        let script = generate_python_script("test_job", &job).unwrap();
        assert_eq!(
            script.matches("from pyspark.sql import functions as F").count(),
            1,
            "F import should only appear once"
        );
        assert!(script.contains("from pyspark.sql.window import Window"));
    }

    // --- PII masking ---

    #[test]
    fn pii_single_entity_generates_detect_block() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        job.mask_pii = vec!["USA_SSN".to_string()];
        let script = generate_python_script("test_job", &job).unwrap();

        // Section header (GEN-05)
        assert!(script.contains("# --- PII Masking ---"));
        // Import management (GEN-04)
        assert!(script.contains("from awsglueml.transforms import EntityDetector"));
        assert!(script.contains("from awsglue.dynamicframe import DynamicFrame"));
        // DynamicFrame conversion sandwich (GEN-02)
        assert!(script.contains("DynamicFrame.fromDF(df_events"));
        assert!(script.contains(".toDF()"));
        // EntityDetector.detect call (GEN-01)
        assert!(script.contains("EntityDetector.detect("));
        assert!(script.contains("\"USA_SSN\""));
        assert!(script.contains("\"REDACT\""));
        assert!(script.contains("\"****\""));
        // _yard_pii_ prefix (GEN-06)
        assert!(script.contains("_yard_pii_dyf"));
        // Metadata column dropped (GEN-03)
        assert!(script.contains(".drop(\"DetectedEntities\")"));
    }

    #[test]
    fn pii_multiple_entities_all_present() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        job.mask_pii = vec![
            "USA_SSN".to_string(),
            "CREDIT_CARD".to_string(),
            "EMAIL".to_string(),
        ];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("\"USA_SSN\""));
        assert!(script.contains("\"CREDIT_CARD\""));
        assert!(script.contains("\"EMAIL\""));
    }

    #[test]
    fn pii_block_between_transforms_and_sink() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "filter".to_string(),
            condition: Some("col('active')".to_string()),
            ..Default::default()
        }];
        job.sink = Some(yard_structs::Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        job.mask_pii = vec!["USA_SSN".to_string()];
        let script = generate_python_script("test_job", &job).unwrap();

        let transforms_pos = script
            .find("# --- Transforms ---")
            .expect("Transforms section must exist");
        let pii_pos = script
            .find("# --- PII Masking ---")
            .expect("PII Masking section must exist");
        let sink_pos = script
            .find("# --- Sink ---")
            .expect("Sink section must exist");
        assert!(
            transforms_pos < pii_pos,
            "PII block must come after transforms"
        );
        assert!(pii_pos < sink_pos, "PII block must come before sink");
    }

    #[test]
    fn pii_uses_sink_source_variable() {
        let mut job = base_job();
        job.sources = vec![
            s3_source("orders", "s3://b/orders"),
            s3_source("customers", "s3://b/customers"),
        ];
        job.transforms = vec![yard_structs::Transform {
            transform_type: "join".to_string(),
            output: Some("enriched".to_string()),
            left: Some("orders".to_string()),
            right: Some("customers".to_string()),
            on: Some("customer_id".to_string()),
            how: Some("left".to_string()),
            ..Default::default()
        }];
        job.sink = Some(yard_structs::Sink {
            source: Some("enriched".to_string()),
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        job.mask_pii = vec!["USA_SSN".to_string()];
        let script = generate_python_script("test_job", &job).unwrap();

        // PII block operates on the sink source variable, not the first source
        assert!(script.contains("DynamicFrame.fromDF(df_enriched"));
        assert!(script.contains("df_enriched = _yard_pii_dyf.toDF()"));
    }

    #[test]
    fn pii_empty_mask_no_artifacts() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        // mask_pii is empty (default)
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(
            !script.contains("EntityDetector"),
            "empty mask_pii should produce no EntityDetector reference"
        );
        assert!(
            !script.contains("_yard_pii_"),
            "empty mask_pii should produce no _yard_pii_ variables"
        );
        assert!(
            !script.contains("PII Masking"),
            "empty mask_pii should produce no PII Masking section"
        );
        assert!(
            !script.contains("awsglueml"),
            "empty mask_pii should produce no awsglueml import"
        );
    }

    #[test]
    fn pii_body_override_skips_pii() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.body = Some("print('custom')".to_string());
        job.mask_pii = vec!["USA_SSN".to_string()];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(
            !script.contains("EntityDetector"),
            "body override must skip PII codegen"
        );
        assert!(
            !script.contains("_yard_pii_"),
            "body override must produce no PII variables"
        );
        assert!(
            script.contains("print('custom')"),
            "body content must still be present"
        );
    }

    #[test]
    fn pii_job_file_skips_pii() {
        let dir = std::env::temp_dir().join(format!("yard_pii_jf_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let script_path = dir.join("external.py");
        std::fs::write(&script_path, "print('external')\n").unwrap();

        let mut job = base_job();
        job.job_file = Some(script_path.to_string_lossy().to_string());
        job.mask_pii = vec!["USA_SSN".to_string()];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(
            !script.contains("EntityDetector"),
            "job_file must skip PII codegen"
        );
        assert!(
            !script.contains("_yard_pii_"),
            "job_file must produce no PII variables"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pii_no_duplicate_dynamicframe_import() {
        let mut job = base_job();
        // Catalog source already triggers DynamicFrame import
        job.sources = vec![yard_structs::Source {
            name: "catalog_src".to_string(),
            source_type: "catalog".to_string(),
            database: Some("mydb".to_string()),
            table: Some("mytable".to_string()),
            ..Default::default()
        }];
        job.sink = Some(yard_structs::Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        job.mask_pii = vec!["USA_SSN".to_string()];
        let script = generate_python_script("test_job", &job).unwrap();

        let count = script
            .matches("from awsglue.dynamicframe import DynamicFrame")
            .count();
        assert_eq!(
            count, 1,
            "DynamicFrame import must appear exactly once, found {count}"
        );
    }

    #[test]
    fn pii_existing_non_pii_jobs_unchanged() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(yard_structs::Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out".to_string()),
            mode: Some("overwrite".to_string()),
            ..Default::default()
        });
        // No mask_pii set (default empty vec)
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(
            !script.contains("_yard_pii_"),
            "non-PII job must have no PII variables"
        );
        assert!(
            !script.contains("awsglueml"),
            "non-PII job must have no awsglueml import"
        );
        // Regression: sink write line still present
        assert!(
            script.contains("df_events.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://b/out\")"),
            "sink write line must be present for non-PII job"
        );
    }

}
