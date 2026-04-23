mod helpers;
mod sink;
mod source;
mod transform;

use anyhow::{Context as AnyhowContext, Result, anyhow};
use tera::{Context, Tera};
use yard_structs::JobDefinition;

// Re-import sub-module items so they are in scope for generate_python_script
// and for `use super::*` in the test module
use helpers::*;
use sink::render_sink;
use source::render_sources;
use transform::render_transforms;

const GLUE_TEMPLATE: &str = include_str!("../templates/glue.py.tera");
const EMR_TEMPLATE: &str = include_str!("../templates/emr.py.tera");

/// Emitted inline at module scope when an iceberg sink is writing with
/// `fill_nulls` enabled. Coerces null/void-typed columns (common in JSON
/// ingestion) into type-appropriate defaults so Iceberg writes don't fail on
/// void schemas or unresolvable nullable nested types. Opt-out via
/// `fill_nulls: false` on the sink.
const ICEBERG_FILL_NULLS_HELPERS: &str = r#"def _yard_default_struct(struct_type):
    out = []
    for f in struct_type.fields:
        dt = f.dataType
        if isinstance(dt, StructType):
            out.append(_yard_default_struct(dt).alias(f.name))
        elif isinstance(dt, (DoubleType, FloatType)):
            out.append(F.lit(0.0).cast(dt).alias(f.name))
        elif isinstance(dt, (IntegerType, LongType)):
            out.append(F.lit(0).cast(dt).alias(f.name))
        elif isinstance(dt, ArrayType):
            out.append(F.array().cast(dt).alias(f.name))
        elif isinstance(dt, (TimestampType, DateType)):
            out.append(F.lit(None).cast(dt).alias(f.name))
        elif isinstance(dt, BooleanType):
            out.append(F.lit(False).alias(f.name))
        else:
            out.append(F.lit("").cast("string").alias(f.name))
    return F.struct(*out)


def _yard_coerce_struct_voids(col, struct_type):
    fields = []
    for f in struct_type.fields:
        dt = f.dataType
        sub = col[f.name]
        if isinstance(dt, NullType):
            fields.append(F.lit("").alias(f.name))
        elif isinstance(dt, StructType):
            fields.append(_yard_coerce_struct_voids(sub, dt).alias(f.name))
        elif isinstance(dt, ArrayType) and isinstance(dt.elementType, NullType):
            fields.append(F.when(sub.isNull(), F.array().cast("array<string>"))
                .otherwise(F.transform(sub, lambda _: F.lit(""))).alias(f.name))
        elif isinstance(dt, ArrayType) and isinstance(dt.elementType, StructType):
            inner = _yard_default_struct(dt.elementType)
            fields.append(F.when(sub.isNull(), F.array().cast(dt))
                .otherwise(F.transform(sub, lambda x: F.when(x.isNull(), inner).otherwise(_yard_coerce_struct_voids(x, dt.elementType)))).alias(f.name))
        else:
            fields.append(sub.alias(f.name))
    return F.struct(*fields)


def _yard_fill_nulls(df):
    for field in df.schema.fields:
        dt, name = field.dataType, field.name
        col = F.col(f"`{name}`")
        if isinstance(dt, NullType):
            df = df.withColumn(name, F.coalesce(col.cast("string"), F.lit("")))
        elif isinstance(dt, StructType):
            df = df.withColumn(name, F.when(col.isNull(), _yard_default_struct(dt)).otherwise(_yard_coerce_struct_voids(col, dt)))
        elif isinstance(dt, ArrayType):
            et = dt.elementType
            if isinstance(et, NullType):
                df = df.withColumn(name, F.when(col.isNull(), F.array().cast("array<string>"))
                    .otherwise(F.transform(col, lambda _: F.lit(""))))
            elif isinstance(et, StructType):
                inner = _yard_default_struct(et)
                df = df.withColumn(name, F.when(col.isNull(), F.array().cast(dt))
                    .otherwise(F.transform(col, lambda x: F.when(x.isNull(), inner).otherwise(_yard_coerce_struct_voids(x, et)))))
            else:
                df = df.withColumn(name, F.when(col.isNull(), F.array().cast(dt)).otherwise(col))
        elif isinstance(dt, (DoubleType, FloatType, IntegerType, LongType)):
            df = df.withColumn(name, F.coalesce(col, F.lit(0).cast(dt)))
        elif isinstance(dt, BooleanType):
            df = df.withColumn(name, F.coalesce(col, F.lit(False)))
        else:
            df = df.withColumn(name, F.coalesce(col.cast("string"), F.lit("")))
    return df
"#;

pub fn generate_python_script(job_name: &str, job_def: &JobDefinition) -> Result<String> {
    // Task-only job types (bash, ...) don't produce a standalone PySpark script;
    // they participate in Airflow DAG codegen instead. Return an empty string so
    // callers that blindly hash/write the script output continue to work -- the
    // apply path skips deploy for these types via `is_task_only`.
    if crate::is_task_only(&job_def.job_type) {
        return Ok(String::new());
    }

    // If job_file is specified, use the external file as the complete script
    if let Some(ref path) = job_def.job_file {
        return std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read job_file: {path}"));
    }

    let template = match job_def.job_type.as_str() {
        "glue" => GLUE_TEMPLATE,
        "emr" => EMR_TEMPLATE,
        other => return Err(anyhow!("Unsupported job type: {}", other)),
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

    // Build extra imports needed by template features
    let mut extra_imports = Vec::new();
    let iceberg = has_iceberg_sink(job_def);
    let partitioning = !job_def.partition_by.is_empty();
    if needs_secrets_imports(job_def) || iceberg {
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
            "from pyspark.sql.types import (StructType, ArrayType, DoubleType, FloatType, \
             IntegerType, LongType, TimestampType, DateType, BooleanType, NullType)"
                .to_string(),
        );
    }
    if needs_window_import(job_def) {
        extra_imports.push("from pyspark.sql.window import Window".to_string());
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
        let mut parts = Vec::new();
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
            if fill_nulls {
                let var = format!("df_{sink_source}");
                parts.push(format!(
                    "    # --- Null/void coercion for Iceberg ---\n    \
                     {var} = _yard_fill_nulls({var})"
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
                render_sink(&effective_sink, default_source)?
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
    let warehouse = job_def
        .config
        .get("glue")
        .and_then(|g| g.get("warehouse"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    context.insert("iceberg_warehouse", warehouse);

    let rendered = tera
        .render("script", &context)
        .context("Failed to render Python template")?;

    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn base_job() -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
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

    #[test]
    fn unsupported_job_type_errors() {
        let mut job = base_job();
        job.job_type = "unknown".to_string();
        assert!(generate_python_script("test", &job).is_err());
    }

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
        assert!(script.contains(".writeTo(_tbl).option(\"mergeSchema\", \"true\").append()"));
    }

    #[test]
    fn iceberg_sink_empty_path_omits_location() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", Some("")));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(!script.contains(".tableProperty(\"location\""));
    }

    // --- fill_nulls helper shape (Phase 14 regression matrix) ---

    #[test]
    fn fill_nulls_uses_exact_null_type_check() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // New pattern wired (FILL-02 helper body)
        assert!(script.contains("isinstance(dt, NullType)"));
        // Old buggy substring-match fully gone (regression guard)
        assert!(!script.contains("\"void\" in dt.simpleString()"));
        // NullType appended to emitted pyspark.sql.types imports (FILL-02 import side, D-10)
        assert!(script.contains("BooleanType, NullType)"));
    }

    #[test]
    fn fill_nulls_top_level_void_still_coerced() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Top-level NullType branch still coerces to empty string (FILL-03/FILL-05.a)
        assert!(script.contains("F.coalesce(col.cast(\"string\"), F.lit(\"\"))"));
    }

    #[test]
    fn fill_nulls_null_struct_still_defaults() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Null-struct branch still invokes _yard_default_struct (FILL-03/FILL-05.b; D-06)
        assert!(script.contains("_yard_default_struct(dt)"));
        // StructType branch still wires the null-check guard
        assert!(script.contains("F.when(col.isNull(), _yard_default_struct(dt))"));
    }

    #[test]
    fn fill_nulls_other_branches_intact() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // ArrayType branch (with and without nested struct element)
        assert!(script.contains("elif isinstance(dt, ArrayType):"));
        assert!(script.contains("F.array().cast(dt)"));
        // Numeric branch (Double/Float/Integer/Long coalesce to 0)
        assert!(script.contains("elif isinstance(dt, (DoubleType, FloatType, IntegerType, LongType)):"));
        assert!(script.contains("F.coalesce(col, F.lit(0).cast(dt))"));
        // Boolean branch (coalesce to False)
        assert!(script.contains("elif isinstance(dt, BooleanType):"));
        assert!(script.contains("F.coalesce(col, F.lit(False))"));
        // Fallback branch (else → coerce to empty string)
        assert!(script.contains("else:\n            df = df.withColumn(name, F.coalesce(col.cast(\"string\"), F.lit(\"\")))"));
    }

    #[test]
    fn fill_nulls_coerces_nested_struct_voids() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Non-null struct branch routes through FILL-06 helper (D-12)
        assert!(script.contains("_yard_coerce_struct_voids(col, dt)"));
        // The FILL-06 helper is defined in the emitted script
        assert!(script.contains("def _yard_coerce_struct_voids(col, struct_type):"));
        // The helper's NullType branch emits an empty-string alias (preserves outer struct shape)
        assert!(script.contains("F.lit(\"\").alias(f.name)"));
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
        assert!(script.contains("users_source_secret[\"username\"]"));
        assert!(script.contains("df_users = spark.read.format(\"jdbc\")"));
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
            job_type: "bash".to_string(),
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
}
