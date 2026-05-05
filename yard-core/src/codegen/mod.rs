mod helpers;
mod sink;
mod source;
mod transform;

use anyhow::{Context as AnyhowContext, Result, anyhow};
use tera::{Context, Tera};
use yard_structs::{JobDefinition, JobType};

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
    if len(struct_type.fields) == 0:
        return F.struct(F.lit("").cast("string").alias("_yard_empty"))
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
            out.append(F.array().cast(_yard_void_free_ddl(dt)).alias(f.name))
        elif isinstance(dt, MapType):
            out.append(F.create_map().cast(_yard_void_free_ddl(dt)).alias(f.name))
        elif isinstance(dt, (TimestampType, DateType)):
            out.append(F.lit(None).cast(dt).alias(f.name))
        elif isinstance(dt, BooleanType):
            out.append(F.lit(False).alias(f.name))
        else:
            out.append(F.lit("").cast("string").alias(f.name))
    return F.struct(*out)


def _yard_has_void(dt):
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
        return "string"
    if isinstance(dt, StructType):
        if len(dt.fields) == 0:
            return "struct<`_yard_empty`:string>"
        parts = ["`" + f.name.replace("`", "``") + "`:" + _yard_void_free_ddl(f.dataType) for f in dt.fields]
        return "struct<" + ",".join(parts) + ">"
    if isinstance(dt, ArrayType):
        return "array<" + _yard_void_free_ddl(dt.elementType) + ">"
    if isinstance(dt, MapType):
        k = "string" if isinstance(dt.keyType, NullType) else _yard_void_free_ddl(dt.keyType)
        v = "string" if isinstance(dt.valueType, NullType) else _yard_void_free_ddl(dt.valueType)
        return "map<" + k + "," + v + ">"
    return dt.simpleString()


def _yard_coerce_voids(col, dt):
    if isinstance(dt, NullType):
        return F.coalesce(col.cast("string"), F.lit(""))
    if isinstance(dt, StructType):
        if len(dt.fields) == 0:
            return F.struct(F.lit("").cast("string").alias("_yard_empty"))
        fields = [_yard_coerce_voids(col[f.name], f.dataType).alias(f.name) for f in dt.fields]
        return F.struct(*fields)
    if isinstance(dt, ArrayType):
        et = dt.elementType
        target = _yard_void_free_ddl(dt)
        if isinstance(et, NullType):
            return F.when(col.isNull(), F.lit(None).cast(target)) \
                .otherwise(F.transform(col, lambda x: F.coalesce(x.cast("string"), F.lit(""))))
        if _yard_has_void(et):
            return F.when(col.isNull(), F.lit(None).cast(target)) \
                .otherwise(F.transform(col, lambda x: _yard_coerce_voids(x, et)))
        return col
    if isinstance(dt, MapType):
        target = _yard_void_free_ddl(dt)
        if isinstance(dt.keyType, NullType):
            return F.create_map().cast("map<string,string>")
        if isinstance(dt.valueType, NullType):
            return F.when(col.isNull(), F.lit(None).cast(target)) \
                .otherwise(F.map_from_arrays(F.map_keys(col),
                    F.transform(F.map_values(col), lambda _: F.lit(""))))
        if _yard_has_void(dt.valueType):
            return F.when(col.isNull(), F.lit(None).cast(target)) \
                .otherwise(F.map_from_arrays(F.map_keys(col),
                    F.transform(F.map_values(col), lambda v: _yard_coerce_voids(v, dt.valueType))))
        return col
    return col


def _yard_fill_nulls(df):
    for field in df.schema.fields:
        dt, name = field.dataType, field.name
        col = F.col(f"`{name}`")
        if isinstance(dt, NullType):
            df = df.withColumn(name, F.coalesce(col.cast("string"), F.lit("")))
        elif isinstance(dt, StructType):
            if _yard_has_void(dt):
                df = df.withColumn(name, F.when(col.isNull(), _yard_default_struct(dt)).otherwise(_yard_coerce_voids(col, dt)))
            else:
                df = df.withColumn(name, F.when(col.isNull(), _yard_default_struct(dt)).otherwise(col))
        elif isinstance(dt, ArrayType):
            target = _yard_void_free_ddl(dt)
            if _yard_has_void(dt):
                df = df.withColumn(name, F.when(col.isNull(), F.array().cast(target)).otherwise(_yard_coerce_voids(col, dt)))
            else:
                df = df.withColumn(name, F.when(col.isNull(), F.array().cast(dt)).otherwise(col))
        elif isinstance(dt, MapType):
            if _yard_has_void(dt):
                df = df.withColumn(name, _yard_coerce_voids(col, dt))
        elif isinstance(dt, (DoubleType, FloatType, IntegerType, LongType, ShortType, ByteType)):
            df = df.withColumn(name, F.coalesce(col, F.lit(0).cast(dt)))
        elif isinstance(dt, BooleanType):
            df = df.withColumn(name, F.coalesce(col, F.lit(False)))
        elif isinstance(dt, (TimestampType, DateType, DecimalType, BinaryType)):
            pass
        else:
            df = df.withColumn(name, F.coalesce(col.cast("string"), F.lit("")))
    return df


def _yard_read_iceberg_schema(spark, tbl):
    return spark.read.format("iceberg").load(tbl).schema


def _yard_void_to_target(col, src_dt, tgt_dt):
    if not _yard_has_void(src_dt):
        return col
    if isinstance(src_dt, NullType):
        return F.lit(None).cast(tgt_dt)
    if isinstance(src_dt, StructType) and isinstance(tgt_dt, StructType):
        if len(src_dt.fields) == 0:
            if len(tgt_dt.fields) == 0:
                return F.when(col.isNull(), F.lit(None).cast(tgt_dt)) \
                    .otherwise(F.struct(F.lit("").cast("string").alias("_yard_empty")))
            return F.when(col.isNull(), F.lit(None).cast(tgt_dt)) \
                .otherwise(F.struct(*[F.lit(None).cast(f.dataType).alias(f.name) for f in tgt_dt.fields]))
        tgt_fields = {f.name: f.dataType for f in tgt_dt.fields}
        out = []
        for f in src_dt.fields:
            sub = col[f.name]
            if f.name in tgt_fields:
                out.append(_yard_void_to_target(sub, f.dataType, tgt_fields[f.name]).alias(f.name))
            elif _yard_has_void(f.dataType):
                out.append(sub.cast(_yard_void_free_ddl(f.dataType)).alias(f.name))
            else:
                out.append(sub.alias(f.name))
        return F.when(col.isNull(), F.lit(None).cast(tgt_dt)).otherwise(F.struct(*out))
    if isinstance(src_dt, ArrayType) and isinstance(tgt_dt, ArrayType):
        src_et, tgt_et = src_dt.elementType, tgt_dt.elementType
        if isinstance(src_et, NullType):
            return F.when(col.isNull(), F.lit(None).cast(tgt_dt)) \
                .otherwise(F.transform(col, lambda x: F.lit(None).cast(tgt_et)))
        if _yard_has_void(src_et):
            return F.when(col.isNull(), F.lit(None).cast(tgt_dt)) \
                .otherwise(F.transform(col, lambda x: _yard_void_to_target(x, src_et, tgt_et)))
        return col
    if isinstance(src_dt, MapType) and isinstance(tgt_dt, MapType):
        src_kt, src_vt = src_dt.keyType, src_dt.valueType
        tgt_vt = tgt_dt.valueType
        if isinstance(src_kt, NullType):
            return F.lit(None).cast(tgt_dt)
        if isinstance(src_vt, NullType):
            return F.when(col.isNull(), F.lit(None).cast(tgt_dt)) \
                .otherwise(F.map_from_arrays(F.map_keys(col),
                    F.transform(F.map_values(col), lambda v: F.lit(None).cast(tgt_vt))))
        if _yard_has_void(src_vt):
            return F.when(col.isNull(), F.lit(None).cast(tgt_dt)) \
                .otherwise(F.map_from_arrays(F.map_keys(col),
                    F.transform(F.map_values(col), lambda v: _yard_void_to_target(v, src_vt, tgt_vt))))
        return col
    return col


def _yard_conform_to_target_schema(df, spark, tbl):
    tgt = _yard_read_iceberg_schema(spark, tbl)
    tgt_map = {f.name: f.dataType for f in tgt.fields}
    for field in df.schema.fields:
        name, src_dt = field.name, field.dataType
        if name not in tgt_map:
            if _yard_has_void(src_dt):
                col = F.col(f"`{name}`")
                df = df.withColumn(name, col.cast(_yard_void_free_ddl(src_dt)))
            continue
        if _yard_has_void(src_dt):
            col = F.col(f"`{name}`")
            df = df.withColumn(name, _yard_void_to_target(col, src_dt, tgt_map[name]))
    return df
"#;

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
            "from pyspark.sql.types import (StructType, ArrayType, MapType, DoubleType, FloatType, \
             IntegerType, LongType, ShortType, ByteType, TimestampType, DateType, DecimalType, \
             BinaryType, BooleanType, NullType)"
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
                render_sink(&effective_sink, default_source, fill_nulls)?
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

    // --- fill_nulls helper shape (recursive void coercion) ---

    #[test]
    fn fill_nulls_uses_exact_null_type_check() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Exact NullType check wired in both _yard_fill_nulls and _yard_coerce_voids
        assert!(script.contains("isinstance(dt, NullType)"));
        // Old buggy substring-match fully gone
        assert!(!script.contains("\"void\" in dt.simpleString()"));
    }

    #[test]
    fn fill_nulls_top_level_void_still_coerced() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("F.coalesce(col.cast(\"string\"), F.lit(\"\"))"));
    }

    #[test]
    fn fill_nulls_null_struct_still_defaults() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("_yard_default_struct(dt)"));
        assert!(script.contains("F.when(col.isNull(), _yard_default_struct(dt))"));
    }

    #[test]
    fn fill_nulls_other_branches_intact() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("elif isinstance(dt, ArrayType):"));
        assert!(script.contains("F.array().cast(dt)"));
        assert!(script.contains("elif isinstance(dt, (DoubleType, FloatType, IntegerType, LongType, ShortType, ByteType)):"));
        assert!(script.contains("F.coalesce(col, F.lit(0).cast(dt))"));
        assert!(script.contains("elif isinstance(dt, BooleanType):"));
        assert!(script.contains("F.coalesce(col, F.lit(False))"));
        assert!(script.contains("elif isinstance(dt, (TimestampType, DateType, DecimalType, BinaryType)):"));
        assert!(script.contains("else:\n            df = df.withColumn(name, F.coalesce(col.cast(\"string\"), F.lit(\"\")))"));
    }

    #[test]
    fn fill_nulls_does_not_string_cast_timestamp_date_decimal_binary() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Imports cover every type referenced in the helper body.
        assert!(script.contains("ShortType, ByteType"));
        assert!(script.contains("DecimalType, \\\n             BinaryType") || script.contains("DecimalType, BinaryType"));
        // Pass-through branch exists and uses `pass` (no withColumn cast).
        assert!(script.contains("elif isinstance(dt, (TimestampType, DateType, DecimalType, BinaryType)):\n            pass\n"));
    }

    #[test]
    fn fill_nulls_defines_recursive_helpers() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Three recursive helpers power the coercion at arbitrary nesting depth
        assert!(script.contains("def _yard_has_void(dt):"));
        assert!(script.contains("def _yard_void_free_ddl(dt):"));
        assert!(script.contains("def _yard_coerce_voids(col, dt):"));
        // _yard_has_void recognizes every container type
        assert!(script.contains("isinstance(dt, StructType)"));
        assert!(script.contains("isinstance(dt, ArrayType)"));
        assert!(script.contains("isinstance(dt, MapType)"));
    }

    #[test]
    fn fill_nulls_routes_void_containers_through_coerce_voids() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Top-level struct/array branches gate on _yard_has_void and dispatch to _yard_coerce_voids
        assert!(script.contains("if _yard_has_void(dt):"));
        assert!(script.contains(".otherwise(_yard_coerce_voids(col, dt))"));
    }

    #[test]
    fn fill_nulls_handles_maps() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // MapType added to emitted imports
        assert!(script.contains("StructType, ArrayType, MapType,"));
        // MapType branch exists in _yard_fill_nulls
        assert!(script.contains("elif isinstance(dt, MapType):"));
        // Void-value map path: preserve keys, rewrite values to empty strings via map_from_arrays
        assert!(script.contains("F.map_from_arrays(F.map_keys(col)"));
        assert!(script.contains("F.transform(F.map_values(col), lambda _: F.lit(\"\"))"));
        // Void-key map path: drop to empty string-string map (keys carried no data)
        assert!(script.contains("F.create_map().cast(\"map<string,string>\")"));
    }

    #[test]
    fn coerce_voids_recurses_through_arrays_and_maps() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // ArrayType recursion: non-void element types pass through col; voidful arrays recurse via F.transform
        assert!(script.contains("F.transform(col, lambda x: _yard_coerce_voids(x, et))"));
        // Nested void arrays/maps get cast to the void-free target type on null
        assert!(script.contains("F.lit(None).cast(target)"));
        // MapType value recursion: structurally-voidful value types recurse through _yard_coerce_voids
        assert!(script.contains("lambda v: _yard_coerce_voids(v, dt.valueType)"));
    }

    #[test]
    fn fill_nulls_handles_empty_structs() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Empty structs (struct<>) get a synthetic _yard_empty: string field so Parquet doesn't reject them
        assert!(script.contains("if len(struct_type.fields) == 0:"));
        assert!(script.contains("if len(dt.fields) == 0:"));
        assert!(script.contains("F.struct(F.lit(\"\").cast(\"string\").alias(\"_yard_empty\"))"));
        assert!(script.contains("\"struct<`_yard_empty`:string>\""));
        // _yard_has_void flags empty structs so they get routed through coercion
        assert!(script.contains("if len(dt.fields) == 0:\n            return True"));
    }

    #[test]
    fn void_free_ddl_covers_all_container_types() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // _yard_void_free_ddl rewrites NullType → "string" and recurses through containers
        assert!(script.contains("if isinstance(dt, NullType):\n        return \"string\""));
        assert!(script.contains("\"array<\" + _yard_void_free_ddl(dt.elementType) + \">\""));
        assert!(script.contains("\"map<\" + k + \",\" + v + \">\""));
        // Struct DDL backtick-quotes field names to survive dots/spaces
        assert!(script.contains("\"`\" + f.name.replace(\"`\", \"``\") + \"`:\""));
    }

    // --- iceberg per-branch coerce: fill_nulls (new-table) vs schema-aware conform (existing-table) ---

    #[test]
    fn existing_table_branch_uses_schema_aware_conform_append() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // Existing-table branch reads the live target schema rather than
        // re-running source-side fill_nulls — preserves nulls in nullable
        // real-typed columns and routes void subtypes through target type.
        assert!(
            script.contains("df_events = _yard_conform_to_target_schema(df_events, spark, _tbl)"),
            "existing-table branch must invoke _yard_conform_to_target_schema"
        );
        // Confirm we land in the append arm, not overwritePartitions.
        assert!(script.contains("df_events.writeTo(_tbl).option(\"mergeSchema\", \"true\").append()"));
    }

    #[test]
    fn existing_table_branch_uses_schema_aware_conform_overwrite() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        if let Some(s) = job.sink.as_mut() {
            s.mode = Some("overwrite".to_string());
        }
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_events = _yard_conform_to_target_schema(df_events, spark, _tbl)"));
        // Overwrite mode maps to overwritePartitions, not append.
        assert!(script.contains("df_events.writeTo(_tbl).option(\"mergeSchema\", \"true\").overwritePartitions()"));
    }

    #[test]
    fn new_table_branch_preserves_fill_nulls() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(
            script.contains("df_events = _yard_fill_nulls(df_events)"),
            "new-table branch must still invoke _yard_fill_nulls on the DF (D-04 fallback)"
        );
        // Confirm the create-branch itself is still structurally present.
        assert!(script.contains("if not spark.catalog.tableExists(_tbl):"));
        assert!(script.contains(".create())"));
    }

    #[test]
    fn schema_aware_conform_only_inside_else_branch() {
        // Regression guard for Sean's gating concern: _yard_conform_to_target_schema
        // reads the live Iceberg metadata, so it must NEVER fire on the new-table
        // (.create()) branch — if it does, the read errors before the table exists.
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();

        // Locate the if/else block. The conform call must sit AFTER the `else:`
        // line and BEFORE the existing-table writeTo on that same arm. The
        // new-table branch (between `if not ... tableExists(_tbl):` and `else:`)
        // must contain _yard_fill_nulls but NOT _yard_conform_to_target_schema.
        let if_idx = script
            .find("if not spark.catalog.tableExists(_tbl):")
            .expect("if/tableExists block must be present");
        let else_idx = script[if_idx..]
            .find("\n    else:\n")
            .map(|o| if_idx + o)
            .expect("else: branch must follow the tableExists check");
        let new_table_block = &script[if_idx..else_idx];
        let existing_table_block = &script[else_idx..];

        assert!(
            new_table_block.contains("_yard_fill_nulls(df_events)"),
            "new-table branch must call _yard_fill_nulls"
        );
        assert!(
            !new_table_block.contains("_yard_conform_to_target_schema"),
            "_yard_conform_to_target_schema must NOT appear on the new-table branch — \
             it reads live Iceberg metadata and would error before the table exists"
        );
        assert!(
            existing_table_block.contains("_yard_conform_to_target_schema(df_events, spark, _tbl)"),
            "existing-table branch must call _yard_conform_to_target_schema"
        );
        assert!(
            !existing_table_block.contains("_yard_fill_nulls(df_events)"),
            "existing-table branch must NOT call _yard_fill_nulls — that path drops \
             schema awareness and overwrites real nulls with typed defaults (0/False/empty)"
        );
    }

    #[test]
    fn schema_aware_helpers_in_prelude() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // The three new helpers that drive the schema-aware existing-table path.
        assert!(script.contains("def _yard_read_iceberg_schema(spark, tbl):"));
        assert!(script.contains("def _yard_void_to_target(col, src_dt, tgt_dt):"));
        assert!(script.contains("def _yard_conform_to_target_schema(df, spark, tbl):"));
        // Schema-aware empty-struct expansion uses target field shape — when the
        // target struct has real fields, no _yard_empty placeholder is emitted
        // for the existing-table path. (The new-table path still synthesizes
        // _yard_empty via _yard_default_struct because parquet rejects struct<>.)
        assert!(script.contains("F.lit(None).cast(f.dataType).alias(f.name)"));
    }

    #[test]
    fn no_try_except_around_write() {
        let mut job = base_job();
        job.sources = vec![s3_source("events", "s3://b/in")];
        job.sink = Some(iceberg_sink("analytics", "events", None));
        let script = generate_python_script("test_job", &job).unwrap();
        // SPEC acceptance 7: no try/except wraps .writeTo(...).option("mergeSchema"...).
        // Two unrelated try/except blocks exist in the rendered script and must not
        // trip this test: (a) the Glue database-create try/except at sink.rs:89-94
        // (`except _glue.exceptions.EntityNotFoundException`), and (b) the template's
        // outer `if __name__ == "__main__":` wrapper in glue.py.tera:44-49
        // (`try: run(); job.commit() except Exception as e:`). Both are unrelated to
        // the Iceberg write — we scope this assertion strictly to the writeTo call-site.
        assert!(
            !script.contains("try:\n        df_events.writeTo(_tbl)"),
            "writeTo(_tbl) on the existing-table branch must not be wrapped in try:"
        );
        // No `except` immediately follows the write's terminating call on either
        // branch (`.append()`, `.overwritePartitions()`, or `.create())`) — which
        // would be the structural signature of a try/except wrapping the write.
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
        // D-07: opt-out preserves verbatim mental model — no yard-side DF rewriting on either branch.
        assert!(
            !script.contains("df_events = _yard_fill_nulls(df_events)"),
            "fill_nulls: false must suppress _yard_fill_nulls on the new-table branch"
        );
        assert!(
            !script.contains("df_events = _yard_conform_to_target_schema(df_events, spark, _tbl)"),
            "fill_nulls: false must suppress _yard_conform_to_target_schema on the existing-table branch"
        );
        // Structural shape of the sink block is otherwise intact.
        assert!(script.contains("if not spark.catalog.tableExists(_tbl):"));
        assert!(script.contains("df_events.writeTo(_tbl).option(\"mergeSchema\", \"true\").append()"));
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
}
