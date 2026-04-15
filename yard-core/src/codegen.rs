use anyhow::{Context as AnyhowContext, Result, anyhow};
use tera::{Context, Tera};
use yard_structs::{Import, JobDefinition, Sink, Source, Transform};

const GLUE_TEMPLATE: &str = include_str!("templates/glue.py.tera");
const EMR_TEMPLATE: &str = include_str!("templates/emr.py.tera");

// --- Import rendering ---

fn render_import(import: &Import) -> String {
    match &import.from {
        Some(module) => format!("from {} import {}", module, import.name),
        None => format!("import {}", import.name),
    }
}

fn render_imports(imports: &[Import]) -> String {
    imports
        .iter()
        .map(render_import)
        .collect::<Vec<_>>()
        .join("\n")
}

// --- Source rendering (now named: produces df_<name>) ---

/// Render a serde_json::Value as a Python literal. Strings, numbers, bools,
/// and null map directly; arrays and objects recurse. Used for opaque
/// `options:` passthrough.
fn python_literal(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "None".to_string(),
        serde_json::Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(python_literal).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort();
            let items: Vec<String> = keys
                .iter()
                .map(|k| format!("\"{}\": {}", k, python_literal(&obj[*k])))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

fn effective_engine(source: &Source, default_engine: &str) -> String {
    source
        .engine
        .clone()
        .unwrap_or_else(|| default_engine.to_string())
}

fn require_str<'a>(value: Option<&'a str>, source_name: &str, field: &str) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("source '{source_name}': '{field}' is required"))
}

/// Build a Python dict literal from a seed of ordered (key, value) pairs,
/// merging in arbitrary user-supplied options afterward.
fn build_options_dict(
    seed: &[(&str, serde_json::Value)],
    user_opts: &std::collections::HashMap<String, serde_json::Value>,
) -> String {
    let mut opts = serde_json::Map::new();
    for (k, v) in seed {
        opts.insert((*k).to_string(), v.clone());
    }
    for (k, v) in user_opts {
        opts.insert(k.clone(), v.clone());
    }
    python_literal(&serde_json::Value::Object(opts))
}

/// Append `.option(k, v)` calls onto a Spark reader chain. `seed` pairs are
/// emitted as literal strings; `extra` entries use `python_literal`.
fn append_spark_options(
    chain: &mut String,
    seed: &[(&str, &str)],
    extra: &std::collections::HashMap<String, serde_json::Value>,
) {
    for (k, v) in seed {
        chain.push_str(&format!(".option(\"{k}\", \"{v}\")"));
    }
    for (k, v) in extra {
        chain.push_str(&format!(".option(\"{}\", {})", k, python_literal(v)));
    }
}

/// Render a `glueContext.create_dynamic_frame.from_options(...).toDF()` call.
/// `options_expr` may be a literal dict or a variable name.
fn glue_from_options(
    var: &str,
    connection_type: &str,
    options_expr: &str,
    ctx: &str,
    format: Option<&str>,
) -> String {
    let format_arg = format
        .map(|f| format!("format=\"{f}\", "))
        .unwrap_or_default();
    format!(
        "    {var} = glueContext.create_dynamic_frame.from_options(\
             connection_type=\"{connection_type}\", \
             {format_arg}connection_options={options_expr}, \
             transformation_ctx=\"{ctx}\").toDF()"
    )
}

fn render_source(source: &Source, default_engine: &str) -> Result<String> {
    let name = &source.name;
    let var = format!("df_{name}");
    let ctx = format!("{name}_ctx");
    let engine = effective_engine(source, default_engine);
    let secret_var = format!("{name}_source_secret");
    let mut lines = Vec::new();

    if let Some(secret_id) = &source.secret_id {
        lines.push(render_secret_fetch(secret_id, &format!("{name}_source")));
    }

    match source.source_type.as_str() {
        "s3" => {
            let format = source.format.as_deref().unwrap_or("parquet");
            let path = require_str(source.path.as_deref(), name, "path")?;
            if engine == "glue" {
                let opts = build_options_dict(
                    &[(
                        "paths",
                        serde_json::Value::Array(vec![serde_json::Value::String(path.into())]),
                    )],
                    &source.options,
                );
                lines.push(glue_from_options(&var, "s3", &opts, &ctx, Some(format)));
            } else {
                let mut chain = format!("spark.read.format(\"{format}\")");
                append_spark_options(&mut chain, &[], &source.options);
                chain.push_str(&format!(".load(\"{path}\")"));
                lines.push(format!("    {var} = {chain}"));
            }
        }
        "jdbc" => {
            let url = require_str(source.connection_url.as_deref(), name, "connection_url")?;
            let table = require_str(source.table.as_deref(), name, "table")?;
            if engine == "glue" {
                let connection_type = source.connection_type.as_deref().ok_or_else(|| {
                    anyhow!(
                        "source '{name}': 'connection_type' is required for jdbc+glue (mysql, postgresql, sqlserver, oracle, redshift)"
                    )
                })?;
                let base_opts = build_options_dict(
                    &[
                        ("url", serde_json::Value::String(url.into())),
                        ("dbtable", serde_json::Value::String(table.into())),
                    ],
                    &source.options,
                );
                let options_expr = if source.secret_id.is_some() {
                    let opts_var = format!("_opts_{name}");
                    lines.push(format!(
                        "    {opts_var} = {{**{base_opts}, \"user\": {secret_var}[\"username\"], \"password\": {secret_var}[\"password\"]}}"
                    ));
                    opts_var
                } else {
                    base_opts
                };
                lines.push(glue_from_options(&var, connection_type, &options_expr, &ctx, None));
            } else {
                let mut chain = "spark.read.format(\"jdbc\")".to_string();
                append_spark_options(
                    &mut chain,
                    &[("url", url), ("dbtable", table)],
                    &source.options,
                );
                if source.secret_id.is_some() {
                    chain.push_str(&format!(
                        ".option(\"user\", {secret_var}[\"username\"]).option(\"password\", {secret_var}[\"password\"])"
                    ));
                }
                chain.push_str(".load()");
                lines.push(format!("    {var} = {chain}"));
            }
        }
        "catalog" => {
            let db = require_str(source.database.as_deref(), name, "database")?;
            let table = require_str(source.table.as_deref(), name, "table")?;
            lines.push(format!(
                "    {var} = glueContext.create_dynamic_frame.from_catalog(database=\"{db}\", table_name=\"{table}\", transformation_ctx=\"{ctx}\").toDF()"
            ));
        }
        "kafka" => {
            let servers = require_str(source.connection_url.as_deref(), name, "connection_url")?;
            let topic = require_str(source.topic.as_deref(), name, "topic")?;
            let mut chain = "spark.read.format(\"kafka\")".to_string();
            append_spark_options(
                &mut chain,
                &[("kafka.bootstrap.servers", servers), ("subscribe", topic)],
                &source.options,
            );
            chain.push_str(".load()");
            lines.push(format!("    {var} = {chain}"));
        }
        "api" => {
            let url = require_str(source.url.as_deref(), name, "url")?;
            let headers_obj: serde_json::Map<String, serde_json::Value> = source
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            let headers_lit = python_literal(&serde_json::Value::Object(headers_obj));
            let resp_var = format!("_resp_{name}");
            lines.push(format!(
                "    {resp_var} = requests.get(\"{url}\", headers={headers_lit})"
            ));
            lines.push(format!("    {resp_var}.raise_for_status()"));
            lines.push(format!("    {var} = spark.createDataFrame({resp_var}.json())"));
        }
        other => {
            lines.push(format!("    # Unsupported source type: {other}"));
        }
    }

    Ok(lines.join("\n"))
}

fn render_sources(sources: &[Source], default_engine: &str) -> Result<String> {
    let rendered: Vec<String> = sources
        .iter()
        .map(|s| render_source(s, default_engine))
        .collect::<Result<Vec<_>>>()?;
    Ok(rendered.join("\n"))
}

// --- Transform rendering (now with named dataframes) ---

fn resolve_df(transform: &Transform, default_source: &str) -> (String, String) {
    let input = transform.source.as_deref().unwrap_or(default_source);
    let output = transform.output.as_deref().unwrap_or(input);
    (format!("df_{input}"), format!("df_{output}"))
}

fn render_transform(
    transform: &Transform,
    default_source: &str,
    all_source_names: &[String],
) -> Result<String> {
    match transform.transform_type.as_str() {
        "filter" => {
            let (input, output) = resolve_df(transform, default_source);
            let condition = transform.condition.as_deref().unwrap_or("True").trim();
            Ok(format!("    {output} = {input}.filter({condition})"))
        }
        "sql" => {
            let output_name = transform.output.as_deref().unwrap_or(default_source);
            let output_var = format!("df_{output_name}");
            let query = transform
                .query
                .as_deref()
                .unwrap_or("SELECT * FROM source")
                .trim();
            let mut lines = Vec::new();
            // Register all named sources as temp views
            for name in all_source_names {
                lines.push(format!("    df_{name}.createOrReplaceTempView(\"{name}\")"));
            }
            lines.push(format!("    {output_var} = spark.sql(\"{query}\")"));
            Ok(lines.join("\n"))
        }
        "join" => {
            let left = transform.left.as_deref().unwrap_or(default_source);
            let right = transform
                .right
                .as_deref()
                .ok_or_else(|| anyhow!("join transform: 'right' is required"))?;
            let on_col = transform
                .on
                .as_deref()
                .ok_or_else(|| anyhow!("join transform: 'on' is required"))?;
            let how = transform.how.as_deref().unwrap_or("inner");
            let output_name = transform.output.as_deref().unwrap_or(left);
            let output_var = format!("df_{output_name}");
            Ok(format!("    {output_var} = df_{left}.join(df_{right}, on=\"{on_col}\", how=\"{how}\")"))
        }
        "drop_columns" => {
            let (input, output) = resolve_df(transform, default_source);
            Ok(format!(
                "    {output} = {input}.drop({})",
                quoted_list(&transform.columns)
            ))
        }
        "select" => {
            let (input, output) = resolve_df(transform, default_source);
            Ok(format!(
                "    {output} = {input}.select({})",
                quoted_list(&transform.columns)
            ))
        }
        "rename" => {
            let (input, output) = resolve_df(transform, default_source);
            let mut lines: Vec<String> = Vec::new();
            let mut first = true;
            for (old, new) in &transform.mapping {
                if first {
                    lines.push(format!(
                        "    {output} = {input}.withColumnRenamed(\"{old}\", \"{new}\")"
                    ));
                    first = false;
                } else {
                    lines.push(format!(
                        "    {output} = {output}.withColumnRenamed(\"{old}\", \"{new}\")"
                    ));
                }
            }
            Ok(lines.join("\n"))
        }
        "add_column" => {
            let (input, output) = resolve_df(transform, default_source);
            let name = transform
                .name
                .as_deref()
                .ok_or_else(|| anyhow!("add_column transform: 'name' is required"))?
                .trim();
            let expr = transform
                .expression
                .as_deref()
                .unwrap_or("lit(None)")
                .trim();
            Ok(format!("    {output} = {input}.withColumn(\"{name}\", {expr})"))
        }
        "aggregate" => {
            let (input, output) = resolve_df(transform, default_source);
            let group_cols = quoted_list(&transform.group_by);
            let mut agg_entries: Vec<(&String, &String)> = transform.aggs.iter().collect();
            agg_entries.sort_by(|a, b| a.0.cmp(b.0));
            let agg_exprs = agg_entries
                .iter()
                .map(|(alias, expr)| format!("F.expr(\"{}\").alias(\"{}\")", expr.trim(), alias))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "    {output} = {input}.groupBy({group_cols}).agg({agg_exprs})"
            ))
        }
        "window" => {
            let (input, output) = resolve_df(transform, default_source);
            let col_name = transform
                .name
                .as_deref()
                .ok_or_else(|| anyhow!("window transform: 'name' is required"))?
                .trim();
            let expr = transform
                .expression
                .as_deref()
                .ok_or_else(|| anyhow!("window transform: 'expression' is required"))?
                .trim();
            let window_var = format!("_w_{col_name}");
            let mut spec = String::from("Window");
            if !transform.partition_by.is_empty() {
                spec.push_str(&format!(
                    ".partitionBy({})",
                    quoted_list(&transform.partition_by)
                ));
            }
            if !transform.order_by.is_empty() {
                let orders = transform
                    .order_by
                    .iter()
                    .map(|o| {
                        let dir = if o.desc { "desc" } else { "asc" };
                        format!("F.col(\"{}\").{dir}()", o.column)
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                spec.push_str(&format!(".orderBy({orders})"));
            }
            let line1 = format!("    {window_var} = {spec}");
            let line2 = format!(
                "    {output} = {input}.withColumn(\"{col_name}\", F.expr(\"{expr}\").over({window_var}))"
            );
            Ok(format!("{line1}\n{line2}"))
        }
        _ => Ok(format!(
            "    # Unsupported transform type: {}",
            transform.transform_type
        )),
    }
}

fn render_transforms(
    transforms: &[Transform],
    default_source: &str,
    all_source_names: &[String],
) -> Result<String> {
    let rendered: Vec<String> = transforms
        .iter()
        .map(|t| render_transform(t, default_source, all_source_names))
        .collect::<Result<Vec<_>>>()?;
    Ok(rendered.join("\n"))
}

// --- Sink rendering (now with named dataframe) ---

fn require_sink_str<'a>(value: Option<&'a str>, sink_type: &str, field: &str) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("sink: '{field}' is required for {sink_type} sink"))
}

fn quoted_list(cols: &[String]) -> String {
    cols.iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

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


def _yard_fill_nulls(df):
    for field in df.schema.fields:
        dt, name = field.dataType, field.name
        col = F.col(f"`{name}`")
        if "void" in dt.simpleString():
            df = df.withColumn(name, F.coalesce(col.cast("string"), F.lit("")))
        elif isinstance(dt, StructType):
            df = df.withColumn(name, F.when(col.isNull(), _yard_default_struct(dt)).otherwise(col))
        elif isinstance(dt, ArrayType):
            if isinstance(dt.elementType, StructType):
                inner = _yard_default_struct(dt.elementType)
                df = df.withColumn(name, F.when(col.isNull(), F.array().cast(dt))
                    .otherwise(F.transform(col, lambda x: F.when(x.isNull(), inner).otherwise(x))))
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

const ICEBERG_TABLE_PROPERTIES: &[(&str, &str)] = &[
    ("format-version", "2"),
    ("write.spark.accept-any-schema", "true"),
    ("write.target-file-size-bytes", "536870912"),
    ("write.parquet.compression-codec", "zstd"),
    ("write.distribution-mode", "hash"),
];

fn render_sink(sink: &Sink, default_source: &str) -> Result<String> {
    let source_name = sink.source.as_deref().unwrap_or(default_source);
    let var = format!("df_{source_name}");
    let mut lines = Vec::new();

    if let Some(secret_id) = &sink.secret_id {
        lines.push(render_secret_fetch(secret_id, "sink"));
    }

    let mode = sink.mode.as_deref().unwrap_or("overwrite");

    match sink.sink_type.as_str() {
        "s3" => {
            let format = sink.format.as_deref().unwrap_or("parquet");
            let path = require_sink_str(sink.path.as_deref(), "s3", "path")?;
            let mut write = format!("    {var}.write.format(\"{format}\").mode(\"{mode}\")");
            if !sink.partition_by.is_empty() {
                write.push_str(&format!(".partitionBy({})", quoted_list(&sink.partition_by)));
            }
            write.push_str(&format!(".save(\"{path}\")"));
            lines.push(write);
        }
        "jdbc" => {
            let url = require_sink_str(sink.connection_url.as_deref(), "jdbc", "connection_url")?;
            let table = require_sink_str(sink.table.as_deref(), "jdbc", "table")?;
            lines.push(format!(
                "    {var}.write.format(\"jdbc\").option(\"url\", \"{url}\").option(\"dbtable\", \"{table}\")\\"
            ));
            if sink.secret_id.is_some() {
                lines.push(
                    "        .option(\"user\", sink_secret[\"username\"]).option(\"password\", sink_secret[\"password\"])\\".to_string()
                );
            }
            lines.push(format!("        .mode(\"{mode}\").save()"));
        }
        "catalog" => {
            let db = require_sink_str(sink.database.as_deref(), "catalog", "database")?;
            let table = require_sink_str(sink.table.as_deref(), "catalog", "table")?;
            lines.push(format!(
                "    sink_frame = DynamicFrame.fromDF({var}, glueContext, \"sink_frame\")"
            ));
            lines.push(format!(
                "    glueContext.write_dynamic_frame.from_catalog(frame=sink_frame, database=\"{db}\", table_name=\"{table}\")"
            ));
        }
        "iceberg" => {
            let db = require_sink_str(sink.database.as_deref(), "iceberg", "database")?;
            let table = require_sink_str(sink.table.as_deref(), "iceberg", "table")?;
            // "overwrite" maps to dynamic partition overwrite; "append" is default.
            let write_op = match mode {
                "overwrite" => "overwritePartitions",
                _ => "append",
            };
            let partition_clause = if sink.partition_by.is_empty() {
                String::new()
            } else {
                format!(
                    "\n            .partitionedBy({})",
                    quoted_list(&sink.partition_by)
                )
            };
            let tbl_props = ICEBERG_TABLE_PROPERTIES
                .iter()
                .map(|(k, v)| format!("\n            .tableProperty(\"{k}\", \"{v}\")"))
                .chain(
                    sink.path
                        .as_deref()
                        .filter(|p| !p.is_empty())
                        .map(|p| format!("\n            .tableProperty(\"location\", \"{p}\")")),
                )
                .collect::<String>();
            lines.push(format!(
                "    _glue = boto3.client(\"glue\")\n    \
                 try:\n        \
                     _glue.get_database(Name=\"{db}\")\n    \
                 except _glue.exceptions.EntityNotFoundException:\n        \
                     _glue.create_database(DatabaseInput={{\"Name\": \"{db}\"}})"
            ));
            lines.push(format!("    _tbl = \"glue_catalog.{db}.{table}\""));
            lines.push(format!(
                "    if not spark.catalog.tableExists(_tbl):\n        \
                     ({var}.writeTo(_tbl)\n            \
                         .using(\"iceberg\"){partition_clause}{tbl_props}\n            \
                         .create())\n    \
                 else:\n        \
                     {var}.writeTo(_tbl).option(\"mergeSchema\", \"true\").{write_op}()"
            ));
        }
        other => {
            lines.push(format!("    # Unsupported sink type: {other}"));
        }
    }

    Ok(lines.join("\n"))
}

// --- Secrets Manager helper ---

fn render_secret_fetch(secret_id: &str, prefix: &str) -> String {
    let var = format!("{prefix}_secret");
    [
        format!("    {var}_client = boto3.client(\"secretsmanager\")"),
        format!("    {var}_resp = {var}_client.get_secret_value(SecretId=\"{secret_id}\")"),
        format!("    {var} = json.loads({var}_resp[\"SecretString\"])"),
    ]
    .join("\n")
}

fn needs_secrets_imports(job_def: &JobDefinition) -> bool {
    let source_has = job_def.sources.iter().any(|s| s.secret_id.is_some());
    let sink_has = job_def.sink.as_ref().is_some_and(|s| s.secret_id.is_some());
    source_has || sink_has
}

fn has_iceberg_sink(job_def: &JobDefinition) -> bool {
    job_def
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "iceberg")
}

/// True when the iceberg sink should be preceded by a `_yard_fill_nulls` pass.
/// Opt-in by default for iceberg sinks; `fill_nulls: false` opts out.
fn should_fill_nulls(job_def: &JobDefinition) -> bool {
    job_def
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "iceberg" && s.fill_nulls != Some(false))
}

fn render_partition_derivation(job_def: &JobDefinition, sink_source: &str) -> Option<String> {
    if job_def.partition_by.is_empty() {
        return None;
    }
    let var = format!("df_{sink_source}");
    let mut lines = Vec::new();
    lines.push("    # --- Partition columns ---".to_string());
    if job_def.create_timestamp {
        lines.push(format!(
            "    {var} = {var}.withColumn(\"ingestion_timestamp\", F.current_timestamp())"
        ));
        lines.push("    _ts = \"ingestion_timestamp\"".to_string());
    } else {
        let col = job_def
            .partition_timestamp_column
            .as_deref()
            .unwrap_or("event_time");
        lines.push(format!("    _ts = \"{col}\""));
    }
    for unit in &job_def.partition_by {
        let func = match unit.as_str() {
            "year" => "year",
            "month" => "month",
            "day" => "dayofmonth",
            _ => continue,
        };
        lines.push(format!(
            "    if \"{unit}\" not in {var}.columns:\n        \
             {var} = {var}.withColumn(\"{unit}\", F.{func}(F.col(_ts)))"
        ));
    }
    Some(lines.join("\n"))
}

fn needs_functions_import(job_def: &JobDefinition) -> bool {
    job_def
        .transforms
        .iter()
        .any(|t| matches!(t.transform_type.as_str(), "aggregate" | "window"))
}

fn needs_window_import(job_def: &JobDefinition) -> bool {
    job_def
        .transforms
        .iter()
        .any(|t| t.transform_type == "window")
}

fn needs_dynamic_frame_import(job_def: &JobDefinition, default_engine: &str) -> bool {
    job_def
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "catalog")
        || job_def.sources.iter().any(|s| {
            s.source_type == "catalog"
                || (matches!(s.source_type.as_str(), "s3" | "jdbc")
                    && effective_engine(s, default_engine) == "glue")
        })
}

fn needs_requests_import(job_def: &JobDefinition) -> bool {
    job_def.sources.iter().any(|s| s.source_type == "api")
}

fn default_engine_for(job_def: &JobDefinition) -> String {
    job_def
        .config
        .get(&job_def.job_type)
        .and_then(|g| g.get("default_engine"))
        .and_then(|v| v.as_str())
        .unwrap_or("spark")
        .to_string()
}

fn indent_body(body: &str) -> String {
    body.lines()
        .map(|line| {
            if line.trim().is_empty() {
                String::new()
            } else {
                format!("    {}", line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn generate_python_script(job_name: &str, job_def: &JobDefinition) -> Result<String> {
    // Task-only job types (bash, ...) don't produce a standalone PySpark script;
    // they participate in Airflow DAG codegen instead. Return an empty string so
    // callers that blindly hash/write the script output continue to work — the
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
             IntegerType, LongType, TimestampType, DateType, BooleanType)"
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

    fn s3_source(name: &str, path: &str) -> Source {
        Source {
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
        job.transforms = vec![Transform {
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
        job.transforms = vec![Transform {
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
        job.transforms = vec![Transform {
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
        job.transforms = vec![Transform {
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
        job.sink = Some(Sink {
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
        job.transforms = vec![Transform {
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
        job.sink = Some(Sink {
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

    fn iceberg_sink(database: &str, table: &str, path: Option<&str>) -> Sink {
        Sink {
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
        job.sources = vec![Source {
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
            Transform {
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
            Transform {
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
            Transform {
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
        job.sink = Some(Sink {
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
        job.sources = vec![Source {
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
        job.sources = vec![Source {
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
        job.sources = vec![Source {
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
        job.sources = vec![Source {
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
        job.sources = vec![Source {
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
        job.transforms = vec![Transform {
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
        job.sink = Some(Sink {
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
        job.sink = Some(Sink {
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
        job.sink = Some(Sink {
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
        job.sink = Some(Sink {
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
        job.sink = Some(Sink {
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
        job.transforms = vec![Transform {
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
        job.transforms = vec![Transform {
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
        job.transforms = vec![Transform {
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
        job.transforms = vec![Transform {
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
            Transform {
                transform_type: "aggregate".to_string(),
                output: Some("totals".to_string()),
                group_by: vec!["region".to_string()],
                aggs,
                ..Default::default()
            },
            Transform {
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
