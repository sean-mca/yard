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

fn render_source(source: &Source) -> Result<String> {
    let var = format!("df_{}", source.name);
    let mut lines = Vec::new();

    if let Some(secret_id) = &source.secret_id {
        lines.push(render_secret_fetch(
            secret_id,
            &format!("{}_source", source.name),
        ));
    }

    let secret_var = format!("{}_source_secret", source.name);

    match source.source_type.as_str() {
        "s3" => {
            let format = source.format.as_deref().unwrap_or("parquet");
            let path = source
                .path
                .as_deref()
                .ok_or_else(|| anyhow!("source '{}': 'path' is required for s3 source", source.name))?;
            lines.push(format!(
                "    {var} = spark.read.format(\"{format}\").load(\"{path}\")"
            ));
        }
        "jdbc" => {
            let url = source
                .connection_url
                .as_deref()
                .ok_or_else(|| anyhow!("source '{}': 'connection_url' is required for jdbc source", source.name))?;
            let table = source
                .table
                .as_deref()
                .ok_or_else(|| anyhow!("source '{}': 'table' is required for jdbc source", source.name))?;
            lines.push(format!(
                "    {var} = spark.read.format(\"jdbc\").option(\"url\", \"{url}\").option(\"dbtable\", \"{table}\")\\"
            ));
            if source.secret_id.is_some() {
                lines.push(format!(
                    "        .option(\"user\", {secret_var}[\"username\"]).option(\"password\", {secret_var}[\"password\"])\\"
                ));
            }
            lines.push("        .load()".to_string());
        }
        "catalog" => {
            let db = source
                .database
                .as_deref()
                .ok_or_else(|| anyhow!("source '{}': 'database' is required for catalog source", source.name))?;
            let table = source
                .table
                .as_deref()
                .ok_or_else(|| anyhow!("source '{}': 'table' is required for catalog source", source.name))?;
            lines.push(format!(
                "    {var} = glueContext.create_dynamic_frame.from_catalog(database=\"{db}\", table_name=\"{table}\").toDF()"
            ));
        }
        _ => {
            lines.push(format!(
                "    # Unsupported source type: {}",
                source.source_type
            ));
        }
    }

    Ok(lines.join("\n"))
}

fn render_sources(sources: &[Source]) -> Result<String> {
    let rendered: Vec<String> = sources
        .iter()
        .map(render_source)
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
            let cols = transform
                .columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!("    {output} = {input}.drop({cols})"))
        }
        "select" => {
            let (input, output) = resolve_df(transform, default_source);
            let cols = transform
                .columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!("    {output} = {input}.select({cols})"))
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
            let group_cols = transform
                .group_by
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
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
                let parts = transform
                    .partition_by
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                spec.push_str(&format!(".partitionBy({parts})"));
            }
            if !transform.order_by.is_empty() {
                let orders = transform
                    .order_by
                    .iter()
                    .map(|o| {
                        if o.desc {
                            format!("F.col(\"{}\").desc()", o.column)
                        } else {
                            format!("F.col(\"{}\").asc()", o.column)
                        }
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
            let path = sink
                .path
                .as_deref()
                .ok_or_else(|| anyhow!("sink: 'path' is required for s3 sink"))?;
            let mut write = format!("    {var}.write.format(\"{format}\").mode(\"{mode}\")");
            if !sink.partition_by.is_empty() {
                let parts = sink
                    .partition_by
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write.push_str(&format!(".partitionBy({parts})"));
            }
            write.push_str(&format!(".save(\"{path}\")"));
            lines.push(write);
        }
        "jdbc" => {
            let url = sink
                .connection_url
                .as_deref()
                .ok_or_else(|| anyhow!("sink: 'connection_url' is required for jdbc sink"))?;
            let table = sink
                .table
                .as_deref()
                .ok_or_else(|| anyhow!("sink: 'table' is required for jdbc sink"))?;
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
            let db = sink
                .database
                .as_deref()
                .ok_or_else(|| anyhow!("sink: 'database' is required for catalog sink"))?;
            let table = sink
                .table
                .as_deref()
                .ok_or_else(|| anyhow!("sink: 'table' is required for catalog sink"))?;
            lines.push(format!(
                "    sink_frame = DynamicFrame.fromDF({var}, glueContext, \"sink_frame\")"
            ));
            lines.push(format!(
                "    glueContext.write_dynamic_frame.from_catalog(frame=sink_frame, database=\"{db}\", table_name=\"{table}\")"
            ));
        }
        _ => {
            lines.push(format!("    # Unsupported sink type: {}", sink.sink_type));
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

fn needs_dynamic_frame_import(job_def: &JobDefinition) -> bool {
    job_def
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "catalog")
        || job_def.sources.iter().any(|s| s.source_type == "catalog")
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

    // Build extra imports needed by template features
    let mut extra_imports = Vec::new();
    if needs_secrets_imports(job_def) {
        extra_imports.push("import boto3".to_string());
        extra_imports.push("import json".to_string());
    }
    if needs_dynamic_frame_import(job_def) {
        extra_imports.push("from awsglue.dynamicframe import DynamicFrame".to_string());
    }
    if needs_functions_import(job_def) {
        extra_imports.push("from pyspark.sql import functions as F".to_string());
    }
    if needs_window_import(job_def) {
        extra_imports.push("from pyspark.sql.window import Window".to_string());
    }

    let user_imports = render_imports(&job_def.imports);
    let all_imports = if extra_imports.is_empty() {
        user_imports
    } else if user_imports.is_empty() {
        extra_imports.join("\n")
    } else {
        format!("{}\n{}", user_imports, extra_imports.join("\n"))
    };

    // Build the run() body
    let run_body = if let Some(body) = &job_def.body {
        indent_body(body)
    } else {
        let mut parts = Vec::new();
        if !job_def.sources.is_empty() {
            parts.push(format!(
                "    # --- Sources ---\n{}",
                render_sources(&job_def.sources)?
            ));
        }
        if !job_def.transforms.is_empty() {
            parts.push(format!(
                "    # --- Transforms ---\n{}",
                render_transforms(&job_def.transforms, default_source, &all_source_names)?
            ));
        }
        if let Some(sink) = &job_def.sink {
            parts.push(format!(
                "    # --- Sink ---\n{}",
                render_sink(sink, default_source)?
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
            imports: vec![],
            body: None,
            job_file: None,
            sources: vec![],
            sink: None,
            transforms: vec![],
            config: json!({"type": "glue"}),
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
        assert!(script.contains("SparkContext()"));
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
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df_enriched.write.format(\"parquet\")"));
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
