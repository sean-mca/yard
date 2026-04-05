use anyhow::{Context as AnyhowContext, Result, anyhow};
use tera::{Context, Tera};
use yard_structs::{Import, JobDefinition, Sink, Source, Transform};

const GLUE_TEMPLATE: &str = include_str!("templates/glue.py.tera");

// --- Import rendering ---

fn render_import(import: &Import) -> String {
    match &import.from {
        Some(module) => format!("from {} import {}", module, import.name),
        None => format!("import {}", import.name),
    }
}

fn render_imports(imports: &[Import]) -> String {
    imports.iter().map(render_import).collect::<Vec<_>>().join("\n")
}

// --- Source rendering ---

fn render_source(source: &Source) -> String {
    let mut lines = Vec::new();

    // Secret fetch if needed
    if let Some(secret_id) = &source.secret_id {
        lines.push(render_secret_fetch(secret_id, "source"));
    }

    match source.source_type.as_str() {
        "s3" => {
            let format = source.format.as_deref().unwrap_or("parquet");
            let path = source.path.as_deref().unwrap_or("s3://MISSING_PATH");
            lines.push(format!(
                "    df = spark.read.format(\"{format}\").load(\"{path}\")"
            ));
        }
        "jdbc" => {
            let url = source.connection_url.as_deref().unwrap_or("MISSING_URL");
            let table = source.table.as_deref().unwrap_or("MISSING_TABLE");
            lines.push(format!("    df = spark.read.format(\"jdbc\").option(\"url\", \"{url}\").option(\"dbtable\", \"{table}\")\\"));
            if source.secret_id.is_some() {
                lines.push("        .option(\"user\", source_secret[\"username\"]).option(\"password\", source_secret[\"password\"])\\".to_string());
            }
            lines.push("        .load()".to_string());
        }
        "catalog" => {
            let db = source.database.as_deref().unwrap_or("MISSING_DATABASE");
            let table = source.table.as_deref().unwrap_or("MISSING_TABLE");
            lines.push(format!(
                "    df = glueContext.create_dynamic_frame.from_catalog(database=\"{db}\", table_name=\"{table}\").toDF()"
            ));
        }
        _ => {
            lines.push(format!("    # Unsupported source type: {}", source.source_type));
        }
    }

    lines.join("\n")
}

// --- Transform rendering ---

fn render_transform(transform: &Transform) -> String {
    match transform.transform_type.as_str() {
        "filter" => {
            let condition = transform.condition.as_deref().unwrap_or("True");
            format!("    df = df.filter({condition})")
        }
        "sql" => {
            let query = transform.query.as_deref().unwrap_or("SELECT * FROM source");
            let mut lines = Vec::new();
            lines.push("    df.createOrReplaceTempView(\"source\")".to_string());
            lines.push(format!("    df = spark.sql(\"{query}\")"));
            lines.join("\n")
        }
        "drop_columns" => {
            let cols = transform.columns.iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            format!("    df = df.drop({cols})")
        }
        "select" => {
            let cols = transform.columns.iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            format!("    df = df.select({cols})")
        }
        "rename" => {
            let lines: Vec<String> = transform.mapping.iter()
                .map(|(old, new)| format!("    df = df.withColumnRenamed(\"{old}\", \"{new}\")"))
                .collect();
            lines.join("\n")
        }
        "add_column" => {
            let name = transform.name.as_deref().unwrap_or("MISSING_NAME");
            let expr = transform.expression.as_deref().unwrap_or("lit(None)");
            format!("    df = df.withColumn(\"{name}\", {expr})")
        }
        _ => {
            format!("    # Unsupported transform type: {}", transform.transform_type)
        }
    }
}

fn render_transforms(transforms: &[Transform]) -> String {
    transforms.iter().map(render_transform).collect::<Vec<_>>().join("\n")
}

// --- Sink rendering ---

fn render_sink(sink: &Sink) -> String {
    let mut lines = Vec::new();

    if let Some(secret_id) = &sink.secret_id {
        lines.push(render_secret_fetch(secret_id, "sink"));
    }

    let mode = sink.mode.as_deref().unwrap_or("overwrite");

    match sink.sink_type.as_str() {
        "s3" => {
            let format = sink.format.as_deref().unwrap_or("parquet");
            let path = sink.path.as_deref().unwrap_or("s3://MISSING_PATH");
            let mut write = format!("    df.write.format(\"{format}\").mode(\"{mode}\")");
            if !sink.partition_by.is_empty() {
                let parts = sink.partition_by.iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");
                write.push_str(&format!(".partitionBy({parts})"));
            }
            write.push_str(&format!(".save(\"{path}\")"));
            lines.push(write);
        }
        "jdbc" => {
            let url = sink.connection_url.as_deref().unwrap_or("MISSING_URL");
            let table = sink.table.as_deref().unwrap_or("MISSING_TABLE");
            lines.push(format!("    df.write.format(\"jdbc\").option(\"url\", \"{url}\").option(\"dbtable\", \"{table}\")\\"));
            if sink.secret_id.is_some() {
                lines.push("        .option(\"user\", sink_secret[\"username\"]).option(\"password\", sink_secret[\"password\"])\\".to_string());
            }
            lines.push(format!("        .mode(\"{mode}\").save()"));
        }
        "catalog" => {
            let db = sink.database.as_deref().unwrap_or("MISSING_DATABASE");
            let table = sink.table.as_deref().unwrap_or("MISSING_TABLE");
            lines.push(format!(
                "    sink_frame = DynamicFrame.fromDF(df, glueContext, \"sink_frame\")"
            ));
            lines.push(format!(
                "    glueContext.write_dynamic_frame.from_catalog(frame=sink_frame, database=\"{db}\", table_name=\"{table}\")"
            ));
        }
        _ => {
            lines.push(format!("    # Unsupported sink type: {}", sink.sink_type));
        }
    }

    lines.join("\n")
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

/// Check if any source or sink uses secrets, requiring extra imports.
fn needs_secrets_imports(job_def: &JobDefinition) -> bool {
    let source_has = job_def.source.as_ref().is_some_and(|s| s.secret_id.is_some());
    let sink_has = job_def.sink.as_ref().is_some_and(|s| s.secret_id.is_some());
    source_has || sink_has
}

/// Check if any sink uses catalog type, requiring DynamicFrame import.
fn needs_dynamic_frame_import(job_def: &JobDefinition) -> bool {
    job_def.sink.as_ref().is_some_and(|s| s.sink_type == "catalog")
        || job_def.source.as_ref().is_some_and(|s| s.source_type == "catalog")
}

/// Indent a body string so it sits correctly inside `def run():`.
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
    let template = match job_def.job_type.as_str() {
        "glue" => GLUE_TEMPLATE,
        other => return Err(anyhow!("Unsupported job type: {}", other)),
    };

    let mut tera = Tera::default();
    tera.add_raw_template("script", template)?;

    // Build extra imports needed by template features
    let mut extra_imports = Vec::new();
    if needs_secrets_imports(job_def) {
        extra_imports.push("import boto3".to_string());
        extra_imports.push("import json".to_string());
    }
    if needs_dynamic_frame_import(job_def) {
        extra_imports.push("from awsglue.dynamicframe import DynamicFrame".to_string());
    }

    // Combine user imports with auto imports
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
        if let Some(source) = &job_def.source {
            parts.push(render_source(source));
        }
        if !job_def.transforms.is_empty() {
            parts.push(render_transforms(&job_def.transforms));
        }
        if let Some(sink) = &job_def.sink {
            parts.push(render_sink(sink));
        }
        if parts.is_empty() {
            "    pass".to_string()
        } else {
            parts.join("\n")
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
    use yard_structs::{Sink, Source, Transform};
    use std::collections::HashMap;

    fn base_job() -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
            imports: vec![],
            body: None,
            source: None,
            sink: None,
            transforms: vec![],
            config: json!({"type": "glue"}),
        }
    }

    // --- Routing ---

    #[test]
    fn unsupported_job_type_errors() {
        let mut job = base_job();
        job.job_type = "unknown".to_string();
        let result = generate_python_script("test", &job);
        assert!(result.is_err());
    }

    // --- Header & template basics ---

    #[test]
    fn generates_header() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("Generated by YARD for job: test_job"));
    }

    #[test]
    fn glue_setup() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("import sys"));
        assert!(script.contains("from awsglue.utils import getResolvedOptions"));
        assert!(script.contains("from pyspark.context import SparkContext"));
        assert!(script.contains("from awsglue.context import GlueContext"));
        assert!(script.contains("from awsglue.job import Job"));
        assert!(script.contains("SparkContext()"));
        assert!(script.contains("job.init"));
    }

    #[test]
    fn glue_teardown() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("job.commit()"));
    }

    #[test]
    fn default_body_is_pass() {
        let script = generate_python_script("test_job", &base_job()).unwrap();
        assert!(script.contains("    pass"));
    }

    // --- User imports ---

    #[test]
    fn user_imports() {
        let mut job = base_job();
        job.imports = vec![
            Import { name: "logging".to_string(), from: None },
        ];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("import logging"));
    }

    // --- Body override ---

    #[test]
    fn body_override_skips_source_sink() {
        let mut job = base_job();
        job.body = Some("print('custom')".to_string());
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://bucket/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("    print('custom')"));
        assert!(!script.contains("spark.read"));
    }

    // --- S3 source ---

    #[test]
    fn s3_source() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://my-bucket/raw/".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("spark.read.format(\"parquet\").load(\"s3://my-bucket/raw/\")"));
    }

    // --- JDBC source with secret ---

    #[test]
    fn jdbc_source_with_secret() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: Some("jdbc:postgresql://host:5432/db".to_string()),
            table: Some("public.users".to_string()),
            database: None,
            secret_id: Some("my-rds-secret".to_string()),
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("import boto3"));
        assert!(script.contains("import json"));
        assert!(script.contains("get_secret_value(SecretId=\"my-rds-secret\")"));
        assert!(script.contains("source_secret[\"username\"]"));
        assert!(script.contains("jdbc:postgresql://host:5432/db"));
    }

    // --- Catalog source ---

    #[test]
    fn catalog_source() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "catalog".to_string(),
            format: None, path: None, connection_url: None,
            table: Some("raw_events".to_string()),
            database: Some("my_db".to_string()),
            secret_id: None,
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("from_catalog(database=\"my_db\", table_name=\"raw_events\")"));
    }

    // --- Transforms ---

    #[test]
    fn filter_transform() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![Transform {
            transform_type: "filter".to_string(),
            condition: Some("col('status') == 'active'".to_string()),
            query: None, columns: vec![], mapping: HashMap::new(),
            name: None, expression: None,
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df = df.filter(col('status') == 'active')"));
    }

    #[test]
    fn sql_transform() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("csv".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![Transform {
            transform_type: "sql".to_string(),
            condition: None,
            query: Some("SELECT id, name FROM source WHERE amount > 100".to_string()),
            columns: vec![], mapping: HashMap::new(),
            name: None, expression: None,
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("createOrReplaceTempView(\"source\")"));
        assert!(script.contains("spark.sql(\"SELECT id, name FROM source WHERE amount > 100\")"));
    }

    #[test]
    fn drop_columns_transform() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![Transform {
            transform_type: "drop_columns".to_string(),
            condition: None, query: None,
            columns: vec!["temp_col".to_string(), "debug_flag".to_string()],
            mapping: HashMap::new(), name: None, expression: None,
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df = df.drop(\"temp_col\", \"debug_flag\")"));
    }

    #[test]
    fn rename_transform() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![Transform {
            transform_type: "rename".to_string(),
            condition: None, query: None, columns: vec![],
            mapping: HashMap::from([("old_name".to_string(), "new_name".to_string())]),
            name: None, expression: None,
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("withColumnRenamed(\"old_name\", \"new_name\")"));
    }

    #[test]
    fn select_transform() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![Transform {
            transform_type: "select".to_string(),
            condition: None, query: None,
            columns: vec!["id".to_string(), "name".to_string()],
            mapping: HashMap::new(), name: None, expression: None,
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df = df.select(\"id\", \"name\")"));
    }

    #[test]
    fn add_column_transform() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![Transform {
            transform_type: "add_column".to_string(),
            condition: None, query: None, columns: vec![], mapping: HashMap::new(),
            name: Some("year".to_string()),
            expression: Some("year(col('created_at'))".to_string()),
        }];
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df = df.withColumn(\"year\", year(col('created_at')))"));
    }

    // --- S3 sink ---

    #[test]
    fn s3_sink() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.sink = Some(Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out/".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
            mode: Some("overwrite".to_string()),
            partition_by: vec![],
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("df.write.format(\"parquet\").mode(\"overwrite\").save(\"s3://b/out/\")"));
    }

    #[test]
    fn s3_sink_with_partitions() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.sink = Some(Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/out/".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
            mode: Some("overwrite".to_string()),
            partition_by: vec!["year".to_string(), "month".to_string()],
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains(".partitionBy(\"year\", \"month\")"));
    }

    // --- JDBC sink with secret ---

    #[test]
    fn jdbc_sink_with_secret() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://b/in".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.sink = Some(Sink {
            sink_type: "jdbc".to_string(),
            format: None,
            path: None,
            connection_url: Some("jdbc:postgresql://host:5432/db".to_string()),
            table: Some("public.output".to_string()),
            database: None,
            secret_id: Some("my-sink-secret".to_string()),
            mode: Some("append".to_string()),
            partition_by: vec![],
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("get_secret_value(SecretId=\"my-sink-secret\")"));
        assert!(script.contains("sink_secret[\"username\"]"));
        assert!(script.contains(".mode(\"append\").save()"));
    }

    // --- Full pipeline ---

    #[test]
    fn full_pipeline_s3_to_s3() {
        let mut job = base_job();
        job.source = Some(Source {
            source_type: "s3".to_string(),
            format: Some("csv".to_string()),
            path: Some("s3://raw/events/".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
        });
        job.transforms = vec![
            Transform {
                transform_type: "filter".to_string(),
                condition: Some("col('status') == 'active'".to_string()),
                query: None, columns: vec![], mapping: HashMap::new(),
                name: None, expression: None,
            },
            Transform {
                transform_type: "drop_columns".to_string(),
                condition: None, query: None,
                columns: vec!["debug".to_string()],
                mapping: HashMap::new(), name: None, expression: None,
            },
        ];
        job.sink = Some(Sink {
            sink_type: "s3".to_string(),
            format: Some("parquet".to_string()),
            path: Some("s3://curated/events/".to_string()),
            connection_url: None, table: None, database: None, secret_id: None,
            mode: Some("overwrite".to_string()),
            partition_by: vec![],
        });
        let script = generate_python_script("test_job", &job).unwrap();
        assert!(script.contains("spark.read.format(\"csv\").load(\"s3://raw/events/\")"));
        assert!(script.contains("df = df.filter("));
        assert!(script.contains("df = df.drop("));
        assert!(script.contains("df.write.format(\"parquet\")"));
        assert!(!script.contains("    pass"));
    }

    #[test]
    fn different_jobs_produce_different_scripts() {
        let a = generate_python_script("job_a", &base_job()).unwrap();
        let b = generate_python_script("job_b", &base_job()).unwrap();
        assert_ne!(a, b);
    }
}
