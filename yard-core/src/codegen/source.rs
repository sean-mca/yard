use anyhow::Result;
use yard_structs::Source;

use super::helpers::{
    append_spark_options, build_options_dict, effective_engine,
    python_literal, render_jdbc_auth, render_secret_fetch, require_str,
};

/// Render a `glueContext.create_dynamic_frame.from_options(...).toDF()` call.
/// `options_expr` may be a literal dict or a variable name.
pub(super) fn glue_from_options(
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

pub(super) fn render_source(source: &Source, default_engine: &str) -> Result<String> {
    let name = &source.name;
    let var = format!("df_{name}");
    let ctx = format!("{name}_ctx");
    let engine = effective_engine(source, default_engine);
    let auth_prefix = format!("{name}_source");
    let mut lines = Vec::new();

    if let Some(secret_id) = &source.secret_id {
        lines.push(render_secret_fetch(secret_id, &auth_prefix));
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
            let auth = render_jdbc_auth(&auth_prefix, source.secret_id.as_deref(), source.auth.as_ref());
            if let Some((_, _, ref pre)) = auth {
                lines.extend(pre.iter().cloned());
            }
            if engine == "glue" {
                let connection_type = source.connection_type.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
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
                let options_expr = if let Some((user_expr, password_expr, _)) = &auth {
                    let opts_var = format!("_opts_{name}");
                    lines.push(format!(
                        "    {opts_var} = {{**{base_opts}, \"user\": {user_expr}, \"password\": {password_expr}}}"
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
                if let Some((user_expr, password_expr, _)) = &auth {
                    chain.push_str(&format!(
                        ".option(\"user\", {user_expr}).option(\"password\", {password_expr})"
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

pub(super) fn render_sources(sources: &[Source], default_engine: &str) -> Result<String> {
    let rendered: Vec<String> = sources
        .iter()
        .map(|s| render_source(s, default_engine))
        .collect::<Result<Vec<_>>>()?;
    Ok(rendered.join("\n"))
}
