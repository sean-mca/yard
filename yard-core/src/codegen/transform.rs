//! Transform codegen: renders PySpark transform operations (filter, SQL,
//! join, drop_columns, select, rename, add_column, aggregate, window).

use std::fmt::Write;

use anyhow::{Result, anyhow};
use yard_structs::Transform;

use super::helpers::quoted_list;

/// Resolve the input and output dataframe variable names for a transform.
///
/// Falls back to `default_source` when the transform does not specify
/// explicit `source` / `output` fields.
#[inline]
pub(super) fn resolve_df(transform: &Transform, default_source: &str) -> (String, String) {
    let input = transform.source.as_deref().unwrap_or(default_source);
    let output = transform.output.as_deref().unwrap_or(input);
    (format!("df_{input}"), format!("df_{output}"))
}

/// Render a single transform step as a Python statement.
///
/// Dispatches by `transform.transform_type` (filter, sql, join,
/// drop_columns, select, rename, add_column, aggregate, window).
///
/// # Errors
///
/// Returns an error when required fields are missing for the given
/// transform type.
pub(super) fn render_transform(
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
                write!(spec, ".partitionBy({})", quoted_list(&transform.partition_by))
                    .expect("write to String is infallible");
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
                write!(spec, ".orderBy({orders})").expect("write to String is infallible");
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

/// Render all transforms as a newline-joined block of Python statements.
///
/// # Errors
///
/// Returns an error if any individual transform is missing required
/// fields.
pub(super) fn render_transforms(
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
