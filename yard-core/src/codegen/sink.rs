use anyhow::{Result, anyhow};
use yard_structs::Sink;

use super::helpers::{quoted_list, render_jdbc_auth, render_secret_fetch};

pub(super) fn require_sink_str<'a>(value: Option<&'a str>, sink_type: &str, field: &str) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("sink: '{field}' is required for {sink_type} sink"))
}

pub(super) const ICEBERG_TABLE_PROPERTIES: &[(&str, &str)] = &[
    ("format-version", "2"),
    ("write.spark.accept-any-schema", "true"),
    ("write.target-file-size-bytes", "536870912"),
    ("write.parquet.compression-codec", "zstd"),
    ("write.distribution-mode", "hash"),
];

pub(super) fn render_sink(sink: &Sink, default_source: &str, fill_nulls: bool) -> Result<String> {
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
            let auth = render_jdbc_auth("sink", sink.secret_id.as_deref(), sink.auth.as_ref());
            if let Some((_, _, ref pre)) = auth {
                lines.extend(pre.iter().cloned());
            }
            lines.push(format!(
                "    {var}.write.format(\"jdbc\").option(\"url\", \"{url}\").option(\"dbtable\", \"{table}\")\\"
            ));
            if let Some((user_expr, password_expr, _)) = &auth {
                lines.push(format!(
                    "        .option(\"user\", {user_expr}).option(\"password\", {password_expr})\\"
                ));
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
            // New-table branch: dataframe-inferred fill (D-04). No live target
            // schema to consult; _yard_fill_nulls coerces voids to source-side
            // defaults so parquet accepts the schema at .create() time.
            //
            // Existing-table branch: schema-aware conform. _yard_conform_to_target_schema
            // reads the live Iceberg schema and drives _yard_void_to_target
            // element-wise for void/void-subtype source columns — uses the
            // target column type rather than dataframe-inferred type, preserves
            // non-void cells verbatim, and preserves nulls in nullable real-typed
            // columns (no 0/False/"" fills against existing tables).
            let new_table_coerce = if fill_nulls {
                format!("{var} = _yard_fill_nulls({var})\n        ")
            } else {
                String::new()
            };
            let existing_table_coerce = if fill_nulls {
                format!("{var} = _yard_conform_to_target_schema({var}, spark, _tbl)\n        ")
            } else {
                String::new()
            };
            lines.push(format!(
                "    if not spark.catalog.tableExists(_tbl):\n        \
                     {new_table_coerce}({var}.writeTo(_tbl)\n            \
                         .using(\"iceberg\"){partition_clause}{tbl_props}\n            \
                         .create())\n    \
                 else:\n        \
                     {existing_table_coerce}{var}.writeTo(_tbl).option(\"mergeSchema\", \"true\").{write_op}()"
            ));
        }
        other => {
            lines.push(format!("    # Unsupported sink type: {other}"));
        }
    }

    Ok(lines.join("\n"))
}
