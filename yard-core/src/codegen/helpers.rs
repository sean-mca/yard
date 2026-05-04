use anyhow::{Result, anyhow};
use yard_structs::{Import, JdbcAuth, JobDefinition, RdsIamAuth, Source};

// --- Import rendering ---

pub(super) fn render_import(import: &Import) -> String {
    match &import.from {
        Some(module) => format!("from {} import {}", module, import.name),
        None => format!("import {}", import.name),
    }
}

pub(super) fn render_imports(imports: &[Import]) -> String {
    imports
        .iter()
        .map(render_import)
        .collect::<Vec<_>>()
        .join("\n")
}

// --- Source rendering helpers ---

/// Render a serde_json::Value as a Python literal. Strings, numbers, bools,
/// and null map directly; arrays and objects recurse. Used for opaque
/// `options:` passthrough.
pub(super) fn python_literal(v: &serde_json::Value) -> String {
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

pub(super) fn effective_engine(source: &Source, default_engine: &str) -> String {
    source
        .engine
        .clone()
        .unwrap_or_else(|| default_engine.to_string())
}

pub(super) fn require_str<'a>(value: Option<&'a str>, source_name: &str, field: &str) -> Result<&'a str> {
    value.ok_or_else(|| anyhow!("source '{source_name}': '{field}' is required"))
}

/// Build a Python dict literal from a seed of ordered (key, value) pairs,
/// merging in arbitrary user-supplied options afterward.
pub(super) fn build_options_dict(
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
pub(super) fn append_spark_options(
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

// --- Sink helpers ---

pub(super) fn quoted_list(cols: &[String]) -> String {
    cols.iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ")
}

// --- Secrets Manager helper ---

pub(super) fn render_secret_fetch(secret_id: &str, prefix: &str) -> String {
    let var = format!("{prefix}_secret");
    [
        format!("    {var}_client = boto3.client(\"secretsmanager\")"),
        format!("    {var}_resp = {var}_client.get_secret_value(SecretId=\"{secret_id}\")"),
        format!("    {var} = json.loads({var}_resp[\"SecretString\"])"),
    ]
    .join("\n")
}

pub(super) fn needs_secrets_imports(job_def: &JobDefinition) -> bool {
    let source_has = job_def.sources.iter().any(|s| s.secret_id.is_some());
    let sink_has = job_def.sink.as_ref().is_some_and(|s| s.secret_id.is_some());
    source_has || sink_has
}

// --- JDBC auth (RDS IAM) ---

pub(super) fn needs_jdbc_auth_imports(job_def: &JobDefinition) -> bool {
    let source_has = job_def.sources.iter().any(|s| s.auth.is_some());
    let sink_has = job_def.sink.as_ref().is_some_and(|s| s.auth.is_some());
    source_has || sink_has
}

/// Resolve the JDBC user / password expressions for a jdbc source/sink, given
/// its `secret_id` and `auth`. Returns `(user_expr, password_expr,
/// pre_lines)` — `pre_lines` are Python statements emitted before the
/// reader/writer call, `user_expr` and `password_expr` are inlined into
/// `.option("user", …)` / `.option("password", …)`. Returns `None` if no
/// auth options should be emitted (neither secret_id nor auth set).
///
/// Validation enforces that `auth.username` and `secret_id` cannot both be
/// set, and that one of them must supply the username — so codegen here
/// can assume well-formed input.
pub(super) fn render_jdbc_auth(
    prefix: &str,
    secret_id: Option<&str>,
    auth: Option<&JdbcAuth>,
) -> Option<(String, String, Vec<String>)> {
    let secret_var = format!("{prefix}_secret");
    match (secret_id, auth) {
        (None, None) => None,
        (Some(_), None) => Some((
            format!("{secret_var}[\"username\"]"),
            format!("{secret_var}[\"password\"]"),
            Vec::new(),
        )),
        (secret, Some(JdbcAuth::RdsIam(rds))) => {
            let user_expr = if secret.is_some() {
                format!("{secret_var}[\"username\"]")
            } else {
                // Validation guarantees username is Some when secret_id is None.
                let u = rds.username.as_deref().unwrap_or("");
                format!("\"{u}\"")
            };
            let token_var = format!("{prefix}_token");
            let pre = render_rds_iam_token_fetch(prefix, rds, &user_expr);
            Some((user_expr, token_var, pre))
        }
    }
}

fn render_rds_iam_token_fetch(prefix: &str, rds: &RdsIamAuth, user_expr: &str) -> Vec<String> {
    let RdsIamAuth { host, port, region, .. } = rds;
    let client_var = format!("_{prefix}_rds");
    let token_var = format!("{prefix}_token");
    vec![
        format!("    {client_var} = boto3.client(\"rds\", region_name=\"{region}\")"),
        format!("    {token_var} = {client_var}.generate_db_auth_token("),
        format!("        DBHostname=\"{host}\","),
        format!("        Port={port},"),
        format!("        DBUsername={user_expr},"),
        format!("        Region=\"{region}\","),
        "    )".to_string(),
    ]
}

pub(super) fn has_iceberg_sink(job_def: &JobDefinition) -> bool {
    job_def
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "iceberg")
}

/// True when the iceberg sink should be preceded by a `_yard_fill_nulls` pass.
/// Opt-in by default for iceberg sinks; `fill_nulls: false` opts out.
pub(super) fn should_fill_nulls(job_def: &JobDefinition) -> bool {
    job_def
        .sink
        .as_ref()
        .is_some_and(|s| s.sink_type == "iceberg" && s.fill_nulls != Some(false))
}

pub(super) fn render_partition_derivation(job_def: &JobDefinition, sink_source: &str) -> Option<String> {
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

pub(super) fn needs_functions_import(job_def: &JobDefinition) -> bool {
    job_def
        .transforms
        .iter()
        .any(|t| matches!(t.transform_type.as_str(), "aggregate" | "window"))
}

pub(super) fn needs_window_import(job_def: &JobDefinition) -> bool {
    job_def
        .transforms
        .iter()
        .any(|t| t.transform_type == "window")
}

pub(super) fn needs_dynamic_frame_import(job_def: &JobDefinition, default_engine: &str) -> bool {
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

pub(super) fn needs_requests_import(job_def: &JobDefinition) -> bool {
    job_def.sources.iter().any(|s| s.source_type == "api")
}

pub(super) fn default_engine_for(job_def: &JobDefinition) -> String {
    // The `config` map is keyed by the wire-format string ("glue", "emr",
    // "bash"); JobType::to_string() returns that canonical form.
    job_def
        .config
        .get(job_def.job_type.to_string().as_str())
        .and_then(|g| g.get("default_engine"))
        .and_then(|v| v.as_str())
        .unwrap_or("spark")
        .to_string()
}

pub(super) fn indent_body(body: &str) -> String {
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
