use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

fn default_path_buf() -> PathBuf {
    PathBuf::new()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StateBackend {
    Local {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        key: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectManifest {
    pub project: String,
    pub state: StateBackend,
    /// Per-provider config, keyed by job type (e.g. "glue", "emr").
    /// Each value is the raw provider config block from yard.yaml.
    pub providers: HashMap<String, serde_json::Value>,
    pub jobs: HashMap<String, JobDefinition>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Import {
    pub name: String,
    pub from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Source {
    pub name: String,                   // variable name: produces df_<name>
    pub source_type: String,            // s3, jdbc, catalog
    pub format: Option<String>,         // parquet, csv, json, orc
    pub path: Option<String>,           // s3 path
    pub connection_url: Option<String>, // jdbc
    pub table: Option<String>,          // jdbc or catalog
    pub database: Option<String>,       // catalog
    pub secret_id: Option<String>,      // Secrets Manager secret
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Sink {
    pub source: Option<String>, // which df to write (defaults to first/only source)
    pub sink_type: String,      // s3, jdbc, catalog
    pub format: Option<String>, // parquet, csv, json, orc
    pub path: Option<String>,   // s3 path
    pub connection_url: Option<String>, // jdbc
    pub table: Option<String>,  // jdbc or catalog
    pub database: Option<String>, // catalog
    pub secret_id: Option<String>, // Secrets Manager secret
    pub mode: Option<String>,   // overwrite, append, error
    pub partition_by: Vec<String>, // partition columns
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct OrderBySpec {
    pub column: String,
    pub desc: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Transform {
    pub transform_type: String, // filter, sql, drop_columns, rename, select, add_column, join, aggregate, window
    pub source: Option<String>, // which df to operate on (defaults to first/only source)
    pub output: Option<String>, // name for result df (defaults to same as source)
    pub condition: Option<String>, // filter
    pub query: Option<String>,  // sql
    pub columns: Vec<String>,   // drop_columns, select
    pub mapping: HashMap<String, String>, // rename (old -> new)
    pub name: Option<String>,   // add_column, window (new column name)
    pub expression: Option<String>, // add_column, window (window expression)
    // join fields
    pub left: Option<String>,  // join: left df name
    pub right: Option<String>, // join: right df name
    pub on: Option<String>,    // join: column to join on
    pub how: Option<String>,   // join: inner, left, right, outer
    // aggregate fields
    pub group_by: Vec<String>,         // aggregate: grouping columns
    pub aggs: HashMap<String, String>, // aggregate: alias -> expression (e.g. "total" -> "sum(amount)")
    // window fields
    pub partition_by: Vec<String>,  // window: partition columns
    pub order_by: Vec<OrderBySpec>, // window: order spec
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct JobDefinition {
    pub job_type: String,
    pub imports: Vec<Import>,
    pub body: Option<String>,
    /// Path to an external Python file that replaces YARD's generated script entirely.
    pub job_file: Option<String>,
    pub sources: Vec<Source>,
    pub sink: Option<Sink>,
    pub transforms: Vec<Transform>,
    /// Per-job Airflow metadata, parsed from the optional `airflow:` block.
    /// `None` means the job does not participate in any DAG.
    pub airflow: Option<AirflowJobBlock>,
    pub config: serde_json::Value,
    /// Directory containing the job's YAML file. Populated during discovery;
    /// not serialized to state. Used to locate the nearest ancestor `dag.yaml`
    /// when grouping jobs into DAGs.
    #[serde(skip, default = "default_path_buf")]
    pub dir: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct YARDContext {
    pub account: serde_json::Value,
    pub region: serde_json::Value,
    pub transforms: serde_json::Value,
    /// Loaded from the optional `dag.yaml` marker file in a job's directory
    /// (or the nearest ancestor). Presence marks the directory as a DAG grouping.
    /// Contents hold DAG-level Airflow config (schedule, default_args, etc).
    pub dag: serde_json::Value,
}

/// Airflow config shared across inheritance layers (yard.yaml, region.yaml,
/// account.yaml, dag.yaml, and the per-job `airflow:` block). Every layer has
/// the same shape; later layers override earlier ones via shallow merge.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct AirflowSection {
    pub schedule: Option<String>,
    pub owner: Option<String>,
    pub retries: Option<i32>,
    pub dags_bucket: Option<String>,
    pub dags_prefix: Option<String>,
}

/// Per-job Airflow metadata lifted out of the `airflow:` block on a job file.
/// Includes the shared [`AirflowSection`] overrides plus job-specific fields
/// like `depends_on`.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct AirflowJobBlock {
    pub depends_on: Vec<String>,
    #[serde(flatten, default)]
    pub overrides: AirflowSection,
}
