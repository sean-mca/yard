use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

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

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transform {
    pub transform_type: String, // filter, sql, drop_columns, rename, select, add_column, join
    pub source: Option<String>, // which df to operate on (defaults to first/only source)
    pub output: Option<String>, // name for result df (defaults to same as source)
    pub condition: Option<String>, // filter
    pub query: Option<String>,  // sql
    pub columns: Vec<String>,   // drop_columns, select
    pub mapping: HashMap<String, String>, // rename (old -> new)
    pub name: Option<String>,   // add_column
    pub expression: Option<String>, // add_column
    // join fields
    pub left: Option<String>,  // join: left df name
    pub right: Option<String>, // join: right df name
    pub on: Option<String>,    // join: column to join on
    pub how: Option<String>,   // join: inner, left, right, outer
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobDefinition {
    pub job_type: String,
    pub imports: Vec<Import>,
    pub body: Option<String>,
    /// Path to an external Python file that replaces YARD's generated script entirely.
    pub job_file: Option<String>,
    pub sources: Vec<Source>,
    pub sink: Option<Sink>,
    pub transforms: Vec<Transform>,
    pub config: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YARDContext {
    pub account: serde_json::Value,
    pub region: serde_json::Value,
    pub transforms: serde_json::Value,
}
