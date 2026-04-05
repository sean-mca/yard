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
    pub jobs: HashMap<String, JobDefinition>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Import {
    pub name: String,
    pub from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Source {
    pub source_type: String,          // s3, jdbc, catalog
    pub format: Option<String>,       // parquet, csv, json, orc (s3)
    pub path: Option<String>,         // s3 path
    pub connection_url: Option<String>, // jdbc
    pub table: Option<String>,        // jdbc or catalog
    pub database: Option<String>,     // catalog
    pub secret_id: Option<String>,    // Secrets Manager secret
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Sink {
    pub sink_type: String,            // s3, jdbc, catalog
    pub format: Option<String>,       // parquet, csv, json, orc (s3)
    pub path: Option<String>,         // s3 path
    pub connection_url: Option<String>, // jdbc
    pub table: Option<String>,        // jdbc or catalog
    pub database: Option<String>,     // catalog
    pub secret_id: Option<String>,    // Secrets Manager secret
    pub mode: Option<String>,         // overwrite, append, error
    pub partition_by: Vec<String>,    // partition columns
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transform {
    pub transform_type: String,                   // filter, sql, drop_columns, rename, select, add_column
    pub condition: Option<String>,                // filter
    pub query: Option<String>,                    // sql
    pub columns: Vec<String>,                     // drop_columns, select
    pub mapping: HashMap<String, String>,         // rename (old -> new)
    pub name: Option<String>,                     // add_column
    pub expression: Option<String>,               // add_column
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobDefinition {
    pub job_type: String,
    pub imports: Vec<Import>,
    pub body: Option<String>,
    pub source: Option<Source>,
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
