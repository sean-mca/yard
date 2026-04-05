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
pub struct JobDefinition {
    pub job_type: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YARDContext {
    pub account: serde_json::Value,
    pub region: serde_json::Value,
    pub transforms: serde_json::Value,
}
