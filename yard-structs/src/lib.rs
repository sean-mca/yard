use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub enum YardAction {
    Init { manifest: ProjectManifest },
    Plan { manifest: ProjectManifest },
    Apply { manifest: ProjectManifest },
    Destroy { resource_id: String },
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectManifest {
    pub project: String,
    pub state: StateBackend,
    pub jobs: HashMap<String, JobDefinition>, // This was missing!
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobDefinition {
    pub job_type: String,          // e.g., "glue"
    pub config: serde_json::Value, // Catch-all for Glue/Lambda/etc. configs
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Resource {
    pub r#type: String,
    pub id: String,       // Unique identifier (ARN/Name)
    pub provider: String, // "aws", "azure", "local" - helps the Core know which driver to use
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Deployment {
    pub env: Option<String>,
    pub config_hash: String,
    pub config: serde_json::Value,
    pub status: String, // "success", "failed", "running"
    pub applied_at: String,
    pub resources: Vec<Resource>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectState {
    pub project: String,
    pub last_updated: String,
    pub deployments: HashMap<String, Deployment>, // Key = "job_name"
}

#[derive(Debug)]
pub enum StateChange {
    Create(String),
    Delete(String),
    Modify(String),
    NoChange(String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct YARDContext {
    pub account: serde_json::Value,
    pub region: serde_json::Value,
    pub transforms: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DiffType {
    Create,
    Modify {
        // Key -> (Old Value, New Value)
        changes: std::collections::HashMap<String, (String, String)>,
    },
    Delete,
    None,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobDiff {
    pub name: String,
    pub diff_type: DiffType,
    pub old_hash: Option<String>,
    pub new_hash: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobState {
    pub config_hash: String,
    pub config: serde_json::Value,
    pub status: String,
    pub applied_at: String,
    pub resources: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct State {
    pub project: String,
    pub last_updated: String,
    pub deployments: std::collections::HashMap<String, JobState>,
}
