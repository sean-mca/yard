use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Resource {
    pub r#type: String,
    pub id: String,
    pub provider: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Deployment {
    pub env: Option<String>,
    pub config_hash: String,
    pub config: serde_json::Value,
    pub status: String,
    pub applied_at: String,
    pub resources: Vec<Resource>,
}

/// Aggregate view of all per-job state files. Not persisted directly —
/// assembled at runtime by reading individual JobState files.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectState {
    pub project: String,
    pub last_updated: String,
    pub deployments: HashMap<String, Deployment>,
}

/// Per-job state file, stored as <job_name>.json in the state directory/prefix.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobState {
    pub job_name: String,
    pub project: String,
    pub deployment: Deployment,
}

/// Written to <job_name>.json.lock alongside the state file.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockInfo {
    pub who: String,
    pub created_at: String,
}
