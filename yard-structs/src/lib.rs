use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug)]
pub enum YardAction {
    Init {
        manifest: ProjectManifest,
    },
    Apply {
        manifest_path: String,
        target_env: String,
    },
    Destroy {
        resource_id: String,
    },
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
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Resource {
    pub r#type: String,
    pub id: String,       // Unique identifier (ARN/Name)
    pub provider: String, // "aws", "azure", "local" - helps the Core know which driver to use
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Deployment {
    pub env: String,
    pub config_hash: String,
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
