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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Resource {
    pub r#type: String, // 'type' is a reserved keyword in Rust
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Deployment {
    pub env: String,
    pub config_hash: String,
    pub status: String,
    pub applied_at: String, // We'll use ISO 8601 strings
    pub resources: Vec<Resource>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectState {
    pub project: String,
    pub lineage: String,
    pub last_updated: String,
    pub deployments: HashMap<String, Deployment>,
}

impl Default for ProjectState {
    fn default() -> Self {
        Self {
            project: String::new(),
            lineage: uuid::Uuid::new_v4().to_string(), // Requires 'uuid' crate
            last_updated: String::new(),
            deployments: HashMap::new(),
        }
    }
}
