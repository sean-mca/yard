use serde::{Deserialize, Serialize};
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
