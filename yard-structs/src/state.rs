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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectState {
    pub project: String,
    pub last_updated: String,
    pub deployments: HashMap<String, Deployment>,
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
    pub deployments: HashMap<String, JobState>,
}
