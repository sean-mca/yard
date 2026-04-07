pub mod dynamo;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

pub use dynamo::DynamoDatabase;

// ---- Models ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEvent {
    pub id: String,
    pub pr_number: u64,
    pub action: String,
    pub sha: String,
    pub payload: Value,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanStatus {
    Pending,
    Success,
    Failure,
}

impl PlanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanStatus::Pending => "pending",
            PlanStatus::Success => "success",
            PlanStatus::Failure => "failure",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "success" => PlanStatus::Success,
            "failure" => PlanStatus::Failure,
            _ => PlanStatus::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanResultRow {
    pub id: String,
    pub pr_number: u64,
    pub sha: String,
    pub status: PlanStatus,
    pub raw_output: String,
    pub diff_summary: Option<Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriftSnapshot {
    pub id: String,
    pub job_name: String,
    pub repo_hash: String,
    pub state_hash: String,
    pub drifted: bool,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Setting {
    pub key: String,
    pub value: String,
}

// ---- Configuration ----

pub struct DbConfig {
    pub table_name: String,
    pub region: String,
    pub endpoint_url: Option<String>,
}

impl DbConfig {
    pub fn from_env() -> Self {
        let prefix =
            std::env::var("YARD_DB_TABLE_PREFIX").unwrap_or_else(|_| "yard".to_string());
        let region = std::env::var("YARD_DB_REGION")
            .or_else(|_| std::env::var("AWS_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());
        let endpoint_url = std::env::var("YARD_DB_ENDPOINT_URL").ok();

        DbConfig {
            table_name: format!("{prefix}_yard"),
            region,
            endpoint_url,
        }
    }
}

// ---- Factory ----

pub async fn connect(config: &DbConfig) -> anyhow::Result<Arc<DynamoDatabase>> {
    let db = DynamoDatabase::connect(
        &config.table_name,
        &config.region,
        config.endpoint_url.as_deref(),
    )
    .await?;
    Ok(Arc::new(db))
}
