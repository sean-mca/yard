pub mod dynamo;

use async_trait::async_trait;
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

// ---- Database Trait ----

#[async_trait]
pub trait Database: Send + Sync {
    // Webhooks
    async fn insert_webhook_event(&self, event: &WebhookEvent) -> anyhow::Result<()>;
    async fn list_webhook_events(&self, pr_number: u64, limit: u32) -> anyhow::Result<Vec<WebhookEvent>>;
    // Plans
    async fn insert_plan_result(&self, result: &PlanResultRow) -> anyhow::Result<()>;
    async fn get_latest_plan_result(&self, pr_number: u64) -> anyhow::Result<Option<PlanResultRow>>;
    async fn list_plan_results(&self, limit: u32) -> anyhow::Result<Vec<PlanResultRow>>;
    // Drift
    async fn insert_drift_snapshot(&self, snapshot: &DriftSnapshot) -> anyhow::Result<()>;
    async fn get_latest_drift_snapshot(&self, job_name: &str) -> anyhow::Result<Option<DriftSnapshot>>;
    async fn list_drift_snapshots(&self, drifted_only: bool, limit: u32) -> anyhow::Result<Vec<DriftSnapshot>>;
    // Settings
    async fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>>;
    async fn set_setting(&self, key: &str, value: &str) -> anyhow::Result<()>;
    async fn list_settings(&self) -> anyhow::Result<Vec<Setting>>;
    // Cache
    async fn set_cache(&self, key: &str, data: &str) -> anyhow::Result<()>;
    async fn get_cache(&self, key: &str) -> anyhow::Result<Option<String>>;
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

pub async fn connect(config: &DbConfig) -> anyhow::Result<Arc<dyn Database>> {
    let db = DynamoDatabase::connect(
        &config.table_name,
        &config.region,
        config.endpoint_url.as_deref(),
    )
    .await?;
    Ok(Arc::new(db) as Arc<dyn Database>)
}

// ---- Test Support ----

#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::Mutex;

    pub struct InMemoryDb {
        webhooks: Mutex<Vec<WebhookEvent>>,
        plans: Mutex<Vec<PlanResultRow>>,
        drift: Mutex<Vec<DriftSnapshot>>,
        settings: Mutex<HashMap<String, String>>,
        cache: Mutex<HashMap<String, String>>,
    }

    impl InMemoryDb {
        pub fn new() -> Self {
            Self {
                webhooks: Mutex::new(Vec::new()),
                plans: Mutex::new(Vec::new()),
                drift: Mutex::new(Vec::new()),
                settings: Mutex::new(HashMap::new()),
                cache: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl Database for InMemoryDb {
        async fn insert_webhook_event(&self, event: &WebhookEvent) -> anyhow::Result<()> {
            self.webhooks.lock().await.push(event.clone());
            Ok(())
        }
        async fn list_webhook_events(&self, pr_number: u64, limit: u32) -> anyhow::Result<Vec<WebhookEvent>> {
            let events = self.webhooks.lock().await;
            Ok(events.iter()
                .filter(|e| e.pr_number == pr_number)
                .take(limit as usize)
                .cloned()
                .collect())
        }
        async fn insert_plan_result(&self, result: &PlanResultRow) -> anyhow::Result<()> {
            self.plans.lock().await.push(result.clone());
            Ok(())
        }
        async fn get_latest_plan_result(&self, pr_number: u64) -> anyhow::Result<Option<PlanResultRow>> {
            let plans = self.plans.lock().await;
            Ok(plans.iter()
                .filter(|p| p.pr_number == pr_number)
                .last()
                .cloned())
        }
        async fn list_plan_results(&self, limit: u32) -> anyhow::Result<Vec<PlanResultRow>> {
            let plans = self.plans.lock().await;
            Ok(plans.iter().rev().take(limit as usize).cloned().collect())
        }
        async fn insert_drift_snapshot(&self, snapshot: &DriftSnapshot) -> anyhow::Result<()> {
            self.drift.lock().await.push(snapshot.clone());
            Ok(())
        }
        async fn get_latest_drift_snapshot(&self, job_name: &str) -> anyhow::Result<Option<DriftSnapshot>> {
            let drift = self.drift.lock().await;
            Ok(drift.iter()
                .filter(|d| d.job_name == job_name)
                .last()
                .cloned())
        }
        async fn list_drift_snapshots(&self, drifted_only: bool, limit: u32) -> anyhow::Result<Vec<DriftSnapshot>> {
            let drift = self.drift.lock().await;
            Ok(drift.iter()
                .rev()
                .filter(|d| !drifted_only || d.drifted)
                .take(limit as usize)
                .cloned()
                .collect())
        }
        async fn get_setting(&self, key: &str) -> anyhow::Result<Option<String>> {
            Ok(self.settings.lock().await.get(key).cloned())
        }
        async fn set_setting(&self, key: &str, value: &str) -> anyhow::Result<()> {
            self.settings.lock().await.insert(key.to_string(), value.to_string());
            Ok(())
        }
        async fn list_settings(&self) -> anyhow::Result<Vec<Setting>> {
            Ok(self.settings.lock().await.iter()
                .map(|(k, v)| Setting { key: k.clone(), value: v.clone() })
                .collect())
        }
        async fn set_cache(&self, key: &str, data: &str) -> anyhow::Result<()> {
            self.cache.lock().await.insert(key.to_string(), data.to_string());
            Ok(())
        }
        async fn get_cache(&self, key: &str) -> anyhow::Result<Option<String>> {
            Ok(self.cache.lock().await.get(key).cloned())
        }
    }
}
