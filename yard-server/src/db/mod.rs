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

#[allow(dead_code)]
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

#[allow(dead_code)]
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
                .rfind(|p| p.pr_number == pr_number)
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
                .rfind(|d| d.job_name == job_name)
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

#[cfg(test)]
mod tests {
    use super::*;
    use test_support::InMemoryDb;
    use chrono::Utc;
    use serde_json::json;

    fn make_webhook(pr: u64, action: &str) -> WebhookEvent {
        WebhookEvent {
            id: uuid::Uuid::new_v4().to_string(),
            pr_number: pr,
            action: action.to_string(),
            sha: "abc123".to_string(),
            payload: json!({"test": true}),
            received_at: Utc::now(),
        }
    }

    fn make_plan(pr: u64, status: PlanStatus) -> PlanResultRow {
        PlanResultRow {
            id: uuid::Uuid::new_v4().to_string(),
            pr_number: pr,
            sha: "abc123".to_string(),
            status,
            raw_output: "test output".to_string(),
            diff_summary: None,
            created_at: Utc::now(),
        }
    }

    fn make_drift(job: &str, drifted: bool) -> DriftSnapshot {
        DriftSnapshot {
            id: uuid::Uuid::new_v4().to_string(),
            job_name: job.to_string(),
            repo_hash: "abc".to_string(),
            state_hash: "def".to_string(),
            drifted,
            checked_at: Utc::now(),
        }
    }

    // Webhook tests

    #[tokio::test]
    async fn test_insert_and_list_webhook_events() {
        let db = InMemoryDb::new();
        let e1 = make_webhook(42, "opened");
        let e2 = make_webhook(42, "synchronize");
        db.insert_webhook_event(&e1).await.unwrap();
        db.insert_webhook_event(&e2).await.unwrap();
        let events = db.list_webhook_events(42, 10).await.unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_list_webhook_events_filters_by_pr() {
        let db = InMemoryDb::new();
        db.insert_webhook_event(&make_webhook(1, "opened")).await.unwrap();
        db.insert_webhook_event(&make_webhook(2, "opened")).await.unwrap();
        db.insert_webhook_event(&make_webhook(1, "synchronize")).await.unwrap();
        let events = db.list_webhook_events(1, 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e.pr_number == 1));
    }

    #[tokio::test]
    async fn test_list_webhook_events_respects_limit() {
        let db = InMemoryDb::new();
        for i in 0..5 {
            db.insert_webhook_event(&make_webhook(1, &format!("action-{i}"))).await.unwrap();
        }
        let events = db.list_webhook_events(1, 2).await.unwrap();
        assert_eq!(events.len(), 2);
    }

    // Plan tests

    #[tokio::test]
    async fn test_insert_and_get_latest_plan_result() {
        let db = InMemoryDb::new();
        let p1 = make_plan(10, PlanStatus::Pending);
        let p2 = make_plan(10, PlanStatus::Success);
        db.insert_plan_result(&p1).await.unwrap();
        db.insert_plan_result(&p2).await.unwrap();
        let latest = db.get_latest_plan_result(10).await.unwrap().unwrap();
        assert_eq!(latest.id, p2.id);
    }

    #[tokio::test]
    async fn test_get_latest_plan_result_none() {
        let db = InMemoryDb::new();
        let result = db.get_latest_plan_result(999).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_plan_results() {
        let db = InMemoryDb::new();
        db.insert_plan_result(&make_plan(1, PlanStatus::Success)).await.unwrap();
        db.insert_plan_result(&make_plan(2, PlanStatus::Failure)).await.unwrap();
        db.insert_plan_result(&make_plan(3, PlanStatus::Pending)).await.unwrap();
        let plans = db.list_plan_results(10).await.unwrap();
        assert_eq!(plans.len(), 3);
    }

    // Drift tests

    #[tokio::test]
    async fn test_insert_and_get_latest_drift_snapshot() {
        let db = InMemoryDb::new();
        let snap = make_drift("job-a", true);
        db.insert_drift_snapshot(&snap).await.unwrap();
        let latest = db.get_latest_drift_snapshot("job-a").await.unwrap().unwrap();
        assert_eq!(latest.job_name, "job-a");
        assert!(latest.drifted);
    }

    #[tokio::test]
    async fn test_list_drift_snapshots_all() {
        let db = InMemoryDb::new();
        db.insert_drift_snapshot(&make_drift("a", true)).await.unwrap();
        db.insert_drift_snapshot(&make_drift("b", true)).await.unwrap();
        db.insert_drift_snapshot(&make_drift("c", false)).await.unwrap();
        let all = db.list_drift_snapshots(false, 10).await.unwrap();
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_list_drift_snapshots_drifted_only() {
        let db = InMemoryDb::new();
        db.insert_drift_snapshot(&make_drift("a", true)).await.unwrap();
        db.insert_drift_snapshot(&make_drift("b", true)).await.unwrap();
        db.insert_drift_snapshot(&make_drift("c", false)).await.unwrap();
        let drifted = db.list_drift_snapshots(true, 10).await.unwrap();
        assert_eq!(drifted.len(), 2);
        assert!(drifted.iter().all(|d| d.drifted));
    }

    // Settings tests

    #[tokio::test]
    async fn test_set_and_get_setting() {
        let db = InMemoryDb::new();
        db.set_setting("theme", "dark").await.unwrap();
        let val = db.get_setting("theme").await.unwrap().unwrap();
        assert_eq!(val, "dark");
    }

    #[tokio::test]
    async fn test_get_setting_missing() {
        let db = InMemoryDb::new();
        let result = db.get_setting("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_list_settings() {
        let db = InMemoryDb::new();
        db.set_setting("theme", "dark").await.unwrap();
        db.set_setting("drift_interval", "5").await.unwrap();
        let settings = db.list_settings().await.unwrap();
        assert_eq!(settings.len(), 2);
    }

    // Cache tests

    #[tokio::test]
    async fn test_set_and_get_cache() {
        let db = InMemoryDb::new();
        db.set_cache("drift", "{}").await.unwrap();
        let val = db.get_cache("drift").await.unwrap().unwrap();
        assert_eq!(val, "{}");
    }

    #[tokio::test]
    async fn test_get_cache_missing() {
        let db = InMemoryDb::new();
        let result = db.get_cache("nonexistent").await.unwrap();
        assert!(result.is_none());
    }
}
