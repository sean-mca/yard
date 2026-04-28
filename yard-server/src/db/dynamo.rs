use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    KeyType, Projection, ProjectionType, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use tracing::info;

use super::{Database, DriftSnapshot, PlanResultRow, PlanStatus, Setting, WebhookEvent};

pub struct DynamoDatabase {
    client: Client,
    table_name: String,
}

#[allow(dead_code)]
impl DynamoDatabase {
    pub async fn connect(
        table_name: &str,
        region: &str,
        endpoint_url: Option<&str>,
    ) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()));

        if let Some(url) = endpoint_url {
            loader = loader.endpoint_url(url);
        }

        let config = loader.load().await;

        let client = Client::new(&config);

        Ok(DynamoDatabase {
            client,
            table_name: table_name.to_string(),
        })
    }

    pub async fn migrate(&self) -> Result<()> {
        let result = self
            .client
            .create_table()
            .table_name(&self.table_name)
            .billing_mode(BillingMode::PayPerRequest)
            // Key schema
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("SK")
                    .key_type(KeyType::Range)
                    .build()?,
            )
            // Attribute definitions (PK, SK, GSI1PK, GSI1SK)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("SK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("GSI1PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("GSI1SK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            // GSI1
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("GSI1")
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("GSI1PK")
                            .key_type(KeyType::Hash)
                            .build()?,
                    )
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("GSI1SK")
                            .key_type(KeyType::Range)
                            .build()?,
                    )
                    .projection(
                        Projection::builder()
                            .projection_type(ProjectionType::All)
                            .build(),
                    )
                    .build()?,
            )
            .send()
            .await;

        match result {
            Ok(_) => {
                info!(table = %self.table_name, "Created DynamoDB table");
                self.wait_for_table_active().await?;
            }
            Err(err) => {
                let service_err = err.into_service_error();
                if service_err.is_resource_in_use_exception() {
                    info!(table = %self.table_name, "DynamoDB table already exists");
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to create DynamoDB table: {service_err}"
                    ));
                }
            }
        }

        Ok(())
    }

    async fn wait_for_table_active(&self) -> Result<()> {
        for _ in 0..30 {
            let resp = self
                .client
                .describe_table()
                .table_name(&self.table_name)
                .send()
                .await?;

            if let Some(table) = resp.table()
                && table.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active)
            {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        anyhow::bail!("Timed out waiting for table to become active")
    }
}

#[async_trait]
impl Database for DynamoDatabase {
    // ---- Webhook Events ----

    async fn insert_webhook_event(&self, event: &WebhookEvent) -> Result<()> {
        let pk = format!("PR#{}", event.pr_number);
        let sk = format!("WEBHOOK#{}#{}", event.received_at.to_rfc3339(), event.id);
        let payload_str =
            serde_json::to_string(&event.payload).context("serialize webhook payload")?;

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .item("GSI1PK", AttributeValue::S("WEBHOOK".to_string()))
            .item(
                "GSI1SK",
                AttributeValue::S(event.received_at.to_rfc3339()),
            )
            .item("id", AttributeValue::S(event.id.clone()))
            .item(
                "pr_number",
                AttributeValue::N(event.pr_number.to_string()),
            )
            .item("action", AttributeValue::S(event.action.clone()))
            .item("sha", AttributeValue::S(event.sha.clone()))
            .item("payload", AttributeValue::S(payload_str))
            .item(
                "received_at",
                AttributeValue::S(event.received_at.to_rfc3339()),
            )
            .send()
            .await
            .context("insert webhook event")?;

        Ok(())
    }

    async fn list_webhook_events(
        &self,
        pr_number: u64,
        limit: u32,
    ) -> Result<Vec<WebhookEvent>> {
        let pk = format!("PR#{pr_number}");

        let resp = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("PK = :pk AND begins_with(SK, :sk_prefix)")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":sk_prefix", AttributeValue::S("WEBHOOK#".to_string()))
            .scan_index_forward(false)
            .limit(limit as i32)
            .send()
            .await
            .context("list webhook events")?;

        let items = resp.items();
        Ok(items.iter().filter_map(parse_webhook_event).collect())
    }

    // ---- Plan Results ----

    async fn insert_plan_result(&self, result: &PlanResultRow) -> Result<()> {
        let pk = format!("PR#{}", result.pr_number);
        let sk = format!("PLAN#{}#{}", result.created_at.to_rfc3339(), result.id);

        let mut put = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .item("GSI1PK", AttributeValue::S("PLAN".to_string()))
            .item(
                "GSI1SK",
                AttributeValue::S(result.created_at.to_rfc3339()),
            )
            .item("id", AttributeValue::S(result.id.clone()))
            .item(
                "pr_number",
                AttributeValue::N(result.pr_number.to_string()),
            )
            .item("sha", AttributeValue::S(result.sha.clone()))
            .item(
                "status",
                AttributeValue::S(result.status.as_str().to_string()),
            )
            .item("raw_output", AttributeValue::S(result.raw_output.clone()))
            .item(
                "created_at",
                AttributeValue::S(result.created_at.to_rfc3339()),
            );

        if let Some(ref summary) = result.diff_summary {
            let summary_str = serde_json::to_string(summary).context("serialize diff summary")?;
            put = put.item("diff_summary", AttributeValue::S(summary_str));
        }

        put.send().await.context("insert plan result")?;

        Ok(())
    }

    async fn get_latest_plan_result(&self, pr_number: u64) -> Result<Option<PlanResultRow>> {
        let pk = format!("PR#{pr_number}");

        let resp = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("PK = :pk AND begins_with(SK, :sk_prefix)")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":sk_prefix", AttributeValue::S("PLAN#".to_string()))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .context("get latest plan result")?;

        match resp.items().first() {
            Some(item) => Ok(Some(parse_plan_result(item)?)),
            None => Ok(None),
        }
    }

    async fn list_plan_results(&self, limit: u32) -> Result<Vec<PlanResultRow>> {
        let resp = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("GSI1")
            .key_condition_expression("GSI1PK = :gsi1pk")
            .expression_attribute_values(":gsi1pk", AttributeValue::S("PLAN".to_string()))
            .scan_index_forward(false)
            .limit(limit as i32)
            .send()
            .await
            .context("list plan results")?;

        let items: Vec<PlanResultRow> = resp.items().iter().map(parse_plan_result).collect::<anyhow::Result<Vec<_>>>()?;
        Ok(items)
    }

    // ---- Drift Snapshots ----

    async fn insert_drift_snapshot(&self, snapshot: &DriftSnapshot) -> Result<()> {
        let pk = format!("JOB#{}", snapshot.job_name);
        let sk = format!(
            "DRIFT#{}#{}",
            snapshot.checked_at.to_rfc3339(),
            snapshot.id
        );

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .item("GSI1PK", AttributeValue::S("DRIFT".to_string()))
            .item(
                "GSI1SK",
                AttributeValue::S(snapshot.checked_at.to_rfc3339()),
            )
            .item("id", AttributeValue::S(snapshot.id.clone()))
            .item("job_name", AttributeValue::S(snapshot.job_name.clone()))
            .item("repo_hash", AttributeValue::S(snapshot.repo_hash.clone()))
            .item(
                "state_hash",
                AttributeValue::S(snapshot.state_hash.clone()),
            )
            .item(
                "drifted",
                AttributeValue::Bool(snapshot.drifted),
            )
            .item(
                "checked_at",
                AttributeValue::S(snapshot.checked_at.to_rfc3339()),
            )
            .send()
            .await
            .context("insert drift snapshot")?;

        Ok(())
    }

    async fn get_latest_drift_snapshot(
        &self,
        job_name: &str,
    ) -> Result<Option<DriftSnapshot>> {
        let pk = format!("JOB#{job_name}");

        let resp = self
            .client
            .query()
            .table_name(&self.table_name)
            .key_condition_expression("PK = :pk AND begins_with(SK, :sk_prefix)")
            .expression_attribute_values(":pk", AttributeValue::S(pk))
            .expression_attribute_values(":sk_prefix", AttributeValue::S("DRIFT#".to_string()))
            .scan_index_forward(false)
            .limit(1)
            .send()
            .await
            .context("get latest drift snapshot")?;

        Ok(resp.items().first().and_then(parse_drift_snapshot))
    }

    async fn list_drift_snapshots(
        &self,
        drifted_only: bool,
        limit: u32,
    ) -> Result<Vec<DriftSnapshot>> {
        let mut query = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("GSI1")
            .key_condition_expression("GSI1PK = :gsi1pk")
            .expression_attribute_values(":gsi1pk", AttributeValue::S("DRIFT".to_string()))
            .scan_index_forward(false)
            .limit(limit as i32);

        if drifted_only {
            query = query
                .filter_expression("drifted = :drifted")
                .expression_attribute_values(":drifted", AttributeValue::Bool(true));
        }

        let resp = query.send().await.context("list drift snapshots")?;

        Ok(resp
            .items()
            .iter()
            .filter_map(parse_drift_snapshot)
            .collect())
    }

    // ---- Settings ----

    async fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let pk = format!("SETTING#{key}");

        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk.clone()))
            .key("SK", AttributeValue::S(pk))
            .send()
            .await
            .context("get setting")?;

        Ok(resp
            .item()
            .and_then(|item| item.get("value"))
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string()))
    }

    async fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let pk = format!("SETTING#{key}");

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk.clone()))
            .item("SK", AttributeValue::S(pk))
            .item("GSI1PK", AttributeValue::S("SETTING".to_string()))
            .item("GSI1SK", AttributeValue::S(key.to_string()))
            .item("key", AttributeValue::S(key.to_string()))
            .item("value", AttributeValue::S(value.to_string()))
            .send()
            .await
            .context("set setting")?;

        Ok(())
    }

    async fn list_settings(&self) -> Result<Vec<Setting>> {
        let resp = self
            .client
            .query()
            .table_name(&self.table_name)
            .index_name("GSI1")
            .key_condition_expression("GSI1PK = :gsi1pk")
            .expression_attribute_values(":gsi1pk", AttributeValue::S("SETTING".to_string()))
            .send()
            .await
            .context("list settings")?;

        Ok(resp
            .items()
            .iter()
            .filter_map(|item| {
                let key = item.get("key")?.as_s().ok()?.to_string();
                let value = item.get("value")?.as_s().ok()?.to_string();
                Some(Setting { key, value })
            })
            .collect())
    }

    // ---- Cache ----

    async fn set_cache(&self, key: &str, data: &str) -> Result<()> {
        let pk = format!("CACHE#{key}");

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk.clone()))
            .item("SK", AttributeValue::S(pk))
            .item("GSI1PK", AttributeValue::S("CACHE".to_string()))
            .item("GSI1SK", AttributeValue::S(key.to_string()))
            .item("data", AttributeValue::S(data.to_string()))
            .send()
            .await
            .context("set cache")?;

        Ok(())
    }

    async fn get_cache(&self, key: &str) -> Result<Option<String>> {
        let pk = format!("CACHE#{key}");

        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk.clone()))
            .key("SK", AttributeValue::S(pk))
            .send()
            .await
            .context("get cache")?;

        Ok(resp
            .item()
            .and_then(|item| item.get("data"))
            .and_then(|v| v.as_s().ok())
            .map(|s| s.to_string()))
    }
}

// ---- Item Parsers ----

fn get_s(item: &HashMap<String, AttributeValue>, key: &str) -> Option<String> {
    item.get(key)?.as_s().ok().map(|s| s.to_string())
}

fn get_n_u64(item: &HashMap<String, AttributeValue>, key: &str) -> Option<u64> {
    item.get(key)?.as_n().ok()?.parse().ok()
}

fn get_dt(item: &HashMap<String, AttributeValue>, key: &str) -> Option<DateTime<Utc>> {
    let s = get_s(item, key)?;
    DateTime::parse_from_rfc3339(&s).ok().map(|dt| dt.to_utc())
}

#[allow(dead_code)]
fn parse_webhook_event(item: &HashMap<String, AttributeValue>) -> Option<WebhookEvent> {
    let payload_str = get_s(item, "payload")?;
    let payload: Value = serde_json::from_str(&payload_str).ok()?;

    Some(WebhookEvent {
        id: get_s(item, "id")?,
        pr_number: get_n_u64(item, "pr_number")?,
        action: get_s(item, "action")?,
        sha: get_s(item, "sha")?,
        payload,
        received_at: get_dt(item, "received_at")?,
    })
}

/// Parse a DDB attribute map into a `PlanResultRow`.
///
/// Returns `Err` on:
/// - missing required fields (id, pr_number, sha, status, raw_output, created_at)
/// - corrupt PlanStatus value (unknown enum variant; D-20 fail-fast semantics)
///
/// `diff_summary` remains tolerant of missing/malformed JSON (existing behavior;
/// it's an optional analytical field, not a row-validity gate).
fn parse_plan_result(item: &HashMap<String, AttributeValue>) -> anyhow::Result<PlanResultRow> {
    let diff_summary = get_s(item, "diff_summary")
        .and_then(|s| serde_json::from_str(&s).ok());

    let status_raw = get_s(item, "status")
        .ok_or_else(|| anyhow::anyhow!("plan_result row missing status field"))?;
    let status: PlanStatus = status_raw
        .parse()
        .with_context(|| "parsing PlanStatus for plan_result row".to_string())?;

    Ok(PlanResultRow {
        id: get_s(item, "id")
            .ok_or_else(|| anyhow::anyhow!("plan_result row missing id field"))?,
        pr_number: get_n_u64(item, "pr_number")
            .ok_or_else(|| anyhow::anyhow!("plan_result row missing pr_number field"))?,
        sha: get_s(item, "sha")
            .ok_or_else(|| anyhow::anyhow!("plan_result row missing sha field"))?,
        status,
        raw_output: get_s(item, "raw_output")
            .ok_or_else(|| anyhow::anyhow!("plan_result row missing raw_output field"))?,
        diff_summary,
        created_at: get_dt(item, "created_at")
            .ok_or_else(|| anyhow::anyhow!("plan_result row missing created_at field"))?,
    })
}

fn parse_drift_snapshot(item: &HashMap<String, AttributeValue>) -> Option<DriftSnapshot> {
    Some(DriftSnapshot {
        id: get_s(item, "id")?,
        job_name: get_s(item, "job_name")?,
        repo_hash: get_s(item, "repo_hash")?,
        state_hash: get_s(item, "state_hash")?,
        drifted: item.get("drifted")?.as_bool().ok().copied()?,
        checked_at: get_dt(item, "checked_at")?,
    })
}
