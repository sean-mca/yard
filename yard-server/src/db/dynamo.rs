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

use super::{
    AccountHealth, Database, DriftSnapshot, Environment, JobSummaryEntity, OAuthState,
    PlanResultRow, PlanStatus, RegionEntity, Session, Setting, WebhookEvent,
};

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

        // Enable TTL on the "ttl" attribute for session/OAuth state auto-expiry.
        // This is idempotent — calling when TTL is already enabled is a no-op
        // (AWS returns ValidationException with "already enabled" message).
        match self
            .client
            .update_time_to_live()
            .table_name(&self.table_name)
            .time_to_live_specification(
                aws_sdk_dynamodb::types::TimeToLiveSpecification::builder()
                    .enabled(true)
                    .attribute_name("ttl")
                    .build()
                    .map_err(|e| anyhow::anyhow!("failed to build TTL spec: {e}"))?,
            )
            .send()
            .await
        {
            Ok(_) => {
                info!(table = %self.table_name, "TTL enabled on 'ttl' attribute");
            }
            Err(err) => {
                let service_err = err.into_service_error();
                let msg = format!("{service_err}");
                // "TimeToLive is already enabled" or similar validation exception
                // when TTL is already active — this is expected and safe to ignore.
                if msg.contains("already enabled") || msg.contains("TimeToLive") {
                    info!(table = %self.table_name, "TTL already enabled on table");
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to enable TTL on DynamoDB table: {service_err}"
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
        let capped_limit = std::cmp::min(limit, i32::MAX as u32) as i32;
        let mut events = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression("PK = :pk AND begins_with(SK, :sk_prefix)")
                .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
                .expression_attribute_values(
                    ":sk_prefix",
                    AttributeValue::S("WEBHOOK#".to_string()),
                )
                .scan_index_forward(false)
                .limit(capped_limit);

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list webhook events")?;

            for item in resp.items() {
                if let Some(event) = parse_webhook_event(item) {
                    events.push(event);
                    if events.len() >= limit as usize {
                        return Ok(events);
                    }
                }
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(events)
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
        let capped_limit = std::cmp::min(limit, i32::MAX as u32) as i32;
        let mut results = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .index_name("GSI1")
                .key_condition_expression("GSI1PK = :gsi1pk")
                .expression_attribute_values(":gsi1pk", AttributeValue::S("PLAN".to_string()))
                .scan_index_forward(false)
                .limit(capped_limit);

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list plan results")?;

            for item in resp.items() {
                results.push(parse_plan_result(item)?);
                if results.len() >= limit as usize {
                    return Ok(results);
                }
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(results)
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
        let capped_limit = std::cmp::min(limit, i32::MAX as u32) as i32;
        let mut snapshots = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .index_name("GSI1")
                .key_condition_expression("GSI1PK = :gsi1pk")
                .expression_attribute_values(":gsi1pk", AttributeValue::S("DRIFT".to_string()))
                .scan_index_forward(false);

            if drifted_only {
                query = query
                    .filter_expression("drifted = :drifted")
                    .expression_attribute_values(":drifted", AttributeValue::Bool(true));
            } else {
                query = query.limit(capped_limit);
            }

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list drift snapshots")?;

            for item in resp.items() {
                if let Some(snap) = parse_drift_snapshot(item) {
                    snapshots.push(snap);
                    if snapshots.len() >= limit as usize {
                        return Ok(snapshots);
                    }
                }
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(snapshots)
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

    // ---- Environments ----

    async fn upsert_environment(&self, env: &Environment) -> Result<()> {
        validate_key_component(&env.name, "env.name")?;

        let pk = format!("ENV#{}", env.name);
        let regions_json =
            serde_json::to_string(&env.regions).context("serialize environment regions")?;

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk.clone()))
            .item("SK", AttributeValue::S(pk))
            .item("GSI1PK", AttributeValue::S("TYPE#ENV".to_string()))
            .item("GSI1SK", AttributeValue::S(env.name.clone()))
            .item("name", AttributeValue::S(env.name.clone()))
            .item("regions", AttributeValue::S(regions_json))
            .item(
                "job_count",
                AttributeValue::N(env.job_count.to_string()),
            )
            .item(
                "last_scanned",
                AttributeValue::S(env.last_scanned.to_rfc3339()),
            )
            .send()
            .await
            .context("upsert environment")?;

        Ok(())
    }

    async fn list_environments(&self) -> Result<Vec<Environment>> {
        let mut environments = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .index_name("GSI1")
                .key_condition_expression("GSI1PK = :gsi1pk")
                .expression_attribute_values(
                    ":gsi1pk",
                    AttributeValue::S("TYPE#ENV".to_string()),
                );

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list environments")?;

            for item in resp.items() {
                environments.push(parse_environment(item)?);
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(environments)
    }

    // ---- Regions (D-14) ----

    async fn upsert_region(&self, env_name: &str, region: &RegionEntity) -> Result<()> {
        validate_key_component(env_name, "env_name")?;
        validate_key_component(&region.name, "region.name")?;

        let pk = format!("ENV#{env_name}");
        let sk = format!("REGION#{}", region.name);

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .item("GSI1PK", AttributeValue::S("TYPE#REGION".to_string()))
            .item(
                "GSI1SK",
                AttributeValue::S(format!("{}#{}", env_name, region.name)),
            )
            .item("name", AttributeValue::S(region.name.clone()))
            .item(
                "job_count",
                AttributeValue::N(region.job_count.to_string()),
            )
            .item(
                "dag_count",
                AttributeValue::N(region.dag_count.to_string()),
            )
            .send()
            .await
            .context("upsert region")?;

        Ok(())
    }

    async fn list_regions(&self, env_name: &str) -> Result<Vec<RegionEntity>> {
        validate_key_component(env_name, "env_name")?;

        let pk = format!("ENV#{env_name}");
        let mut regions = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression("PK = :pk AND begins_with(SK, :sk_prefix)")
                .expression_attribute_values(":pk", AttributeValue::S(pk.clone()))
                .expression_attribute_values(
                    ":sk_prefix",
                    AttributeValue::S("REGION#".to_string()),
                );

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list regions")?;

            for item in resp.items() {
                regions.push(parse_region(env_name, item)?);
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(regions)
    }

    // ---- Job Summaries (D-15) ----

    async fn upsert_job_summary(&self, env_name: &str, job: &JobSummaryEntity) -> Result<()> {
        validate_key_component(env_name, "env_name")?;
        validate_key_component(&job.name, "job.name")?;

        let pk = format!("ENV#{env_name}");
        let sk = format!("JOB#{}", job.name);

        let mut put = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S(sk))
            .item("GSI1PK", AttributeValue::S("TYPE#JOB".to_string()))
            .item(
                "GSI1SK",
                AttributeValue::S(format!("{}#{}", env_name, job.name)),
            )
            .item("name", AttributeValue::S(job.name.clone()))
            .item("job_type", AttributeValue::S(job.job_type.clone()))
            .item(
                "region_name",
                AttributeValue::S(job.region_name.clone()),
            );

        if let Some(ref yaml) = job.config_yaml {
            put = put.item("config_yaml", AttributeValue::S(yaml.clone()));
        }

        put.send()
            .await
            .context("upsert job summary")?;

        Ok(())
    }

    // ---- Job Detail (DASH-04) ----

    async fn get_job_summary(&self, env_name: &str, job_name: &str) -> Result<Option<JobSummaryEntity>> {
        validate_key_component(env_name, "env_name")?;
        validate_key_component(job_name, "job_name")?;

        let pk = format!("ENV#{env_name}");
        let sk = format!("JOB#{job_name}");

        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S(sk))
            .send()
            .await
            .context("get job summary")?;

        match resp.item() {
            Some(item) => Ok(Some(parse_job_summary(item)?)),
            None => Ok(None),
        }
    }

    // ---- Account Health (D-11) ----

    async fn set_account_health(&self, health: &AccountHealth) -> Result<()> {
        validate_key_component(&health.account_id, "health.account_id")?;

        let pk = format!("HEALTH#{}", health.account_id);

        let mut put = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S("STATUS".to_string()))
            .item("GSI1PK", AttributeValue::S("TYPE#HEALTH".to_string()))
            .item(
                "GSI1SK",
                AttributeValue::S(health.account_id.clone()),
            )
            .item(
                "account_id",
                AttributeValue::S(health.account_id.clone()),
            )
            .item("status", AttributeValue::S(health.status.clone()))
            .item(
                "last_checked",
                AttributeValue::S(health.last_checked.to_rfc3339()),
            );

        if let Some(ref msg) = health.error_message {
            put = put.item("error_message", AttributeValue::S(msg.clone()));
        }

        put.send().await.context("set account health")?;

        Ok(())
    }

    async fn get_account_health(&self, account_id: &str) -> Result<Option<AccountHealth>> {
        validate_key_component(account_id, "account_id")?;

        let pk = format!("HEALTH#{account_id}");

        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("STATUS".to_string()))
            .send()
            .await
            .context("get account health")?;

        match resp.item() {
            Some(item) => Ok(Some(parse_account_health(item)?)),
            None => Ok(None),
        }
    }

    // ---- Sessions (Phase 45, D-08) ----

    async fn create_session(&self, session: &Session) -> Result<()> {
        validate_key_component(&session.session_id, "session.session_id")?;
        let pk = format!("SESSION#{}", session.session_id);
        let ttl_epoch = session.expires_at.timestamp();

        let mut put = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S("USER".to_string()))
            .item("email", AttributeValue::S(session.email.clone()))
            .item("provider", AttributeValue::S(session.provider.clone()))
            .item(
                "created_at",
                AttributeValue::S(session.created_at.to_rfc3339()),
            )
            .item("ttl", AttributeValue::N(ttl_epoch.to_string()));

        if let Some(ref token) = session.refresh_token {
            put = put.item("refresh_token", AttributeValue::S(token.clone()));
        }

        put.send().await.context("create session")?;

        Ok(())
    }

    async fn get_session(&self, session_id: &str) -> Result<Option<Session>> {
        validate_key_component(session_id, "session_id")?;
        let pk = format!("SESSION#{session_id}");

        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("USER".to_string()))
            .send()
            .await
            .context("get session")?;

        match resp.item() {
            Some(item) => {
                let session = parse_session(item)?;
                // Belt-and-suspenders TTL check alongside DynamoDB TTL.
                if session.expires_at < Utc::now() {
                    Ok(None)
                } else {
                    Ok(Some(session))
                }
            }
            None => Ok(None),
        }
    }

    async fn update_session_tokens(
        &self,
        session_id: &str,
        refresh_token: Option<&str>,
        new_expires_at: DateTime<Utc>,
    ) -> Result<()> {
        validate_key_component(session_id, "session_id")?;
        let pk = format!("SESSION#{session_id}");
        let ttl_epoch = new_expires_at.timestamp();

        let mut update = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("USER".to_string()))
            .update_expression("SET #ttl = :ttl, #rt = :rt")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_names("#rt", "refresh_token")
            .expression_attribute_values(":ttl", AttributeValue::N(ttl_epoch.to_string()));

        match refresh_token {
            Some(token) => {
                update = update.expression_attribute_values(
                    ":rt",
                    AttributeValue::S(token.to_string()),
                );
            }
            None => {
                // Set to NULL to clear the refresh token.
                update = update
                    .expression_attribute_values(":rt", AttributeValue::Null(true));
            }
        }

        update.send().await.context("update session tokens")?;

        Ok(())
    }

    async fn delete_session(&self, session_id: &str) -> Result<()> {
        validate_key_component(session_id, "session_id")?;
        let pk = format!("SESSION#{session_id}");

        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("USER".to_string()))
            .send()
            .await
            .context("delete session")?;

        Ok(())
    }

    // ---- OAuth State (Phase 45, Pitfall 2) ----

    async fn store_oauth_state(&self, state: &OAuthState) -> Result<()> {
        validate_key_component(&state.csrf_state, "oauth_state.csrf_state")?;
        let pk = format!("OAUTH_STATE#{}", state.csrf_state);
        // 10-minute TTL for OAuth state.
        let ttl_epoch = (Utc::now() + chrono::Duration::minutes(10)).timestamp();

        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("PK", AttributeValue::S(pk))
            .item("SK", AttributeValue::S("PKCE".to_string()))
            .item(
                "pkce_verifier",
                AttributeValue::S(state.pkce_verifier.clone()),
            )
            .item("provider", AttributeValue::S(state.provider.clone()))
            .item(
                "created_at",
                AttributeValue::S(state.created_at.to_rfc3339()),
            )
            .item("ttl", AttributeValue::N(ttl_epoch.to_string()))
            .send()
            .await
            .context("store oauth state")?;

        Ok(())
    }

    async fn get_oauth_state(&self, csrf_state: &str) -> Result<Option<OAuthState>> {
        validate_key_component(csrf_state, "csrf_state")?;
        let pk = format!("OAUTH_STATE#{csrf_state}");

        let resp = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("PKCE".to_string()))
            .send()
            .await
            .context("get oauth state")?;

        match resp.item() {
            Some(item) => Ok(Some(parse_oauth_state(item)?)),
            None => Ok(None),
        }
    }

    async fn delete_oauth_state(&self, csrf_state: &str) -> Result<()> {
        validate_key_component(csrf_state, "csrf_state")?;
        let pk = format!("OAUTH_STATE#{csrf_state}");

        self.client
            .delete_item()
            .table_name(&self.table_name)
            .key("PK", AttributeValue::S(pk))
            .key("SK", AttributeValue::S("PKCE".to_string()))
            .send()
            .await
            .context("delete oauth state")?;

        Ok(())
    }

    async fn list_all_account_health(&self) -> Result<Vec<AccountHealth>> {
        let mut health_records = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .index_name("GSI1")
                .key_condition_expression("GSI1PK = :gsi1pk")
                .expression_attribute_values(
                    ":gsi1pk",
                    AttributeValue::S("TYPE#HEALTH".to_string()),
                );

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list all account health")?;

            for item in resp.items() {
                health_records.push(parse_account_health(item)?);
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(health_records)
    }

    async fn list_job_summaries_all(&self) -> Result<Vec<JobSummaryEntity>> {
        let mut jobs = Vec::new();
        let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut query = self
                .client
                .query()
                .table_name(&self.table_name)
                .index_name("GSI1")
                .key_condition_expression("GSI1PK = :gsi1pk")
                .expression_attribute_values(
                    ":gsi1pk",
                    AttributeValue::S("TYPE#JOB".to_string()),
                );

            if let Some(start_key) = exclusive_start_key {
                query = query.set_exclusive_start_key(Some(start_key));
            }

            let resp = query.send().await.context("list all job summaries")?;

            for item in resp.items() {
                jobs.push(parse_job_summary(item)?);
            }

            match resp.last_evaluated_key() {
                Some(key) if !key.is_empty() => {
                    exclusive_start_key = Some(key.to_owned());
                }
                _ => break,
            }
        }

        Ok(jobs)
    }

    // ---- Health ----

    async fn health_check(&self) -> Result<()> {
        self.client
            .describe_table()
            .table_name(&self.table_name)
            .send()
            .await
            .context("DynamoDB health check (DescribeTable)")?;
        Ok(())
    }
}

// ---- Key Validation ----

/// Validates that a user-derived value is safe to use as a DynamoDB key component.
/// Rejects empty strings and strings containing the '#' delimiter to prevent
/// sort-key injection (T-40-03).
#[allow(dead_code)]
fn validate_key_component(value: &str, field_name: &str) -> Result<()> {
    if value.is_empty() {
        anyhow::bail!("{field_name} must not be empty");
    }
    if value.contains('#') {
        anyhow::bail!("{field_name} must not contain '#' delimiter, got: {value:?}");
    }
    Ok(())
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

#[allow(dead_code)]
fn parse_environment(item: &HashMap<String, AttributeValue>) -> Result<Environment> {
    let regions_json = get_s(item, "regions").unwrap_or_else(|| "[]".to_string());
    let regions: Vec<String> =
        serde_json::from_str(&regions_json).unwrap_or_default();

    Ok(Environment {
        name: get_s(item, "name")
            .ok_or_else(|| anyhow::anyhow!("environment row missing name field"))?,
        regions,
        job_count: get_n_u64(item, "job_count").unwrap_or(0),
        last_scanned: get_dt(item, "last_scanned")
            .ok_or_else(|| anyhow::anyhow!("environment row missing last_scanned field"))?,
    })
}

#[allow(dead_code)]
fn parse_region(env_name: &str, item: &HashMap<String, AttributeValue>) -> Result<RegionEntity> {
    Ok(RegionEntity {
        env_name: env_name.to_string(),
        name: get_s(item, "name")
            .ok_or_else(|| anyhow::anyhow!("region row missing name field"))?,
        job_count: get_n_u64(item, "job_count").unwrap_or(0),
        dag_count: get_n_u64(item, "dag_count").unwrap_or(0),
    })
}

/// Parse a DDB attribute map into a `Session`.
///
/// The `refresh_token` field is optional (absent for NoopAuth or older sessions).
/// The `ttl` (epoch seconds) is converted back to `expires_at` DateTime.
#[allow(dead_code)]
fn parse_session(item: &HashMap<String, AttributeValue>) -> Result<Session> {
    let ttl_epoch: i64 = item
        .get("ttl")
        .and_then(|v| v.as_n().ok())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("session row missing ttl field"))?;

    let expires_at = DateTime::from_timestamp(ttl_epoch, 0)
        .ok_or_else(|| anyhow::anyhow!("session row has invalid ttl timestamp"))?;

    // Extract session_id from PK (format: "SESSION#{session_id}").
    let pk = get_s(item, "PK")
        .ok_or_else(|| anyhow::anyhow!("session row missing PK field"))?;
    let session_id = pk
        .strip_prefix("SESSION#")
        .ok_or_else(|| anyhow::anyhow!("session PK does not start with SESSION#"))?
        .to_string();

    Ok(Session {
        session_id,
        email: get_s(item, "email")
            .ok_or_else(|| anyhow::anyhow!("session row missing email field"))?,
        provider: get_s(item, "provider")
            .ok_or_else(|| anyhow::anyhow!("session row missing provider field"))?,
        refresh_token: get_s(item, "refresh_token"),
        created_at: get_dt(item, "created_at")
            .ok_or_else(|| anyhow::anyhow!("session row missing created_at field"))?,
        expires_at,
    })
}

/// Parse a DDB attribute map into an `OAuthState`.
#[allow(dead_code)]
fn parse_oauth_state(item: &HashMap<String, AttributeValue>) -> Result<OAuthState> {
    // Extract csrf_state from PK (format: "OAUTH_STATE#{csrf_state}").
    let pk = get_s(item, "PK")
        .ok_or_else(|| anyhow::anyhow!("oauth_state row missing PK field"))?;
    let csrf_state = pk
        .strip_prefix("OAUTH_STATE#")
        .ok_or_else(|| anyhow::anyhow!("oauth_state PK does not start with OAUTH_STATE#"))?
        .to_string();

    Ok(OAuthState {
        csrf_state,
        pkce_verifier: get_s(item, "pkce_verifier")
            .ok_or_else(|| anyhow::anyhow!("oauth_state row missing pkce_verifier field"))?,
        provider: get_s(item, "provider")
            .ok_or_else(|| anyhow::anyhow!("oauth_state row missing provider field"))?,
        created_at: get_dt(item, "created_at")
            .ok_or_else(|| anyhow::anyhow!("oauth_state row missing created_at field"))?,
    })
}

#[allow(dead_code)]
fn parse_job_summary(item: &HashMap<String, AttributeValue>) -> Result<JobSummaryEntity> {
    // GSI1SK format: "{env}#{name}" — extract env from the composite key.
    let gsi1sk = get_s(item, "GSI1SK").unwrap_or_default();
    let env_name = gsi1sk
        .split('#')
        .next()
        .unwrap_or("unknown")
        .to_string();

    Ok(JobSummaryEntity {
        env_name,
        region_name: get_s(item, "region_name").unwrap_or_default(),
        name: get_s(item, "name")
            .ok_or_else(|| anyhow::anyhow!("job_summary row missing name field"))?,
        job_type: get_s(item, "job_type")
            .ok_or_else(|| anyhow::anyhow!("job_summary row missing job_type field"))?,
        config_yaml: get_s(item, "config_yaml"),
    })
}

#[allow(dead_code)]
fn parse_account_health(item: &HashMap<String, AttributeValue>) -> Result<AccountHealth> {
    Ok(AccountHealth {
        account_id: get_s(item, "account_id")
            .ok_or_else(|| anyhow::anyhow!("account_health row missing account_id field"))?,
        status: get_s(item, "status")
            .ok_or_else(|| anyhow::anyhow!("account_health row missing status field"))?,
        last_checked: get_dt(item, "last_checked")
            .ok_or_else(|| anyhow::anyhow!("account_health row missing last_checked field"))?,
        error_message: get_s(item, "error_message"),
    })
}
