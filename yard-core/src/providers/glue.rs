use super::{Provider, S3ScriptOps, aws_config, validation_err};
use anyhow::{Context, Result, anyhow};
use aws_sdk_glue::Client as GlueClient;
use aws_sdk_s3::Client as S3Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{Resource, ResourceStatus, ValidationError};

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_script_prefix() -> String {
    "yard-scripts/".to_string()
}

fn default_glue_version() -> String {
    "4.0".to_string()
}

fn default_worker_type() -> String {
    "G.1X".to_string()
}

fn default_number_of_workers() -> i32 {
    2
}

#[derive(Deserialize)]
struct GlueRawConfig {
    #[serde(default = "default_region")]
    region: String,
    #[serde(default = "default_script_prefix")]
    script_prefix: String,
    #[serde(default = "default_glue_version")]
    glue_version: String,
    #[serde(default = "default_worker_type")]
    worker_type: String,
    #[serde(default = "default_number_of_workers")]
    number_of_workers: i32,
    #[serde(default)]
    timeout: Option<i32>,
    #[serde(default)]
    max_retries: Option<i32>,
    #[serde(default)]
    max_concurrent_runs: Option<i32>,
    #[serde(default)]
    bookmark: Option<String>,
    #[serde(default)]
    connections: Vec<String>,
    #[serde(default)]
    default_arguments: HashMap<String, String>,
    #[serde(default, rename = "_aws")]
    aws: Option<serde_json::Value>,
}

pub struct GlueProvider {
    glue_client: GlueClient,
    s3: S3ScriptOps,
    // Runtime defaults (from merged provider + job config)
    glue_version: String,
    worker_type: String,
    number_of_workers: i32,
    timeout: Option<i32>,
    max_retries: Option<i32>,
    max_concurrent_runs: Option<i32>,
    bookmark: Option<String>,
    connections: Vec<String>,
    default_arguments: HashMap<String, String>,
}

impl GlueProvider {
    pub async fn new(config: &Value) -> Result<Self> {
        // Pre-extract script_bucket BEFORE serde deserialization so the
        // legacy error string survives byte-for-byte (SC #4, D-06 option a).
        let script_bucket = config
            .get("script_bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.glue.script_bucket is required"))?
            .to_string();

        let cfg: GlueRawConfig = serde_json::from_value(config.clone())
            .context("invalid providers.glue config")?;

        let sdk_config = aws_config(&cfg.region, cfg.aws.as_ref()).await;
        let glue_client = GlueClient::new(&sdk_config);
        let s3_client = S3Client::new(&sdk_config);

        Ok(Self {
            glue_client,
            s3: S3ScriptOps {
                s3_client,
                script_bucket,
                script_prefix: cfg.script_prefix,
            },
            glue_version: cfg.glue_version,
            worker_type: cfg.worker_type,
            number_of_workers: cfg.number_of_workers,
            timeout: cfg.timeout,
            max_retries: cfg.max_retries,
            max_concurrent_runs: cfg.max_concurrent_runs,
            bookmark: cfg.bookmark,
            connections: cfg.connections,
            default_arguments: cfg.default_arguments,
        })
    }

    fn build_default_arguments(&self) -> HashMap<String, String> {
        let mut args = self.default_arguments.clone();

        // Enable Iceberg libraries on every Glue job — yard is Iceberg-first.
        // User-supplied default_arguments may still override this explicitly.
        args.entry("--datalake-formats".to_string())
            .or_insert_with(|| "iceberg".to_string());

        // Wire bookmark setting into default arguments
        if let Some(ref bookmark) = self.bookmark {
            let enabled = matches!(bookmark.as_str(), "enabled" | "true");
            args.insert(
                "--job-bookmark-option".to_string(),
                if enabled {
                    "job-bookmark-enable".to_string()
                } else {
                    "job-bookmark-disable".to_string()
                },
            );
        }

        args
    }

    async fn create_or_update_glue_job(
        &self,
        job_name: &str,
        script_location: &str,
        job_config: &Value,
    ) -> Result<()> {
        let execution_role = job_config
            .get("role")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow!("Job \"{job_name}\" requires a \"role\" (Glue execution role)")
            })?;

        let command = aws_sdk_glue::types::JobCommand::builder()
            .name("glueetl")
            .script_location(script_location)
            .python_version("3")
            .build();

        let default_args = self.build_default_arguments();

        // Build the update first, fall back to create if job doesn't exist
        let mut update_builder = aws_sdk_glue::types::JobUpdate::builder()
            .role(execution_role)
            .command(command.clone())
            .glue_version(&self.glue_version)
            .worker_type(aws_sdk_glue::types::WorkerType::from(
                self.worker_type.as_str(),
            ))
            .number_of_workers(self.number_of_workers);

        if let Some(timeout) = self.timeout {
            update_builder = update_builder.timeout(timeout);
        }
        if let Some(max_retries) = self.max_retries {
            update_builder = update_builder.max_retries(max_retries);
        }
        if let Some(max_concurrent) = self.max_concurrent_runs {
            update_builder = update_builder.execution_property(
                aws_sdk_glue::types::ExecutionProperty::builder()
                    .max_concurrent_runs(max_concurrent)
                    .build(),
            );
        }
        if !self.connections.is_empty() {
            update_builder = update_builder.connections(
                aws_sdk_glue::types::ConnectionsList::builder()
                    .set_connections(Some(self.connections.clone()))
                    .build(),
            );
        }
        for (k, v) in &default_args {
            update_builder = update_builder.default_arguments(k.clone(), v.clone());
        }

        let update_result = self
            .glue_client
            .update_job()
            .job_name(job_name)
            .job_update(update_builder.build())
            .send()
            .await;

        match update_result {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|se| se.is_entity_not_found_exception())
                {
                    let mut create_builder = self
                        .glue_client
                        .create_job()
                        .name(job_name)
                        .role(execution_role)
                        .command(command)
                        .glue_version(&self.glue_version)
                        .worker_type(aws_sdk_glue::types::WorkerType::from(
                            self.worker_type.as_str(),
                        ))
                        .number_of_workers(self.number_of_workers);

                    if let Some(timeout) = self.timeout {
                        create_builder = create_builder.timeout(timeout);
                    }
                    if let Some(max_retries) = self.max_retries {
                        create_builder = create_builder.max_retries(max_retries);
                    }
                    if let Some(max_concurrent) = self.max_concurrent_runs {
                        create_builder = create_builder.execution_property(
                            aws_sdk_glue::types::ExecutionProperty::builder()
                                .max_concurrent_runs(max_concurrent)
                                .build(),
                        );
                    }
                    if !self.connections.is_empty() {
                        create_builder = create_builder.connections(
                            aws_sdk_glue::types::ConnectionsList::builder()
                                .set_connections(Some(self.connections.clone()))
                                .build(),
                        );
                    }
                    for (k, v) in &default_args {
                        create_builder = create_builder.default_arguments(k.clone(), v.clone());
                    }

                    create_builder
                        .send()
                        .await
                        .with_context(|| format!("Failed to create Glue job \"{job_name}\""))?;
                    Ok(())
                } else {
                    Err(e).with_context(|| format!("Failed to update Glue job \"{job_name}\""))
                }
            }
        }
    }

    async fn delete_glue_job(&self, job_name: &str) -> Result<()> {
        self.glue_client
            .delete_job()
            .job_name(job_name)
            .send()
            .await
            .with_context(|| format!("Failed to delete Glue job \"{job_name}\""))?;

        Ok(())
    }

    async fn glue_job_exists(&self, job_name: &str) -> Result<bool> {
        let result = self
            .glue_client
            .get_job()
            .job_name(job_name)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|se| se.is_entity_not_found_exception())
                {
                    Ok(false)
                } else {
                    Err(e).with_context(|| format!("Failed to check Glue job: {job_name}"))
                }
            }
        }
    }
}

impl Provider for GlueProvider {
    fn deploy(
        &self,
        job_name: &str,
        artifact: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>> {
        let job_name = job_name.to_string();
        let artifact = artifact.to_string();
        let job_config = job_config.clone();

        Box::pin(async move {
            let script_location = self.s3.upload_script(&job_name, &artifact).await?;

            self.create_or_update_glue_job(&job_name, &script_location, &job_config)
                .await?;

            Ok(vec![
                Resource {
                    r#type: "s3_object".to_string(),
                    id: script_location,
                    provider: "glue".to_string(),
                },
                Resource {
                    r#type: "glue_job".to_string(),
                    id: job_name,
                    provider: "glue".to_string(),
                },
            ])
        })
    }

    fn destroy(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        let resources = resources.to_vec();

        Box::pin(async move {
            for resource in &resources {
                if resource.r#type == "glue_job" {
                    self.delete_glue_job(&resource.id).await?;
                }
            }

            self.s3.delete_script(&job_name).await?;

            Ok(())
        })
    }

    fn verify_resources(
        &self,
        _job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceStatus>>> + Send + '_>> {
        let resources = resources.to_vec();

        Box::pin(async move {
            let mut statuses = Vec::new();

            for resource in &resources {
                let exists = match resource.r#type.as_str() {
                    "s3_object" => {
                        // resource.id is "s3://bucket/key" — extract the key
                        let key = resource
                            .id
                            .strip_prefix(&format!("s3://{}/", self.s3.script_bucket))
                            .unwrap_or(&resource.id);
                        self.s3.s3_object_exists(key).await?
                    }
                    "glue_job" => self.glue_job_exists(&resource.id).await?,
                    _ => true, // Unknown resource types assumed to exist
                };

                statuses.push(ResourceStatus {
                    resource: resource.clone(),
                    exists,
                });
            }

            Ok(statuses)
        })
    }
}

// ---- Validation ----

const VALID_WORKER_TYPES: &[&str] = &["G.025X", "G.1X", "G.2X", "G.4X", "G.8X", "Z.2X"];
const VALID_BOOKMARK_VALUES: &[&str] = &["enabled", "disabled"];

pub fn validate_config(config: &serde_json::Value, errors: &mut Vec<ValidationError>) {
    if let Some(wt) = config.get("worker_type").and_then(|v| v.as_str())
        && !VALID_WORKER_TYPES.contains(&wt)
    {
        errors.push(validation_err(
            "glue.worker_type",
            &format!(
                "\"{}\" is not a valid worker type (expected: {})",
                wt,
                VALID_WORKER_TYPES.join(", ")
            ),
        ));
    }

    if let Some(n) = config.get("number_of_workers").and_then(|v| v.as_i64())
        && n < 1
    {
        errors.push(validation_err(
            "glue.number_of_workers",
            "must be at least 1",
        ));
    }

    if let Some(v) = config.get("glue_version").and_then(|v| v.as_str())
        && !["3.0", "4.0", "5.0"].contains(&v)
    {
        errors.push(validation_err(
            "glue.glue_version",
            &format!(
                "\"{}\" is not a supported Glue version (expected: 3.0, 4.0)",
                v
            ),
        ));
    }

    if let Some(t) = config.get("timeout").and_then(|v| v.as_i64())
        && t < 1
    {
        errors.push(validation_err(
            "glue.timeout",
            "must be at least 1 (minutes)",
        ));
    }

    if let Some(r) = config.get("max_retries").and_then(|v| v.as_i64())
        && r < 0
    {
        errors.push(validation_err("glue.max_retries", "cannot be negative"));
    }

    if let Some(b) = config.get("bookmark").and_then(|v| v.as_str())
        && !VALID_BOOKMARK_VALUES.contains(&b)
    {
        errors.push(validation_err(
            "glue.bookmark",
            &format!(
                "\"{}\" is not valid (expected: {})",
                b,
                VALID_BOOKMARK_VALUES.join(", ")
            ),
        ));
    }

    if let Some(conns) = config.get("connections")
        && !conns.is_array()
    {
        errors.push(validation_err(
            "glue.connections",
            "must be an array of strings",
        ));
    }

    if let Some(args) = config.get("default_arguments")
        && !args.is_object()
    {
        errors.push(validation_err(
            "glue.default_arguments",
            "must be a map of string keys to string values",
        ));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn missing_script_bucket_preserves_legacy_error_string() {
        let config = json!({
            "region": "us-east-1",
            // script_bucket intentionally omitted
        });
        // GlueProvider does not implement Debug, so we can't use unwrap_err();
        // match the result instead to extract the error.
        let err = match GlueProvider::new(&config).await {
            Ok(_) => panic!("expected error for missing script_bucket"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("providers.glue.script_bucket is required"),
            "expected legacy error string, got: {msg}"
        );
    }

    #[tokio::test]
    async fn type_confused_script_bucket_preserves_legacy_error_string() {
        let config = json!({
            "region": "us-east-1",
            "script_bucket": 42, // type-confused — pre-extraction's .as_str() returns None
        });
        let err = match GlueProvider::new(&config).await {
            Ok(_) => panic!("expected error for type-confused script_bucket"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("providers.glue.script_bucket is required"),
            "expected legacy error string, got: {msg}"
        );
    }

    #[tokio::test]
    async fn unknown_field_silently_ignored() {
        // D-15: typos like `glue_versoin` are silently ignored — no deny_unknown_fields.
        let config = json!({
            "region": "us-east-1",
            "script_bucket": "my-bucket",
            "glue_versoin": "4.0", // typo — must NOT cause an error
        });
        // Note: this will still try to construct an SDK client; we only assert
        // that deserialization itself doesn't reject the unknown key. If the
        // SDK init succeeds (depends on test environment), the call returns Ok;
        // if SDK init fails for environmental reasons, the error must NOT be
        // about the unknown field.
        let result = GlueProvider::new(&config).await;
        if let Err(e) = result {
            let msg = format!("{e:#}");
            assert!(
                !msg.contains("glue_versoin")
                    && !msg.contains("unknown field")
                    && !msg.contains("invalid providers.glue config"),
                "unknown field must be silently ignored, got: {msg}"
            );
        }
    }
}
