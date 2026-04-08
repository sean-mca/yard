use super::Provider;
use anyhow::{Context, Result, anyhow};
use yard_structs::ValidationError;
use aws_config::BehaviorVersion;
use aws_sdk_glue::Client as GlueClient;
use aws_sdk_s3::Client as S3Client;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{Resource, ResourceStatus};

pub struct GlueProvider {
    glue_client: GlueClient,
    s3_client: S3Client,
    script_bucket: String,
    script_prefix: String,
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
        let region = config
            .get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");

        let script_bucket = config
            .get("script_bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.glue.script_bucket is required"))?
            .to_string();

        let script_prefix = config
            .get("script_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("yard-scripts/")
            .to_string();

        let glue_version = config
            .get("glue_version")
            .and_then(|v| v.as_str())
            .unwrap_or("4.0")
            .to_string();

        let worker_type = config
            .get("worker_type")
            .and_then(|v| v.as_str())
            .unwrap_or("G.1X")
            .to_string();

        let number_of_workers = config
            .get("number_of_workers")
            .and_then(|v| v.as_i64())
            .unwrap_or(2) as i32;

        let timeout = config
            .get("timeout")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let max_retries = config
            .get("max_retries")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let max_concurrent_runs = config
            .get("max_concurrent_runs")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32);

        let bookmark = config
            .get("bookmark")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let connections = config
            .get("connections")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let default_arguments = config
            .get("default_arguments")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;

        let glue_client = GlueClient::new(&aws_config);
        let s3_client = S3Client::new(&aws_config);

        Ok(Self {
            glue_client,
            s3_client,
            script_bucket,
            script_prefix,
            glue_version,
            worker_type,
            number_of_workers,
            timeout,
            max_retries,
            max_concurrent_runs,
            bookmark,
            connections,
            default_arguments,
        })
    }

    fn script_key(&self, job_name: &str) -> String {
        let prefix = if self.script_prefix.ends_with('/') {
            &self.script_prefix
        } else {
            return format!("{}/{}.py", self.script_prefix, job_name);
        };
        format!("{prefix}{job_name}.py")
    }

    async fn upload_script(&self, job_name: &str, artifact: &str) -> Result<String> {
        let key = self.script_key(job_name);

        self.s3_client
            .put_object()
            .bucket(&self.script_bucket)
            .key(&key)
            .body(artifact.as_bytes().to_vec().into())
            .content_type("text/x-python")
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to upload script to s3://{}/{}",
                    self.script_bucket, key
                )
            })?;

        Ok(format!("s3://{}/{}", self.script_bucket, key))
    }

    async fn delete_script(&self, job_name: &str) -> Result<()> {
        let key = self.script_key(job_name);

        self.s3_client
            .delete_object()
            .bucket(&self.script_bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to delete script at s3://{}/{}",
                    self.script_bucket, key
                )
            })?;

        Ok(())
    }

    fn build_default_arguments(&self) -> HashMap<String, String> {
        let mut args = self.default_arguments.clone();

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

    async fn s3_object_exists(&self, key: &str) -> Result<bool> {
        let result = self
            .s3_client
            .head_object()
            .bucket(&self.script_bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.as_service_error()
                    .is_some_and(|se| se.is_not_found())
                {
                    Ok(false)
                } else {
                    Err(e).with_context(|| format!("Failed to check S3 object: {key}"))
                }
            }
        }
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
            let script_location = self.upload_script(&job_name, &artifact).await?;

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

            self.delete_script(&job_name).await?;

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
                            .strip_prefix(&format!("s3://{}/", self.script_bucket))
                            .unwrap_or(&resource.id);
                        self.s3_object_exists(key).await?
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

fn validation_err(field: &str, message: &str) -> ValidationError {
    ValidationError {
        field: field.to_string(),
        message: message.to_string(),
    }
}

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
        && !["3.0", "4.0"].contains(&v)
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
