pub mod emr;
pub mod glue;

use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{Resource, ResourceStatus, ValidationError};

/// Build a standard AWS SDK config with region and retry policy.
/// Shared by all providers and the S3 storage backend.
pub async fn aws_config(region: &str) -> aws_config::SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
        .load()
        .await
}

/// Shared S3 script operations used by all providers that upload
/// generated PySpark scripts to S3.
pub struct S3ScriptOps {
    pub s3_client: S3Client,
    pub script_bucket: String,
    pub script_prefix: String,
}

impl S3ScriptOps {
    pub fn script_key(&self, job_name: &str) -> String {
        let prefix = if self.script_prefix.ends_with('/') {
            &self.script_prefix
        } else {
            return format!("{}/{}.py", self.script_prefix, job_name);
        };
        format!("{prefix}{job_name}.py")
    }

    pub async fn upload_script(&self, job_name: &str, artifact: &str) -> Result<String> {
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

    pub async fn delete_script(&self, job_name: &str) -> Result<()> {
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

    pub async fn s3_object_exists(&self, key: &str) -> Result<bool> {
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
}

pub fn validation_err(field: &str, message: &str) -> ValidationError {
    ValidationError {
        field: field.to_string(),
        message: message.to_string(),
    }
}

/// Trait for deploying and destroying job artifacts on a target service.
///
/// Each provider (Glue, EMR, Databricks, etc.) implements this trait.
/// Provider config (deploy roles, buckets, etc.) is passed at construction time.
/// Job config (execution roles, sources, etc.) is passed per-call.
pub trait Provider: Send + Sync {
    /// Deploy a generated artifact to the target service.
    /// Returns the resources that were created/updated (for state tracking).
    fn deploy(
        &self,
        job_name: &str,
        artifact: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>>;

    /// Destroy previously deployed resources.
    fn destroy(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Verify that previously deployed resources still exist in the target service.
    /// Used by drift detection to catch out-of-band deletions.
    fn verify_resources(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceStatus>>> + Send + '_>>;
}

/// Construct a provider from the job type and its provider-level config.
pub async fn get_provider(job_type: &str, provider_config: &Value) -> Result<Box<dyn Provider>> {
    match job_type {
        "glue" => Ok(Box::new(glue::GlueProvider::new(provider_config).await?)),
        "emr" => Ok(Box::new(emr::EmrProvider::new(provider_config).await?)),
        other => Err(anyhow!("No provider for job type: {other}")),
    }
}
