pub mod emr;
pub mod glue;

use anyhow::{Result, anyhow};
use aws_config::BehaviorVersion;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{Resource, ResourceStatus};

/// Build a standard AWS SDK config with region and retry policy.
/// Shared by all providers and the S3 storage backend.
pub async fn aws_config(region: &str) -> aws_config::SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3))
        .load()
        .await
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
