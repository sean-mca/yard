pub mod glue;

use anyhow::{Result, anyhow};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::Resource;

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
}

/// Construct a provider from the job type and its provider-level config.
pub async fn get_provider(
    job_type: &str,
    provider_config: &Value,
) -> Result<Box<dyn Provider>> {
    match job_type {
        "glue" => Ok(Box::new(glue::GlueProvider::new(provider_config).await?)),
        other => Err(anyhow!("No provider for job type: {other}")),
    }
}
