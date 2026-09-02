//! Plugin-based provider dispatch for yard job deployment (D-04, D-07).
//!
//! This module defines the [`Provider`] trait that each plugin implements,
//! plus shared infrastructure: [`aws_config()`] for building SDK configs
//! with optional `AssumeRole`, and [`validation_err()`] for constructing
//! validation errors.
//!
//! All provider types are now resolved through [`PluginProvider`] -- there
//! are no compiled-in provider implementations. Jobs without `plugin_version`
//! and `plugin_source` fields receive an actionable migration error.

use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{JobType, Resource, ResourceStatus, SchemaField, ValidationError};

use crate::plugin_host::download;
use crate::plugin_host::PluginHostConfig;
use crate::plugin_host::PluginProvider;

/// Build a standard AWS SDK config with region, retry policy, and optional
/// STS `AssumeRole` wrapped around the default credential provider chain.
///
/// Resolution of AssumeRole params (env vars beat yaml so CI can override):
///   `YARD_AWS_ASSUME_ROLE`  → yaml `assume_role`
///   `YARD_AWS_SESSION_NAME` → yaml `session_name` (default "yard")
///   `YARD_AWS_EXTERNAL_ID`  → yaml `external_id`
///
/// When no role is configured, falls through to the default provider chain
/// (env vars, shared config, IMDS/ECS task role, SSO). This preserves the
/// current behavior for users who don't set an `aws:` block.
pub async fn aws_config(region: &str, aws_cfg: Option<&Value>) -> aws_config::SdkConfig {
    let region_obj = aws_config::Region::new(region.to_string());
    let base = aws_config::defaults(BehaviorVersion::latest())
        .region(region_obj.clone())
        .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(3));

    let yaml_str = |key: &str| {
        aws_cfg
            .and_then(|v| v.get(key))
            .and_then(|v| v.as_str())
            .map(String::from)
    };

    let assume_role = std::env::var("YARD_AWS_ASSUME_ROLE")
        .ok()
        .or_else(|| yaml_str("assume_role"));

    if let Some(role_arn) = assume_role {
        let session_name = std::env::var("YARD_AWS_SESSION_NAME")
            .ok()
            .or_else(|| yaml_str("session_name"))
            .unwrap_or_else(|| "yard".to_string());
        let external_id = std::env::var("YARD_AWS_EXTERNAL_ID")
            .ok()
            .or_else(|| yaml_str("external_id"));

        let mut builder = aws_config::sts::AssumeRoleProvider::builder(role_arn)
            .session_name(session_name)
            .region(region_obj);
        if let Some(eid) = external_id {
            builder = builder.external_id(eid);
        }
        let provider = builder.build().await;
        return base.credentials_provider(provider).load().await;
    }

    base.load().await
}

/// Construct a [`ValidationError`] with the given field name and message.
///
/// This is the single canonical constructor shared by all provider and
/// validation modules.
#[must_use]
pub fn validation_err(field: &str, message: &str) -> ValidationError {
    ValidationError {
        field: field.to_string(),
        message: message.to_string(),
    }
}

/// Trait for deploying and destroying job artifacts on a target service.
///
/// Each provider plugin implements this trait via [`PluginProvider`].
/// Provider config (deploy roles, buckets, etc.) is passed at construction time.
/// Job config (execution roles, sources, etc.) is passed per-call.
///
/// # Design note (D-10)
///
/// Methods return `Pin<Box<dyn Future<...> + Send + '_>>` rather than using
/// `async fn` in the trait definition. This is intentional: [`get_provider_for_job`]
/// returns `Box<dyn Provider>`, which requires object safety. Native `async fn`
/// in traits produces `impl Future` return types that are **not** object-safe,
/// so this desugared form is the correct stdlib-only pattern for async +
/// dynamic dispatch.
pub trait Provider: Send + Sync {
    /// Deploy a generated artifact to the target service.
    /// Returns the resources that were created/updated (for state tracking).
    ///
    /// # Errors
    ///
    /// Returns an error if the artifact upload or service-side job
    /// creation/update fails.
    fn deploy(
        &self,
        job_name: &str,
        artifact: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>>;

    /// Destroy previously deployed resources.
    ///
    /// # Errors
    ///
    /// Returns an error if the service-side deletion or S3 script
    /// cleanup fails.
    fn destroy(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;

    /// Verify that previously deployed resources still exist in the target service.
    /// Used by drift detection to catch out-of-band deletions.
    ///
    /// # Errors
    ///
    /// Returns an error if the service-side existence check fails
    /// (as opposed to returning `exists: false` for missing resources).
    fn verify_resources(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceStatus>>> + Send + '_>>;

    /// Run provider-specific validation on the job config.
    ///
    /// Returns additional validation errors to append to yard-core's
    /// structural validation. Default returns no errors.
    fn validate(
        &self,
        _job_name: &str,
        _job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ValidationError>>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Generate the deployment script for a job.
    ///
    /// Returns `Some(script_content)` if the provider handles codegen,
    /// or `None` if no script is needed. Default returns `None`.
    fn codegen(
        &self,
        _job_name: &str,
        _job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>>> + Send + '_>> {
        Box::pin(async { Ok(None) })
    }

    /// Return the config field descriptors this provider accepts.
    ///
    /// Used by config cascade validation. Default returns an empty schema.
    fn schema(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaField>>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Plugin-only provider dispatch (D-04).
///
/// Downloads the plugin binary (if not already cached) and constructs a
/// [`PluginProvider`]. When `plugin_version` and `plugin_source` are both
/// absent, returns an actionable migration error (D-01) directing users
/// to `docs/reference/migrations/v2.0.md`.
///
/// # Errors
///
/// Returns an error if:
/// - Plugin download fails
/// - Only one of `plugin_version` / `plugin_source` is set (both required)
/// - Neither is set (v1.x user needs to migrate)
pub async fn get_provider_for_job(
    job_type: &JobType,
    _provider_config: &Value,
    plugin_version: Option<&str>,
    plugin_source: Option<&str>,
    plugin_host_config: &PluginHostConfig,
) -> Result<Box<dyn Provider>> {
    let JobType::Plugin(type_name) = job_type;
    match (plugin_version, plugin_source) {
        (Some(version), Some(source)) => {
            let plugin_name = format!("yard-plugin-{type_name}");
            let binary_path =
                download::ensure_plugin_cached(&plugin_name, version, source, plugin_host_config)
                    .await
                    .with_context(|| {
                        format!("failed to ensure plugin binary for {plugin_name} v{version}")
                    })?;

            Ok(Box::new(PluginProvider::from_binary(
                binary_path,
                plugin_name,
                plugin_host_config.clone(),
            )))
        }
        (Some(_), None) => {
            bail!("job has plugin_version but no plugin_source -- both are required")
        }
        (None, Some(_)) => {
            bail!("job has plugin_source but no plugin_version -- both are required")
        }
        (None, None) => {
            bail!(
                "provider '{type_name}' is now a plugin -- add plugin_version and plugin_source \
                 to your job.yaml, see docs/reference/migrations/v2.0.md"
            )
        }
    }
}
