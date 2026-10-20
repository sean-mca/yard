//! Plugin-based provider dispatch for yard job deployment (D-04, D-07).
//!
//! This module defines the [`Provider`] trait that each plugin implements,
//! plus shared infrastructure: [`aws_config()`] for building SDK configs
//! with optional `AssumeRole`, and [`validation_err()`] for constructing
//! validation errors.
//!
//! All provider types are now resolved through [`PluginProvider`] -- there
//! are no compiled-in provider implementations. Jobs without a
//! `plugin_source` field receive an actionable migration error.

use anyhow::{Context, Result, bail};
use aws_config::BehaviorVersion;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{JobDefinition, JobType, Resource, ResourceStatus, SchemaResponse, ValidationError};

use crate::config_merge::effective_job_config;
use crate::plugin_host::PluginHostConfig;
use crate::plugin_host::PluginProvider;
use crate::plugin_host::download;

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

    /// Return the config schema this provider accepts, including any
    /// extra source/sink types it supports.
    ///
    /// Used by schema-aware validation. Default returns an empty schema.
    fn schema(&self) -> Pin<Box<dyn Future<Output = Result<SchemaResponse>> + Send + '_>> {
        Box::pin(async {
            Ok(SchemaResponse {
                fields: Vec::new(),
                supported_source_types: None,
                supported_sink_types: None,
            })
        })
    }
}

/// Plugin-only provider dispatch (D-04).
///
/// Resolves the plugin binary named by `plugin_source` (downloading,
/// cloning, or building it if not already cached) and constructs a
/// [`PluginProvider`]. `plugin_version` is required for git and URL
/// sources and ignored for local `file://` sources.
///
/// When `plugin_source` is absent, returns an actionable migration error
/// (D-01) directing users to `docs/reference/migrations/v2.0.md`.
///
/// # Errors
///
/// Returns an error if:
/// - The plugin binary cannot be resolved (download, clone, or build fails)
/// - `plugin_version` is missing for a git or URL source
/// - `plugin_source` is missing (v1.x user needs to migrate)
pub async fn get_provider_for_job(
    job_type: &JobType,
    plugin_version: Option<&str>,
    plugin_source: Option<&str>,
    plugin_host_config: &PluginHostConfig,
) -> Result<Box<dyn Provider>> {
    let type_name = job_type.to_string();
    let Some(source) = plugin_source else {
        if plugin_version.is_some() {
            bail!("job has plugin_version but no plugin_source");
        }
        bail!(
            "provider '{type_name}' is now a plugin -- add plugin_source (and plugin_version) \
             to your job.yaml, or declare the pin once in a plugins.{type_name} block in \
             yard.yaml, see docs/reference/migrations/v2.0.md"
        )
    };

    let plugin_name = format!("yard-plugin-{type_name}");
    let resolved = download::ensure_plugin_cached(
        &plugin_name,
        plugin_version,
        source,
        plugin_host_config,
    )
    .await
    .with_context(|| format!("failed to resolve plugin binary for {plugin_name} from {source}"))?;

    // Only downloaded binaries are checksum-locked; local files and git
    // builds change legitimately between runs.
    let mut spawner_config = plugin_host_config.clone();
    if !resolved.locked {
        spawner_config.lock_file_path = None;
    }

    Ok(Box::new(PluginProvider::from_binary(
        resolved.path,
        plugin_name,
        spawner_config,
    )))
}

/// Resolve the provider for a job definition.
///
/// Convenience wrapper around [`get_provider_for_job`] that reads the
/// plugin fields from the job.
///
/// # Errors
///
/// See [`get_provider_for_job`].
pub async fn provider_for(
    job_def: &JobDefinition,
    plugin_host_config: &PluginHostConfig,
) -> Result<Box<dyn Provider>> {
    get_provider_for_job(
        &job_def.job_type,
        job_def.plugin_version.as_deref(),
        job_def.plugin_source.as_deref(),
        plugin_host_config,
    )
    .await
}

/// Generate a job's script through its plugin.
///
/// Returns an empty string, with a warning on stderr, when the job has no
/// `plugin_source` (nothing can generate it). A configured plugin that
/// fails to resolve or fails codegen is a hard error -- silently hashing
/// or deploying an empty script would hide the problem.
///
/// # Errors
///
/// Returns an error if the plugin cannot be resolved, the effective job
/// config cannot be built, or the plugin's codegen operation fails.
pub async fn codegen_job(
    providers: &HashMap<String, Value>,
    job_name: &str,
    job_def: &JobDefinition,
    plugin_host_config: &PluginHostConfig,
) -> Result<String> {
    if job_def.plugin_source.is_none() {
        eprintln!(
            "Warning: job \"{job_name}\" has no plugin_source -- no script can be generated for it"
        );
        return Ok(String::new());
    }

    let provider = provider_for(job_def, plugin_host_config).await?;
    let config = effective_job_config(providers, job_def)
        .with_context(|| format!("failed to build effective config for job \"{job_name}\""))?;
    let script = provider
        .codegen(job_name, &config)
        .await
        .with_context(|| format!("plugin codegen failed for job \"{job_name}\""))?;
    Ok(script.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unpinned_plugin_error_mentions_plugins_block() {
        let job_type = JobType::Plugin("glue".to_string());
        let config = PluginHostConfig::default();

        let result = get_provider_for_job(&job_type, None, None, &config).await;

        let Err(err) = result else {
            panic!("expected an error for a job without plugin_source");
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("plugins.glue block in yard.yaml"),
            "got: {msg}"
        );
        assert!(msg.contains("add plugin_source"), "got: {msg}");
    }
}
