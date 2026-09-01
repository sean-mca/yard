//! AWS provider implementations for yard job deployment.
//!
//! This module defines the [`Provider`] trait that each cloud service
//! (Glue, EMR, etc.) implements, plus shared infrastructure such as
//! [`S3ScriptOps`] for uploading generated PySpark scripts and
//! [`aws_config()`] for building SDK configs with optional `AssumeRole`.
//!
//! Sub-modules:
//! - [`glue`] -- AWS Glue ETL provider
//! - [`emr`]  -- AWS EMR Serverless provider

pub mod emr;
pub mod glue;

use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{JobType, Resource, ResourceStatus, SchemaField, SchemaResponse, ValidationError};

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

/// Shared S3 script operations used by all providers that upload
/// generated PySpark scripts to S3.
pub struct S3ScriptOps {
    /// The S3 client used for script upload/download/delete operations.
    pub s3_client: S3Client,
    /// The S3 bucket name where scripts are stored.
    pub script_bucket: String,
    /// The key prefix (folder path) within the bucket.
    pub script_prefix: String,
}

impl S3ScriptOps {
    /// Build the full S3 key for a job's generated PySpark script.
    #[inline]
    pub fn script_key(&self, job_name: &str) -> String {
        let prefix = if self.script_prefix.ends_with('/') {
            &self.script_prefix
        } else {
            return format!("{}/{}.py", self.script_prefix, job_name);
        };
        format!("{prefix}{job_name}.py")
    }

    /// Upload a generated PySpark script to S3 and return its `s3://` URI.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 `PutObject` call fails.
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

    /// Delete a previously uploaded PySpark script from S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 `DeleteObject` call fails.
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

    /// Check whether an S3 object exists in the script bucket.
    ///
    /// Returns `true` if the `HeadObject` call succeeds, `false` if
    /// the object is not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the `HeadObject` call fails for a reason
    /// other than "not found" (e.g. permission denied, network error).
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
/// Each provider (Glue, EMR, Databricks, etc.) implements this trait.
/// Provider config (deploy roles, buckets, etc.) is passed at construction time.
/// Job config (execution roles, sources, etc.) is passed per-call.
///
/// # Design note (D-10)
///
/// Methods return `Pin<Box<dyn Future<...> + Send + '_>>` rather than using
/// `async fn` in the trait definition. This is intentional: [`get_provider`]
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
    /// structural validation. Default returns no errors, which is
    /// correct for compiled-in providers that rely on
    /// [`validate_provider_config`] instead.
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
    /// or `None` to fall back to yard-core's built-in codegen. Default
    /// returns `None`.
    fn codegen(
        &self,
        _job_name: &str,
        _job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>>> + Send + '_>> {
        Box::pin(async { Ok(None) })
    }

    /// Return the config field descriptors this provider accepts.
    ///
    /// Used by config cascade validation (Phase 68). Default returns
    /// an empty schema.
    fn schema(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaField>>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Construct a provider from the job type and its provider-level config.
///
/// # Errors
///
/// Returns an error if:
/// - The job type is `Bash` (bash jobs are task-only and have no provider)
/// - The job type is unsupported
/// - The provider's constructor fails (e.g. missing required config fields)
pub async fn get_provider(job_type: JobType, provider_config: &Value) -> Result<Box<dyn Provider>> {
    match job_type {
        JobType::Glue => Ok(Box::new(glue::GlueProvider::new(provider_config).await?)),
        JobType::Emr => Ok(Box::new(emr::EmrProvider::new(provider_config).await?)),
        JobType::Bash => Err(anyhow!(
            "No provider for job type: {job_type} (bash is task-only — should not reach get_provider)"
        )),
        _ => Err(anyhow!("unsupported job type: {job_type}")),
    }
}

/// Plugin-aware provider dispatch for the apply create/modify path.
///
/// When `plugin_version` and `plugin_source` are both present, downloads the
/// plugin binary (if not already cached) and constructs a [`PluginProvider`].
/// Otherwise falls through to the compiled-in [`get_provider`].
///
/// Target-aware download (D-16) is achieved by design: only jobs that reach
/// the create/modify path call this function, so only the providers needed by
/// targeted jobs are downloaded.
///
/// # Errors
///
/// Returns an error if the plugin download fails, or if the compiled-in
/// provider construction fails when no plugin fields are present.
pub async fn get_provider_for_job(
    job_type: JobType,
    provider_config: &Value,
    plugin_version: Option<&str>,
    plugin_source: Option<&str>,
    plugin_host_config: &PluginHostConfig,
) -> Result<Box<dyn Provider>> {
    if let (Some(version), Some(source)) = (plugin_version, plugin_source) {
        let plugin_name = format!("yard-plugin-{job_type}");
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
    } else {
        get_provider(job_type, provider_config).await
    }
}

/// Validate a job's provider-specific config block. Dispatched from
/// `validation::rules::validate_job` — the only validation-side site
/// where `JobType` is matched in the workspace.
///
/// Note the asymmetry between the Glue and EMR arms: Glue receives the full
/// `job_config` because the Glue `role` check (in `glue::validate_config`)
/// reads from the top level of the job config, not from the inner `glue`
/// block. EMR's arm preserves the inner-block extraction since
/// `emr::validate_config` reads only from the inner `emr` block.
pub fn validate_provider_config(
    job_type: JobType,
    job_config: &Value,
    errors: &mut Vec<ValidationError>,
) {
    match job_type {
        JobType::Glue => glue::validate_config(job_config, errors),
        JobType::Emr => {
            if let Some(config) = job_config.get("emr") {
                emr::validate_config(config, errors);
            }
        }
        JobType::Bash => {}
        _ => {}
    }
}

/// Return the built-in schema for a compiled-in provider type (D-05, D-06).
///
/// Returns `Some(SchemaResponse)` for providers that ship inside yard-core
/// (Glue, EMR, Bash). Returns `None` for unknown/plugin types — the caller
/// must fetch the schema from the plugin binary in that case.
///
/// When Phase 70 extracts Glue/EMR to separate plugin repos, each plugin's
/// `schema` handler will return the same data. This function becomes a
/// fallback for the built-in-only path.
#[must_use]
pub fn built_in_schema(job_type: JobType) -> Option<SchemaResponse> {
    match job_type {
        JobType::Glue => Some(SchemaResponse {
            fields: vec![
                SchemaField {
                    name: "script_bucket".to_string(),
                    field_type: "string".to_string(),
                    required: true,
                    description: "S3 bucket for uploaded PySpark scripts".to_string(),
                },
                SchemaField {
                    name: "script_prefix".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "S3 key prefix within the script bucket".to_string(),
                },
                SchemaField {
                    name: "worker_type".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "Glue worker type (G.025X, G.1X, G.2X, G.4X, G.8X, Z.2X)".to_string(),
                },
                SchemaField {
                    name: "number_of_workers".to_string(),
                    field_type: "integer".to_string(),
                    required: false,
                    description: "Number of Glue workers (minimum 1)".to_string(),
                },
                SchemaField {
                    name: "glue_version".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "Glue ETL version (3.0, 4.0, 5.0)".to_string(),
                },
                SchemaField {
                    name: "timeout".to_string(),
                    field_type: "integer".to_string(),
                    required: false,
                    description: "Job timeout in minutes".to_string(),
                },
                SchemaField {
                    name: "max_retries".to_string(),
                    field_type: "integer".to_string(),
                    required: false,
                    description: "Maximum retry count on failure".to_string(),
                },
                SchemaField {
                    name: "max_concurrent_runs".to_string(),
                    field_type: "integer".to_string(),
                    required: false,
                    description: "Maximum concurrent job runs".to_string(),
                },
                SchemaField {
                    name: "bookmark".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "Job bookmark setting (enabled, disabled)".to_string(),
                },
                SchemaField {
                    name: "connections".to_string(),
                    field_type: "array".to_string(),
                    required: false,
                    description: "List of Glue connection names".to_string(),
                },
                SchemaField {
                    name: "default_arguments".to_string(),
                    field_type: "object".to_string(),
                    required: false,
                    description: "Default arguments map for the Glue job".to_string(),
                },
                SchemaField {
                    name: "region".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "AWS region for the Glue service".to_string(),
                },
            ],
            supported_source_types: None,
            supported_sink_types: None,
        }),
        JobType::Emr => Some(SchemaResponse {
            fields: vec![
                SchemaField {
                    name: "action_on_failure".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "Action on step failure (CONTINUE, CANCEL_AND_WAIT, TERMINATE_CLUSTER)".to_string(),
                },
                SchemaField {
                    name: "deploy_mode".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "Spark deploy mode (cluster, client)".to_string(),
                },
                SchemaField {
                    name: "script_bucket".to_string(),
                    field_type: "string".to_string(),
                    required: true,
                    description: "S3 bucket for uploaded PySpark scripts".to_string(),
                },
                SchemaField {
                    name: "cluster_id".to_string(),
                    field_type: "string".to_string(),
                    required: true,
                    description: "EMR cluster ID to submit steps to".to_string(),
                },
                SchemaField {
                    name: "script_prefix".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "S3 key prefix within the script bucket".to_string(),
                },
                SchemaField {
                    name: "region".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    description: "AWS region for the EMR service".to_string(),
                },
            ],
            supported_source_types: None,
            supported_sink_types: None,
        }),
        JobType::Bash => Some(SchemaResponse {
            fields: vec![],
            ..Default::default()
        }),
        // Unknown/plugin types: no built-in schema. The caller should fetch
        // the schema from the plugin binary (Phase 70 JobType::Plugin(String)).
        _ => None,
    }
}

/// Schema-driven provider config field validation (D-05).
///
/// Validates provider config fields against the schema's declared field list
/// and checks required fields. After schema-level checks, runs compiled-in
/// value-level validation for known provider types.
///
/// When `schema` is `None`, returns immediately — this is the structural prep
/// for Phase 70 when `JobType::Plugin(String)` makes the missing-plugin path
/// reachable (D-07).
///
/// # Note
///
/// The compiled-in `glue::validate_config` / `emr::validate_config` calls
/// are retained until Phase 70 extracts providers to separate repos.
pub fn validate_provider_config_with_schema(
    job_type: JobType,
    job_config: &Value,
    schema: Option<&SchemaResponse>,
    errors: &mut Vec<ValidationError>,
) {
    let job_type_key = job_type.to_string();

    // Schema-driven field validation (D-03, D-05): only when schema is available.
    // When schema is None (D-07: plugin not installed), skip field-level checks
    // but still run compiled-in value-level validation below.
    if let Some(schema) = schema
        && let Some(inner) = job_config.get(&job_type_key)
        && let Some(obj) = inner.as_object()
    {
        let allowed_fields: Vec<&str> =
            schema.fields.iter().map(|f| f.name.as_str()).collect();

        for key in obj.keys() {
            if !allowed_fields.contains(&key.as_str()) {
                errors.push(validation_err(
                    &format!("{job_type_key}.{key}"),
                    &format!(
                        "unknown provider config field '{}' (allowed: {})",
                        key,
                        allowed_fields.join(", ")
                    ),
                ));
            }
        }

        for field in &schema.fields {
            if field.required && inner.get(&field.name).is_none() {
                errors.push(validation_err(
                    &format!("{job_type_key}.{}", field.name),
                    &format!("required field '{}' is missing", field.name),
                ));
            }
        }
    }

    // Compiled-in value-level validation for known types.
    // Runs regardless of schema availability so existing tests and
    // schema-less callers still get value-level checks.
    // Phase 70 removes this dispatch when providers are extracted to plugin repos.
    match job_type {
        JobType::Glue => glue::validate_config(job_config, errors),
        JobType::Emr => {
            if let Some(config) = job_config.get("emr") {
                emr::validate_config(config, errors);
            }
        }
        JobType::Bash => {}
        _ => {}
    }
}
