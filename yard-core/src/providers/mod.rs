//! AWS provider implementations for yard job deployment.
//!
//! This module defines the [`Provider`] trait that each cloud service
//! (Glue, EMR, etc.) implements, plus shared infrastructure such as
//! [`S3ScriptOps`] for uploading generated PySpark scripts and
//! [`aws_config`] for building SDK configs with optional `AssumeRole`.
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
use yard_structs::{JobType, Resource, ResourceStatus, ValidationError};

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
    /// the object is not found, or an error for other failures.
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
