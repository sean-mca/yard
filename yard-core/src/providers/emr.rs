use super::Provider;
use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_emr::Client as EmrClient;
use aws_sdk_s3::Client as S3Client;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{Resource, ResourceStatus, ValidationError};

pub struct EmrProvider {
    emr_client: EmrClient,
    s3_client: S3Client,
    script_bucket: String,
    script_prefix: String,
    cluster_id: String,
    deploy_mode: String,
    action_on_failure: String,
}

impl EmrProvider {
    pub async fn new(config: &Value) -> Result<Self> {
        let region = config
            .get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");

        let script_bucket = config
            .get("script_bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.emr.script_bucket is required"))?
            .to_string();

        let script_prefix = config
            .get("script_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("yard-scripts/")
            .to_string();

        let cluster_id = config
            .get("cluster_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.emr.cluster_id is required"))?
            .to_string();

        let deploy_mode = config
            .get("deploy_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("cluster")
            .to_string();

        let action_on_failure = config
            .get("action_on_failure")
            .and_then(|v| v.as_str())
            .unwrap_or("CONTINUE")
            .to_string();

        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;

        let emr_client = EmrClient::new(&aws_config);
        let s3_client = S3Client::new(&aws_config);

        Ok(Self {
            emr_client,
            s3_client,
            script_bucket,
            script_prefix,
            cluster_id,
            deploy_mode,
            action_on_failure,
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

    async fn submit_step(
        &self,
        job_name: &str,
        script_location: &str,
    ) -> Result<String> {
        let step = aws_sdk_emr::types::StepConfig::builder()
            .name(job_name)
            .action_on_failure(
                self.action_on_failure
                    .parse::<aws_sdk_emr::types::ActionOnFailure>()
                    .unwrap_or(aws_sdk_emr::types::ActionOnFailure::Continue),
            )
            .hadoop_jar_step(
                aws_sdk_emr::types::HadoopJarStepConfig::builder()
                    .jar("command-runner.jar")
                    .args("spark-submit")
                    .args("--deploy-mode")
                    .args(&self.deploy_mode)
                    .args(script_location)
                    .build(),
            )
            .build();

        let resp = self
            .emr_client
            .add_job_flow_steps()
            .job_flow_id(&self.cluster_id)
            .steps(step)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to submit step \"{}\" to cluster {}",
                    job_name, self.cluster_id
                )
            })?;

        let step_id = resp
            .step_ids()
            .first()
            .ok_or_else(|| anyhow!("No step ID returned from EMR"))?
            .clone();

        Ok(step_id)
    }

    async fn cancel_step(&self, step_id: &str) -> Result<()> {
        // Best effort — step may have already completed
        let _ = self
            .emr_client
            .cancel_steps()
            .cluster_id(&self.cluster_id)
            .step_ids(step_id)
            .send()
            .await;

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
}

impl Provider for EmrProvider {
    fn deploy(
        &self,
        job_name: &str,
        artifact: &str,
        _job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>> {
        let job_name = job_name.to_string();
        let artifact = artifact.to_string();

        Box::pin(async move {
            let script_location = self.upload_script(&job_name, &artifact).await?;
            let step_id = self.submit_step(&job_name, &script_location).await?;

            Ok(vec![
                Resource {
                    r#type: "s3_object".to_string(),
                    id: script_location,
                    provider: "emr".to_string(),
                },
                Resource {
                    r#type: "emr_step".to_string(),
                    id: step_id,
                    provider: "emr".to_string(),
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
            // Cancel any pending steps
            for resource in &resources {
                if resource.r#type == "emr_step" {
                    self.cancel_step(&resource.id).await?;
                }
            }

            // Delete the script from S3
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
                        let key = resource
                            .id
                            .strip_prefix(&format!("s3://{}/", self.script_bucket))
                            .unwrap_or(&resource.id);
                        self.s3_object_exists(key).await?
                    }
                    // EMR steps are ephemeral — skip verification
                    "emr_step" => true,
                    _ => true,
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

const VALID_ACTION_ON_FAILURE: &[&str] =
    &["CONTINUE", "CANCEL_AND_WAIT", "TERMINATE_CLUSTER"];

fn validation_err(field: &str, message: &str) -> ValidationError {
    ValidationError {
        field: field.to_string(),
        message: message.to_string(),
    }
}

pub fn validate_config(config: &Value, errors: &mut Vec<ValidationError>) {
    if let Some(aof) = config.get("action_on_failure").and_then(|v| v.as_str())
        && !VALID_ACTION_ON_FAILURE.contains(&aof)
    {
        errors.push(validation_err(
            "emr.action_on_failure",
            &format!(
                "\"{}\" is not valid (expected: {})",
                aof,
                VALID_ACTION_ON_FAILURE.join(", ")
            ),
        ));
    }

    if let Some(dm) = config.get("deploy_mode").and_then(|v| v.as_str())
        && !["cluster", "client"].contains(&dm)
    {
        errors.push(validation_err(
            "emr.deploy_mode",
            &format!("\"{}\" is not valid (expected: cluster, client)", dm),
        ));
    }
}
