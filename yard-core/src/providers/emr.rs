use super::{Provider, S3ScriptOps, aws_config, validation_err};
use anyhow::{Context, Result, anyhow};
use aws_sdk_emr::Client as EmrClient;
use aws_sdk_s3::Client as S3Client;
use serde::Deserialize;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::{Resource, ResourceStatus, ValidationError};

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_script_prefix() -> String {
    "yard-scripts/".to_string()
}

fn default_deploy_mode() -> String {
    "cluster".to_string()
}

fn default_action_on_failure() -> String {
    "CONTINUE".to_string()
}

#[derive(Deserialize)]
struct EmrRawConfig {
    #[serde(default = "default_region")]
    region: String,
    #[serde(default = "default_script_prefix")]
    script_prefix: String,
    #[serde(default = "default_deploy_mode")]
    deploy_mode: String,
    #[serde(default = "default_action_on_failure")]
    action_on_failure: String,
    #[serde(default, rename = "_aws")]
    aws: Option<serde_json::Value>,
}

pub struct EmrProvider {
    emr_client: EmrClient,
    s3: S3ScriptOps,
    cluster_id: String,
    deploy_mode: String,
    action_on_failure: String,
}

impl EmrProvider {
    pub async fn new(config: &Value) -> Result<Self> {
        // Pre-extract script_bucket and cluster_id BEFORE serde deserialization
        // so the legacy error strings survive byte-for-byte (SC #4, D-06 option a).
        let script_bucket = config
            .get("script_bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.emr.script_bucket is required"))?
            .to_string();

        let cluster_id = config
            .get("cluster_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.emr.cluster_id is required"))?
            .to_string();

        let cfg: EmrRawConfig = serde_json::from_value(config.clone())
            .context("invalid providers.emr config")?;

        let sdk_config = aws_config(&cfg.region, cfg.aws.as_ref()).await;
        let emr_client = EmrClient::new(&sdk_config);
        let s3_client = S3Client::new(&sdk_config);

        Ok(Self {
            emr_client,
            s3: S3ScriptOps {
                s3_client,
                script_bucket,
                script_prefix: cfg.script_prefix,
            },
            cluster_id,
            deploy_mode: cfg.deploy_mode,
            action_on_failure: cfg.action_on_failure,
        })
    }

    async fn submit_step(
        &self,
        job_name: &str,
        script_location: &str,
    ) -> Result<String> {
        let step = aws_sdk_emr::types::StepConfig::builder()
            .name(job_name)
            .action_on_failure({
                match self.action_on_failure
                    .parse::<aws_sdk_emr::types::ActionOnFailure>()
                {
                    Ok(aof) => aof,
                    Err(_) => {
                        eprintln!(
                            "Warning: invalid action_on_failure '{}' for job '{}', \
                             using CONTINUE. Fix the emr config.",
                            self.action_on_failure, job_name
                        );
                        aws_sdk_emr::types::ActionOnFailure::Continue
                    }
                }
            })
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
            let script_location = self.s3.upload_script(&job_name, &artifact).await?;
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
                        let key = resource
                            .id
                            .strip_prefix(&format!("s3://{}/", self.s3.script_bucket))
                            .unwrap_or(&resource.id);
                        self.s3.s3_object_exists(key).await?
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

pub(crate) fn validate_config(config: &Value, errors: &mut Vec<ValidationError>) {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn missing_cluster_id_preserves_legacy_error_string() {
        let config = json!({
            "region": "us-east-1",
            "script_bucket": "my-bucket",
            // cluster_id intentionally omitted
        });
        // EmrProvider does not implement Debug, so we can't use unwrap_err();
        // match the result instead to extract the error.
        let err = match EmrProvider::new(&config).await {
            Ok(_) => panic!("expected error for missing cluster_id"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("providers.emr.cluster_id is required"),
            "expected legacy error string, got: {msg}"
        );
    }

    #[tokio::test]
    async fn missing_script_bucket_preserves_legacy_error_string() {
        let config = json!({
            "region": "us-east-1",
            "cluster_id": "j-test",
            // script_bucket intentionally omitted
        });
        let err = match EmrProvider::new(&config).await {
            Ok(_) => panic!("expected error for missing script_bucket"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("providers.emr.script_bucket is required"),
            "expected legacy error string, got: {msg}"
        );
    }

    #[tokio::test]
    async fn unknown_field_silently_ignored() {
        // D-15: typos are silently ignored — no deny_unknown_fields.
        let config = json!({
            "region": "us-east-1",
            "script_bucket": "my-bucket",
            "cluster_id": "j-test",
            "cluster_idd": "j-typo", // typo — must NOT cause a deserialize error
        });
        let result = EmrProvider::new(&config).await;
        if let Err(e) = result {
            let msg = format!("{e:#}");
            assert!(
                !msg.contains("cluster_idd")
                    && !msg.contains("unknown field")
                    && !msg.contains("invalid providers.emr config"),
                "unknown field must be silently ignored, got: {msg}"
            );
        }
    }
}
