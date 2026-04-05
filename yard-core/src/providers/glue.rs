use super::Provider;
use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_glue::Client as GlueClient;
use aws_sdk_s3::Client as S3Client;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use yard_structs::Resource;

pub struct GlueProvider {
    glue_client: GlueClient,
    s3_client: S3Client,
    script_bucket: String,
    script_prefix: String,
    deploy_role: Option<String>,
}

impl GlueProvider {
    pub async fn new(provider_config: &Value) -> Result<Self> {
        let region = provider_config
            .get("region")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1");

        let script_bucket = provider_config
            .get("script_bucket")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("providers.glue.script_bucket is required"))?
            .to_string();

        let script_prefix = provider_config
            .get("script_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("yard-scripts/")
            .to_string();

        let deploy_role = provider_config
            .get("role")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;

        let glue_client = GlueClient::new(&config);
        let s3_client = S3Client::new(&config);

        Ok(Self {
            glue_client,
            s3_client,
            script_bucket,
            script_prefix,
            deploy_role,
        })
    }

    fn script_key(&self, job_name: &str) -> String {
        let prefix = if self.script_prefix.ends_with('/') {
            &self.script_prefix
        } else {
            // shouldn't happen given the default, but be safe
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
            .with_context(|| format!("Failed to upload script to s3://{}/{}", self.script_bucket, key))?;

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
            .with_context(|| format!("Failed to delete script at s3://{}/{}", self.script_bucket, key))?;

        Ok(())
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
            .ok_or_else(|| anyhow!("Job \"{job_name}\" requires a \"role\" (Glue execution role)"))?;

        let glue_version = job_config
            .get("glue_version")
            .and_then(|v| v.as_str())
            .unwrap_or("4.0");

        let worker_type = job_config
            .get("worker_type")
            .and_then(|v| v.as_str())
            .unwrap_or("G.1X");

        let num_workers = job_config
            .get("number_of_workers")
            .and_then(|v| v.as_i64())
            .unwrap_or(2) as i32;

        let command = aws_sdk_glue::types::JobCommand::builder()
            .name("glueetl")
            .script_location(script_location)
            .python_version("3")
            .build();

        // Try update first, fall back to create
        let update_result = self
            .glue_client
            .update_job()
            .job_name(job_name)
            .job_update(
                aws_sdk_glue::types::JobUpdate::builder()
                    .role(execution_role)
                    .command(command.clone())
                    .glue_version(glue_version)
                    .worker_type(aws_sdk_glue::types::WorkerType::from(worker_type))
                    .number_of_workers(num_workers)
                    .build(),
            )
            .send()
            .await;

        match update_result {
            Ok(_) => Ok(()),
            Err(e) => {
                // If the job doesn't exist, create it
                if e.as_service_error()
                    .is_some_and(|se| se.is_entity_not_found_exception())
                {
                    self.glue_client
                        .create_job()
                        .name(job_name)
                        .role(execution_role)
                        .command(command)
                        .glue_version(glue_version)
                        .worker_type(aws_sdk_glue::types::WorkerType::from(worker_type))
                        .number_of_workers(num_workers)
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
            // 1. Upload script to S3
            let script_location = self.upload_script(&job_name, &artifact).await?;

            // 2. Create or update the Glue job
            self.create_or_update_glue_job(&job_name, &script_location, &job_config)
                .await?;

            // 3. Return resources for state tracking
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
            // Delete the Glue job first, then clean up the script
            for resource in &resources {
                match resource.r#type.as_str() {
                    "glue_job" => {
                        self.delete_glue_job(&resource.id).await?;
                    }
                    "s3_object" => {
                        // Script cleanup handled below
                    }
                    _ => {}
                }
            }

            // Delete the script from S3
            self.delete_script(&job_name).await?;

            Ok(())
        })
    }
}
