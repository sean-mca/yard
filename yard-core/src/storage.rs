use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use yard_structs::ProjectState;

pub struct S3Storage {
    pub client: Client,
    pub bucket: String,
    pub key: String,
    pub lock_key: String,
}

impl S3Storage {
    pub async fn new(bucket: String, key: String) -> Result<Self> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        let lock_key = format!("{}.lock", key);

        Ok(S3Storage {
            client,
            bucket,
            key,
            lock_key,
        })
    }

    /// Performs an atomic write only if the object does not already exist.
    /// This uses the S3 native conditional write feature.
    pub async fn write_if_not_exists(&self, state: &ProjectState) -> Result<()> {
        let json = serde_json::to_string_pretty(state)?;

        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(json.into_bytes().into())
            .content_type("application/json")
            .if_none_match("*") // The magic "Terragrunt" header
            .send()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(service_error) = e.as_service_error() {
                    if service_error.is_invalid_request() {
                        return Err(anyhow!("State already exists in S3"));
                    }
                }
                Err(e).context("Failed to perform conditional S3 write")
            }
        }
    }
}
