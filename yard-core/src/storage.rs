use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;

pub struct S3Storage {
    pub client: Client,
    pub bucket: String,
    pub key: String,
    pub lock_key: String,
}

impl S3Storage {
    pub async fn new(bucket: String, key: String) -> Result<Self> {
        // This is the "Magic" function.
        // It checks for OIDC vars, then ENV vars, then EC2 metadata.
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
}
