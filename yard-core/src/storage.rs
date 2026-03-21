use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use std::path::PathBuf;
use yard_structs::{ProjectState, StateBackend};

// 1. Define the specific workers
pub struct LocalStorage {
    pub path: PathBuf,
}

pub struct S3Storage {
    pub client: Client,
    pub bucket: String,
    pub key: String,
}

// 2. Define the Enum that "Dispatches" to them
pub enum Storage {
    Local(LocalStorage),
    S3(S3Storage),
}

impl Storage {
    /// The unified Read interface
    pub async fn read(&self) -> Result<ProjectState> {
        match self {
            Storage::Local(s) => {
                let content = tokio::fs::read_to_string(&s.path)
                    .await
                    .with_context(|| format!("Failed to read local state at {:?}", s.path))?;
                let state: ProjectState = serde_json::from_str(&content)?;
                Ok(state)
            }
            Storage::S3(s) => {
                let resp = s
                    .client
                    .get_object()
                    .bucket(&s.bucket)
                    .key(&s.key)
                    .send()
                    .await?;

                let data = resp.body.collect().await?.into_bytes();
                let state: ProjectState = serde_json::from_slice(&data)?;
                Ok(state)
            }
        }
    }

    /// The unified Write interface
    pub async fn write(&self, state: &ProjectState) -> Result<()> {
        match self {
            Storage::Local(s) => {
                let json = serde_json::to_string_pretty(state)?;
                if let Some(parent) = s.path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(&s.path, json).await?;
                Ok(())
            }
            Storage::S3(s) => {
                let json = serde_json::to_string_pretty(state)?;
                s.client
                    .put_object()
                    .bucket(&s.bucket)
                    .key(&s.key)
                    .body(json.into_bytes().into())
                    .content_type("application/json")
                    .send()
                    .await?;
                Ok(())
            }
        }
    }

    pub async fn write_new(&self, state: &ProjectState) -> Result<()> {
        match self {
            Storage::Local(s) => {
                if s.path.exists() {
                    println!("⚠️  Local state already exists at {:?}. Skipping.", s.path);
                    return Ok(());
                }
                self.write(state).await?;
                println!("✅ Initialized local state.");
            }
            Storage::S3(s) => {
                let json = serde_json::to_string_pretty(state)?;
                let result = s
                    .client
                    .put_object()
                    .bucket(&s.bucket)
                    .key(&s.key)
                    .body(json.into_bytes().into())
                    .content_type("application/json")
                    .if_none_match("*") // The magic "Don't overwrite" header
                    .send()
                    .await;

                match result {
                    Ok(_) => println!("✅ Initialized S3 state."),
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_invalid_request() {
                                println!("⚠️  S3 state already exists. Skipping.");
                                return Ok(());
                            }
                        }
                        return Err(e.into());
                    }
                }
            }
        }
        Ok(())
    }
}

// 3. The Factory (Now returns the Enum instead of a Boxed Trait)
pub async fn get_storage(backend: &StateBackend) -> Result<Storage> {
    match backend {
        StateBackend::Local { path } => Ok(Storage::Local(LocalStorage { path: path.clone() })),
        StateBackend::S3 { bucket, key, .. } => {
            let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
            let client = Client::new(&config);

            Ok(Storage::S3(S3Storage {
                client,
                bucket: bucket.clone(),
                key: key.clone(),
            }))
        }
    }
}
