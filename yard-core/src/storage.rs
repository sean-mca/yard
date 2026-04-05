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
                    println!("Local state already exists at {:?}. Skipping.", s.path);
                    return Ok(());
                }
                self.write(state).await?;
                println!("Initialized local state.");
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
                    Ok(_) => println!("Initialized S3 state."),
                    Err(e) => {
                        if let Some(service_error) = e.as_service_error() {
                            if service_error.is_invalid_request() {
                                println!("S3 state already exists. Skipping.");
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
        StateBackend::S3 { bucket, key, region } => {
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(aws_config::Region::new(region.clone()))
                .load()
                .await;
            let client = Client::new(&config);

            Ok(Storage::S3(S3Storage {
                client,
                bucket: bucket.clone(),
                key: key.clone(),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yard_structs::ProjectState;

    fn test_state() -> ProjectState {
        ProjectState {
            project: "test-project".to_string(),
            last_updated: "2025-01-01T00:00:00Z".to_string(),
            deployments: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn local_write_and_read() {
        let dir = std::env::temp_dir().join(format!("yard_test_{}", std::process::id()));
        let path = dir.join("state.json");

        let storage = Storage::Local(LocalStorage { path: path.clone() });
        let state = test_state();

        storage.write(&state).await.unwrap();
        let loaded = storage.read().await.unwrap();

        assert_eq!(loaded.project, "test-project");
        assert_eq!(loaded.last_updated, "2025-01-01T00:00:00Z");
        assert!(loaded.deployments.is_empty());

        // Cleanup
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_read_missing_file_errors() {
        let path = std::env::temp_dir().join("yard_nonexistent_state.json");
        let storage = Storage::Local(LocalStorage { path });

        let result = storage.read().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn local_write_creates_parent_dirs() {
        let dir = std::env::temp_dir().join(format!("yard_nested_{}", std::process::id()));
        let path = dir.join("deep").join("nested").join("state.json");

        let storage = Storage::Local(LocalStorage { path: path.clone() });
        storage.write(&test_state()).await.unwrap();

        assert!(path.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_write_new_skips_existing() {
        let dir = std::env::temp_dir().join(format!("yard_wn_{}", std::process::id()));
        let path = dir.join("state.json");

        let storage = Storage::Local(LocalStorage { path: path.clone() });

        // First write should succeed
        storage.write_new(&test_state()).await.unwrap();

        // Second write_new should skip (not overwrite)
        let modified = ProjectState {
            project: "overwritten".to_string(),
            last_updated: "".to_string(),
            deployments: HashMap::new(),
        };
        storage.write_new(&modified).await.unwrap();

        // Should still be the original
        let loaded = storage.read().await.unwrap();
        assert_eq!(loaded.project, "test-project");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_write_overwrites_existing() {
        let dir = std::env::temp_dir().join(format!("yard_ow_{}", std::process::id()));
        let path = dir.join("state.json");

        let storage = Storage::Local(LocalStorage { path: path.clone() });
        storage.write(&test_state()).await.unwrap();

        let updated = ProjectState {
            project: "updated-project".to_string(),
            last_updated: "2025-06-01T00:00:00Z".to_string(),
            deployments: HashMap::new(),
        };
        storage.write(&updated).await.unwrap();

        let loaded = storage.read().await.unwrap();
        assert_eq!(loaded.project, "updated-project");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn get_storage_local() {
        let backend = StateBackend::Local {
            path: "/tmp/test.json".into(),
        };
        let storage = get_storage(&backend).await.unwrap();
        assert!(matches!(storage, Storage::Local(_)));
    }
}
