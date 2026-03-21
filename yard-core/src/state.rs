// yard-core/src/state.rs
use crate::storage::S3Storage;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use yard_structs::{ProjectState, StateBackend};

pub async fn initialize_backend(project_name: &str, backend: &StateBackend) -> Result<()> {
    match backend {
        StateBackend::Local { path } => {
            // 1. Prevent overwriting an existing state
            if path.exists() {
                println!(
                    "⚠️  State file already exists at {:?}. Skipping initialization.",
                    path
                );
                return Ok(());
            }

            // 2. Ensure the parent directory exists (e.g., .yard/)
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create directory {:?}", parent))?;
            }

            // 3. Construct the "Empty" ProjectState
            let new_state = ProjectState {
                project: project_name.to_string(),
                last_updated: Utc::now().to_rfc3339(),
                deployments: HashMap::new(), // Starts empty
            };

            // 4. Serialize to JSON and write to disk
            let json = serde_json::to_string_pretty(&new_state)
                .context("Failed to serialize initial state to JSON")?;

            fs::write(path, json)
                .with_context(|| format!("Failed to write state file to {:?}", path))?;

            println!("✅ Initialized local state at {:?}", path);
        }
        StateBackend::S3 { bucket, key, .. } => {
            // 1. Initialize the worker using your existing S3Storage::new
            let storage = S3Storage::new(bucket.clone(), key.clone()).await?;

            // 2. Build the initial state struct
            let new_state = ProjectState {
                project: project_name.to_string(),
                last_updated: Utc::now().to_rfc3339(),
                deployments: HashMap::new(),
            };

            // 3. Call the specialized write method
            match storage.write_if_not_exists(&new_state).await {
                Ok(_) => println!("✅ Initialized S3 state at s3://{}/{}", bucket, key),
                Err(e) if e.to_string().contains("already exists") => {
                    println!("⚠️  State already exists in S3. Skipping.");
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(())
}
