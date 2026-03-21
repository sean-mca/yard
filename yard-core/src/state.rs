// yard-core/src/state.rs
use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::Client;
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
            let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = Client::new(&config);

            let new_state = ProjectState {
                project: project_name.to_string(),
                last_updated: Utc::now().to_rfc3339(),
                deployments: HashMap::new(),
            };
            let json = serde_json::to_string_pretty(&new_state)?;

            println!("☁️  Initializing S3 state: s3://{}/{}", bucket, key);

            let result = client
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(json.into_bytes().into())
                .content_type("application/json")
                // CRITICAL: This header ensures we DON'T overwrite an existing state.
                // S3 interprets If-None-Match: "*" as "Fail if any object already exists at this key"
                .if_none_match("*")
                .send()
                .await;

            match result {
                Ok(_) => println!("✅ Successfully initialized S3 backend."),
                Err(e) => {
                    // Check if the error is specifically a "Precondition Failed" (412)
                    if let Some(service_error) = e.as_service_error() {
                        if service_error.is_invalid_request() {
                            println!(
                                "⚠️  State file already exists in S3. Skipping initialization."
                            );
                            return Ok(());
                        }
                    }
                    return Err(e).context("Failed to upload initial state to S3");
                }
            }
        }
    }
    Ok(())
}
