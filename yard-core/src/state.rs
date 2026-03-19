use anyhow::Result;
use std::fs;
use std::path::Path;
use yard_structs::StateBackend; // Import Path

pub fn initialize_backend(backend: &StateBackend) -> Result<()> {
    match backend {
        StateBackend::Local { path } => {
            // Wrap the String in a Path to get access to filesystem methods
            let state_path = Path::new(path);

            if let Some(parent) = state_path.parent() {
                // If the parent is empty (current dir), create_dir_all doesn't fail,
                // but it's good practice to check if it's not empty.
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    fs::create_dir_all(parent)?;
                    println!("Created local state directory: {:?}", parent);
                }
            }

            if !state_path.exists() {
                fs::write(state_path, "{}")?;
                println!("Initialized empty state file at {:?}", state_path);
            }
        }
        StateBackend::S3 { bucket, .. } => {
            println!(
                "S3 backend detected for bucket '{}'. Infrastructure check pending.",
                bucket
            );
        }
    }
    Ok(())
}
