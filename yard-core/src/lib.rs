use anyhow::Result;
use yard_structs::{StateBackend, YardAction};

pub fn dispatch(action: YardAction) -> Result<()> {
    match action {
        YardAction::Init { manifest } => {
            println!("Initializing project '{}'...", manifest.project);

            // Access the 'state' field we just fixed
            match manifest.state {
                StateBackend::Local { path } => {
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    if !path.exists() {
                        std::fs::write(&path, "{}")?;
                    }
                    println!("✅ Local state ready at {:?}", path);
                }
                StateBackend::S3 { bucket, .. } => {
                    println!("☁️ S3 state backend for '{}' detected.", bucket);
                }
            }
        }
        _ => {}
    }
    Ok(())
}
