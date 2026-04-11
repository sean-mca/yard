pub mod apply;
pub mod destroy;
pub mod force_unlock;
pub mod init;
pub mod plan;
pub mod show;
pub mod validate;

use anyhow::Result;
use std::path::PathBuf;

pub use yard_core::resolve::ResolvedProject;

pub async fn resolve_project(directory: Option<String>) -> Result<ResolvedProject> {
    let base_path = match directory {
        Some(d) => PathBuf::from(d),
        None => std::env::current_dir()
            .map_err(|e| anyhow::anyhow!("Failed to get current directory: {e}"))?,
    };

    yard_core::resolve::resolve_project(&base_path).await
}
