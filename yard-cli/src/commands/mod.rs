//! Per-subcommand handlers for the YARD CLI.
//!
//! Each child module exposes an `execute()` function called from
//! [`crate::run`]. Shared display logic lives in [`display`].
//!
//! - [`apply`] -- Apply infrastructure changes
//! - [`destroy`] -- Tear down deployed resources
//! - [`display`] -- Shared plan-diff formatting
//! - [`force_unlock`] -- Remove a stale lock
//! - [`init`] -- Scaffold a new project
//! - [`list`] -- List deployment targets as JSON
//! - [`plan`] -- Preview changes
//! - [`show`] -- Print generated scripts
//! - [`validate`] -- Validate job configurations

pub mod apply;
pub mod destroy;
pub mod display;
pub mod force_unlock;
pub mod init;
pub mod list;
pub mod plan;
pub mod show;
pub mod validate;

use anyhow::Result;
use std::path::PathBuf;

/// Re-export of the resolved-project type from [`yard_core`].
pub use yard_core::resolve::ResolvedProject;

/// Resolve the YARD project from an optional directory argument.
///
/// Falls back to the current working directory when `directory` is `None`.
///
/// # Errors
///
/// Returns an error if the current directory cannot be determined or if
/// project resolution fails (missing `yard.yaml`, invalid config, etc.).
pub async fn resolve_project(directory: Option<String>) -> Result<ResolvedProject> {
    let base_path = match directory {
        Some(d) => PathBuf::from(d),
        None => std::env::current_dir()
            .map_err(|e| anyhow::anyhow!("failed to get current directory: {e}"))?,
    };

    yard_core::resolve::resolve_project(&base_path).await
}
