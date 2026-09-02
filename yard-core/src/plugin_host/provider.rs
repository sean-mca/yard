//! PluginProvider -- [`Provider`] trait adapter for plugin binaries.
//!
//! [`PluginProvider`] wraps a [`PluginSpawner`] and implements all six
//! [`Provider`] methods by delegating each operation to the spawner's
//! typed convenience methods. This makes plugin-backed providers
//! transparent to `orchestrate.rs` and the rest of yard-core.
//!
//! [`Provider`]: crate::providers::Provider

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use serde_json::Value;
use yard_structs::{Resource, ResourceStatus, SchemaField, ValidationError};

use crate::providers::Provider;

use super::spawner::{PluginHostConfig, PluginSpawner};

/// Provider adapter that delegates all operations to an external plugin
/// binary via [`PluginSpawner`].
///
/// Each trait method spawns a fresh child process, validates the
/// handshake, and exchanges the typed request/response (HOST-04).
#[derive(Debug, Clone)]
pub struct PluginProvider {
    /// The underlying spawner that manages process lifecycle.
    spawner: PluginSpawner,
}

impl PluginProvider {
    /// Create a new `PluginProvider` from an existing [`PluginSpawner`].
    pub fn new(spawner: PluginSpawner) -> Self {
        Self { spawner }
    }

    /// Convenience constructor that creates the internal [`PluginSpawner`]
    /// from the binary path, plugin name, and config.
    pub fn from_binary(
        binary_path: PathBuf,
        plugin_name: String,
        config: PluginHostConfig,
    ) -> Self {
        Self {
            spawner: PluginSpawner::new(binary_path, plugin_name, config),
        }
    }
}

impl Provider for PluginProvider {
    fn deploy(
        &self,
        job_name: &str,
        artifact: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Resource>>> + Send + '_>> {
        let job_name = job_name.to_string();
        let artifact = artifact.to_string();
        let job_config = job_config.clone();

        Box::pin(async move {
            self.spawner
                .call_deploy(&job_name, &artifact, &job_config)
                .await
        })
    }

    fn destroy(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let job_name = job_name.to_string();
        let resources = resources.to_vec();

        Box::pin(async move {
            self.spawner
                .call_destroy(&job_name, &resources)
                .await
        })
    }

    fn verify_resources(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceStatus>>> + Send + '_>> {
        let job_name = job_name.to_string();
        let resources = resources.to_vec();

        Box::pin(async move {
            self.spawner
                .call_verify(&job_name, &resources)
                .await
        })
    }

    fn validate(
        &self,
        job_name: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ValidationError>>> + Send + '_>> {
        let job_name = job_name.to_string();
        let job_config = job_config.clone();

        Box::pin(async move {
            let plugin_errors = self
                .spawner
                .call_validate(&job_name, &job_config)
                .await?;

            // Convert PluginValidationError -> ValidationError (drop severity)
            let errors = plugin_errors
                .into_iter()
                .map(|e| ValidationError {
                    field: e.field,
                    message: e.message,
                })
                .collect();

            Ok(errors)
        })
    }

    fn codegen(
        &self,
        job_name: &str,
        job_config: &Value,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>>> + Send + '_>> {
        let job_name = job_name.to_string();
        let job_config = job_config.clone();

        Box::pin(async move {
            self.spawner
                .call_codegen(&job_name, &job_config)
                .await
        })
    }

    fn schema(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SchemaField>>> + Send + '_>> {
        Box::pin(async move { self.spawner.call_schema().await })
    }
}
