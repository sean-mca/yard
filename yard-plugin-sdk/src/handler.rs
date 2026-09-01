//! Plugin handler trait defining the business-logic interface for provider plugins.
//!
//! Plugin authors implement [`PluginHandler`] to define how their provider
//! handles each operation (validate, codegen, deploy, destroy, verify,
//! schema). The SDK's [`crate::PluginServer`] dispatches incoming requests
//! to the appropriate method and serializes the return value back over the
//! protocol channel.

use yard_structs::{
    CodegenResponse, DeployResponse, DestroyResponse, Resource, SchemaResponse, ValidateResponse,
    VerifyResponse,
};

/// Trait that plugin authors implement to handle provider operations.
///
/// All eight methods are required -- there are no default implementations.
/// The six operation methods mirror the host's typed convenience methods
/// in `yard-core`'s plugin spawner, ensuring symmetric signatures on both
/// sides of the protocol boundary.
///
/// The two metadata methods ([`name`](PluginHandler::name) and
/// [`version`](PluginHandler::version)) are included in the handshake
/// message sent to the host on startup.
pub trait PluginHandler {
    /// Human-readable plugin name (e.g. `"yard-plugin-databricks"`).
    ///
    /// Included in the [`yard_structs::HandshakeMessage`] sent to the
    /// host on startup.
    fn name(&self) -> &str;

    /// Semantic version of the plugin binary (e.g. `"0.3.1"`).
    ///
    /// Included in the [`yard_structs::HandshakeMessage`] sent to the
    /// host on startup.
    fn version(&self) -> &str;

    /// Run provider-specific validation on a job config.
    ///
    /// # Errors
    ///
    /// Returns an error if validation cannot be performed (e.g. config
    /// parsing failure). Validation *findings* should be returned as
    /// entries in [`ValidateResponse::errors`], not as `Err`.
    fn validate(
        &self,
        job_name: &str,
        job_config: &serde_json::Value,
    ) -> anyhow::Result<ValidateResponse>;

    /// Generate the deployment script for a job.
    ///
    /// # Errors
    ///
    /// Returns an error if code generation fails.
    fn codegen(
        &self,
        job_name: &str,
        job_config: &serde_json::Value,
    ) -> anyhow::Result<CodegenResponse>;

    /// Deploy a job artifact to the target service.
    ///
    /// # Errors
    ///
    /// Returns an error if deployment fails.
    fn deploy(
        &self,
        job_name: &str,
        job_config: &serde_json::Value,
        artifact: &str,
    ) -> anyhow::Result<DeployResponse>;

    /// Destroy previously deployed resources.
    ///
    /// # Errors
    ///
    /// Returns an error if destruction fails.
    fn destroy(&self, job_name: &str, resources: &[Resource]) -> anyhow::Result<DestroyResponse>;

    /// Verify that deployed resources still exist.
    ///
    /// # Errors
    ///
    /// Returns an error if verification cannot be performed.
    fn verify(&self, job_name: &str, resources: &[Resource]) -> anyhow::Result<VerifyResponse>;

    /// Return the config field descriptors this provider accepts.
    ///
    /// # Errors
    ///
    /// Returns an error if schema introspection fails.
    fn schema(&self) -> anyhow::Result<SchemaResponse>;
}
