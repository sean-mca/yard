//! JSON-over-stdio plugin protocol types.
//!
//! Defines the typed message contracts shared between the yard host
//! (`yard-core`) and external plugin binaries. The Phase 67 SDK crate
//! (`yard-plugin-sdk`) will re-export these same types so plugin authors
//! work against identical structs on both sides of the stdio boundary.
//!
//! # Protocol overview
//!
//! 1. Host spawns the plugin binary with piped stdin/stdout.
//! 2. Plugin writes a [`HandshakeMessage`] line to stdout.
//! 3. Host validates [`PROTOCOL_VERSION`], then writes a [`PluginRequest`]
//!    line to stdin and closes stdin (EOF).
//! 4. Plugin may emit zero or more [`ProgressMessage`] lines to stdout.
//! 5. Plugin writes the operation-specific response line and exits.

use serde::{Deserialize, Serialize};

use crate::state::{Resource, ResourceStatus};

/// Current protocol version. Increment only on breaking changes (D-01).
pub const PROTOCOL_VERSION: u32 = 1;

/// Operations a plugin can support (PROTO-01).
///
/// Each variant maps to a single request/response exchange over stdio.
/// Plugins advertise which operations they implement in the
/// [`HandshakeMessage::capabilities`] list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PluginOperation {
    /// Run provider-specific validation on a job config.
    Validate,
    /// Generate the deployment script for a job.
    Codegen,
    /// Deploy a job artifact to the target service.
    Deploy,
    /// Destroy previously deployed resources.
    Destroy,
    /// Verify that deployed resources still exist.
    Verify,
    /// Return the config field descriptors this provider accepts.
    Schema,
}

/// Handshake line sent by the plugin on startup (PROTO-02, D-03, D-04).
///
/// The plugin writes this as the first line of stdout immediately after
/// being spawned. The host validates [`PROTOCOL_VERSION`] compatibility
/// before sending any request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeMessage {
    /// Protocol version the plugin speaks (must equal [`PROTOCOL_VERSION`]).
    pub protocol_version: u32,
    /// Human-readable plugin name (e.g. `"yard-plugin-databricks"`).
    pub name: String,
    /// Semantic version of the plugin binary (e.g. `"0.3.1"`).
    pub version: String,
    /// Operations this plugin implements.
    pub capabilities: Vec<PluginOperation>,
}

/// Request sent from host to plugin via stdin (PROTO-01).
///
/// The host writes exactly one request line after reading the handshake.
/// Which fields are populated depends on the [`PluginOperation`]:
///
/// | Operation | `job_name` | `job_config` | `resources` | `artifact` |
/// |-----------|-----------|-------------|------------|-----------|
/// | Validate  | yes       | yes         | --         | --        |
/// | Codegen   | yes       | yes         | --         | --        |
/// | Deploy    | yes       | yes         | --         | yes       |
/// | Destroy   | yes       | --          | yes        | --        |
/// | Verify    | yes       | --          | yes        | --        |
/// | Schema    | --        | --          | --         | --        |
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginRequest {
    /// Which operation to perform.
    pub operation: PluginOperation,
    /// Job name being operated on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_name: Option<String>,
    /// Full job config as a JSON value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_config: Option<serde_json::Value>,
    /// Previously deployed resources (for destroy/verify).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<Resource>>,
    /// Generated deployment artifact (script content for deploy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<String>,
}

/// Progress line emitted by a plugin during long-running operations (PROTO-03, D-06).
///
/// The host reads these interleaved on stdout before the final response
/// line. The `"type":"progress"` discriminator is used by the host's
/// line parser to distinguish progress lines from the operation response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressMessage {
    /// Human-readable status update.
    pub message: String,
    /// Optional completion percentage (0..=100).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<u8>,
}

/// A plugin-side validation error with severity (D-11).
///
/// Plugins return these from the validate operation. The host's
/// `PluginProvider` adapter converts each entry to a
/// [`crate::ValidationError`] (which omits severity) for downstream
/// consumption by yard-core's validation pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginValidationError {
    /// Dot-separated path to the invalid field.
    pub field: String,
    /// Human-readable description of the validation failure.
    pub message: String,
    /// Severity level (e.g. `"error"`, `"warning"`).
    pub severity: String,
}

/// A config field descriptor returned by the schema operation (D-12).
///
/// Describes a single configuration field that the plugin's provider
/// accepts. Used by config cascade validation (Phase 68) to replace
/// hardcoded `ALLOWED_*` lists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field name (e.g. `"region"`, `"instance_type"`).
    pub name: String,
    /// Field type as a string (e.g. `"string"`, `"integer"`, `"boolean"`).
    pub field_type: String,
    /// Whether this field is required in the job config.
    pub required: bool,
    /// Human-readable description of the field's purpose.
    pub description: String,
}

/// Response from the validate operation.
///
/// Contains any validation errors the plugin found in the job config.
/// An empty `errors` list means the config is valid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateResponse {
    /// Validation errors found by the plugin.
    pub errors: Vec<PluginValidationError>,
}

/// Response from the codegen operation.
///
/// Returns the generated script content, or `None` to fall back to
/// yard-core's built-in codegen.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodegenResponse {
    /// Generated script content, if the plugin handles codegen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
}

/// Response from the deploy operation.
///
/// Returns the cloud resources that were created or updated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployResponse {
    /// Resources created or updated by the deployment.
    pub resources: Vec<Resource>,
}

/// Response from the destroy operation.
///
/// An empty struct -- a successful destroy has no payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestroyResponse {}

/// Response from the verify operation.
///
/// Returns the existence status of each previously deployed resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResponse {
    /// Per-resource existence checks.
    pub statuses: Vec<ResourceStatus>,
}

/// Response from the schema operation.
///
/// Returns the config field descriptors this provider accepts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResponse {
    /// Config fields the provider understands.
    pub fields: Vec<SchemaField>,
}
