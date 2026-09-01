//! SDK for building yard provider plugins.
//!
//! Plugin authors depend on this single crate to build provider binaries.
//! The SDK abstracts all JSON-over-stdio protocol mechanics (handshake,
//! request parsing, response serialization, stdout protection, tracing
//! setup) so authors only implement business logic via [`PluginHandler`].
//!
//! # Quick start
//!
//! ```rust,ignore
//! use yard_plugin_sdk::{PluginHandler, PluginServer};
//!
//! struct MyProvider;
//!
//! // implement PluginHandler for MyProvider ...
//!
//! fn main() -> ! {
//!     PluginServer::run(MyProvider)
//! }
//! ```

/// Plugin handler trait defining the business-logic interface for provider
/// plugins.
pub mod handler;

/// Plugin protocol server -- entry point for provider plugin binaries.
pub mod server;

/// Stdout capture and protocol writer for fd-level stdout redirection.
///
/// This module is crate-private because it contains `unsafe` blocks for
/// the `libc::dup` / `libc::dup2` calls required to redirect file
/// descriptor 1 before any handler code runs.
#[allow(unsafe_code)]
mod stdout;

// === SDK convenience re-exports ===

pub use handler::PluginHandler;
pub use server::PluginServer;

// Protocol and domain types from yard-structs (SDK-03: single-dependency
// ergonomics for plugin authors).
pub use yard_structs::{
    CodegenResponse, DeployResponse, DestroyResponse, PluginValidationError, Resource,
    ResourceStatus, SchemaField, SchemaResponse, ValidateResponse, VerifyResponse,
};

/// Re-export `serde_json::Value` so handler methods can accept config
/// values without a direct `serde_json` dependency.
pub use serde_json::Value;

/// Re-export `anyhow` so plugin authors can use `anyhow::Result` in
/// handler implementations without a direct dependency.
pub use anyhow;

/// Re-export `tracing` so plugin authors can emit structured logs (D-09).
pub use tracing;
