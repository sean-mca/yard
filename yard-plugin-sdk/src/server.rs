//! Plugin protocol server -- entry point for provider plugin binaries.

/// Entry point for plugin binaries.
///
/// Handles the full protocol lifecycle: stdout capture, tracing
/// initialization, handshake, request parsing, dispatch, and response
/// serialization.
pub struct PluginServer;
