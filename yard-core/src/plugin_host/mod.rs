//! Plugin host -- process spawning and Provider adapter for out-of-process
//! plugin binaries.
//!
//! This module manages the lifecycle of external plugin processes:
//! spawning binaries, validating the JSON-over-stdio handshake, exchanging
//! typed requests and responses, enforcing timeouts, and verifying binary
//! checksums before execution.
//!
//! # Architecture
//!
//! - [`PluginSpawner`] handles the low-level process lifecycle (spawn,
//!   handshake, request/response, timeout, kill).
//! - [`PluginProvider`] adapts a `PluginSpawner` into the [`Provider`]
//!   trait so that orchestrate.rs can treat plugins identically to
//!   compiled-in providers.
//! - [`PluginHostConfig`] holds runtime settings (plugins directory,
//!   timeout, lock file path).
//!
//! [`Provider`]: crate::providers::Provider

pub mod download;
pub mod git;
pub mod provider;
pub mod spawner;

pub use download::{PluginSource, ResolvedBinary, ensure_plugin_cached, parse_plugin_source};
pub use provider::PluginProvider;
pub use spawner::{PluginHostConfig, PluginSpawner, cleanup_plugin_cache, platform_key, target_triple};
