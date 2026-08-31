#![warn(missing_docs)]
//! Shared data types for the yard ecosystem.
//!
//! This crate defines the core types used across yard-core, yard-cli, and
//! yard-server:
//!
//! - [`config`] -- Project manifest, job definitions, sources, sinks, transforms,
//!   and Airflow config types parsed from `yard.yaml` and job YAML files.
//! - [`state`] -- Per-job and per-DAG deployment state types (`JobState`,
//!   `DagState`, `Deployment`, `DagDeployment`) with `JobName`/`DagName`
//!   newtypes and `DeploymentStatus`/`DagDeploymentStatus` enums.
//! - [`diff`] -- Diff types (`DiffType`, `Diff`) for plan/apply change detection.
//! - [`error`] -- `ValidationError` for config validation results.
//! - [`plugin`] -- JSON-over-stdio plugin protocol types
//!   (`PluginOperation`, `HandshakeMessage`, `PluginRequest`, response
//!   structs) shared between the host and external plugin binaries.
//! - [`trigger`] -- Typed Airflow trigger model (`Trigger`, `SingleSource`)
//!   supporting S3, Dataset, SQS, API, and composite triggers.

/// Project manifest, job definitions, sources, sinks, transforms, and Airflow
/// config types parsed from `yard.yaml` and job YAML files.
pub mod config;
/// Diff types (`DiffType`, `Diff`) for plan/apply change detection.
pub mod diff;
/// `ValidationError` for config validation results.
pub mod error;
/// Per-job and per-DAG deployment state types.
pub mod state;
/// JSON-over-stdio plugin protocol types shared between the host
/// (`yard-core`) and external plugin binaries.
pub mod plugin;
/// Typed Airflow trigger model (`Trigger`, `SingleSource`) supporting S3,
/// Dataset, SQS, API, and composite triggers.
pub mod trigger;

pub use config::*;
pub use diff::*;
pub use error::*;
pub use plugin::*;
pub use state::*;
pub use trigger::*;
