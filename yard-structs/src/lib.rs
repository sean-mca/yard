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
//! - [`trigger`] -- Typed Airflow trigger model (`Trigger`, `SingleSource`)
//!   supporting S3, Dataset, SQS, API, and composite triggers.

#![warn(clippy::unwrap_used, clippy::expect_used)]

pub mod config;
pub mod diff;
pub mod error;
pub mod state;
pub mod trigger;

pub use config::*;
pub use diff::*;
pub use error::*;
pub use state::*;
pub use trigger::*;
