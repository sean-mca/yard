use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::config::AwsCredentialConfig;

/// A job name identifier used to uniquely reference a job within a project.
///
/// Wraps a `String` and is transparent to serde, so the JSON wire format
/// stays `"job_name": "my-job"` (not a nested object). This preserves
/// backward compatibility with existing state files.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobName(String);

impl JobName {
    /// Creates a new `JobName` from any string-like value.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the job name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for JobName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A DAG name identifier used to uniquely reference a DAG within a project.
///
/// Wraps a `String` and is transparent to serde, so the JSON wire format
/// stays `"dag_name": "my-dag"` (not a nested object). This preserves
/// backward compatibility with existing state files.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DagName(String);

impl DagName {
    /// Creates a new `DagName` from any string-like value.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Returns the DAG name as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DagName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for DagName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Deployment lifecycle status for a job.
///
/// Serialized as lowercase strings (`"deployed"`, `"generated"`) to preserve
/// wire-format compatibility with existing state files.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentStatus {
    /// Successfully applied to the target environment.
    Deployed,
    /// Generated locally but not yet deployed.
    Generated,
}

impl std::fmt::Display for DeploymentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentStatus::Deployed => f.write_str("deployed"),
            DeploymentStatus::Generated => f.write_str("generated"),
        }
    }
}

/// Deployment lifecycle status for a DAG.
///
/// Serialized as lowercase strings (`"deployed"`, `"generated"`) to preserve
/// wire-format compatibility with existing state files.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DagDeploymentStatus {
    /// DAG deployed (uploaded to S3).
    Deployed,
    /// DAG generated locally but not yet uploaded.
    Generated,
}

impl std::fmt::Display for DagDeploymentStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagDeploymentStatus::Deployed => f.write_str("deployed"),
            DagDeploymentStatus::Generated => f.write_str("generated"),
        }
    }
}

/// An AWS resource tracked in deployment state.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Resource {
    /// AWS resource type (e.g. `"AWS::Glue::Job"`).
    pub r#type: String,
    /// AWS resource identifier.
    pub id: String,
    /// Provider that manages this resource (e.g. `"glue"`, `"emr"`).
    pub provider: String,
}

/// Result of verifying whether a resource still exists in AWS.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceStatus {
    /// The resource being verified.
    pub resource: Resource,
    /// Whether the resource was found in AWS.
    pub exists: bool,
}

/// Snapshot of a single job deployment -- hash, config, status, and resources.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Deployment {
    /// Environment name for this deployment, if applicable.
    pub env: Option<String>,
    /// blake3 hash of the job config at deploy time.
    pub config_hash: String,
    /// Serialized job config for diffing.
    pub config: serde_json::Value,
    /// Current lifecycle status of this deployment.
    pub status: DeploymentStatus,
    /// ISO 8601 timestamp when the deployment was applied.
    pub applied_at: String,
    /// AWS resources created by this deployment.
    pub resources: Vec<Resource>,
}

/// Aggregate view of all per-job state files. Not persisted directly --
/// assembled at runtime by reading individual `JobState` files.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ProjectState {
    /// Project name from `yard.yaml`.
    pub project: String,
    /// ISO 8601 timestamp of the most recent state update.
    pub last_updated: String,
    /// Per-job deployments keyed by job name.
    pub deployments: HashMap<String, Deployment>,
}

/// Per-job state file, stored as `<job_name>.json` in the state directory/prefix.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct JobState {
    /// Unique job name identifier.
    pub job_name: JobName,
    /// Project name this job belongs to.
    pub project: String,
    /// Deployment snapshot for this job.
    pub deployment: Deployment,
}

/// Written to `<job_name>.json.lock` alongside the state file.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct LockInfo {
    /// Identity of the lock holder (e.g. username or CI job).
    pub who: String,
    /// ISO 8601 timestamp when the lock was acquired.
    pub created_at: String,
}

/// Deployed state of a single Airflow DAG file.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DagDeployment {
    /// blake3 hash of the generated DAG Python content.
    pub content_hash: String,
    /// Serialized AirflowSection for diffing config changes.
    pub config: serde_json::Value,
    /// Task names in the DAG at time of deploy.
    pub tasks: Vec<String>,
    /// Current lifecycle status of this DAG deployment.
    pub status: DagDeploymentStatus,
    /// ISO 8601 timestamp when the deployment was applied.
    pub applied_at: String,
    /// S3 URI where the DAG file was uploaded, if any.
    pub s3_uri: Option<String>,
}

/// Per-DAG state file, stored as `_dag_<dag_name>.json` in the state directory.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DagState {
    /// Unique DAG name identifier.
    pub dag_name: DagName,
    /// Project name this DAG belongs to.
    pub project: String,
    /// Deployment snapshot for this DAG.
    pub deployment: DagDeployment,
    /// AWS credential config resolved at apply time for this DAG's upload
    /// bucket. Persisted so the destroy path (which runs without the DAG's
    /// source dir) can re-authenticate to the same account the upload used.
    /// `None` when the DAG's apply used the default AWS credential chain
    /// (preserves today's behavior for existing pre-Phase-9 state files
    /// via `#[serde(default)]`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsCredentialConfig>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_deployment() -> DagDeployment {
        DagDeployment {
            content_hash: "abc123".to_string(),
            config: json!({"schedule": "@daily"}),
            tasks: vec!["task_a".to_string()],
            status: DagDeploymentStatus::Deployed,
            applied_at: "2026-04-22T00:00:00Z".to_string(),
            s3_uri: Some("s3://my-bucket/dags/test.py".to_string()),
        }
    }

    #[test]
    fn dag_state_no_aws_roundtrip() {
        // State files written before Phase 9 have no `aws` key. They must
        // deserialize (via #[serde(default)]) and re-serialize without
        // inventing an `aws: null` key. Strictly additive.
        let legacy_json = json!({
            "dag_name": "test_dag",
            "project": "test",
            "deployment": {
                "content_hash": "abc123",
                "config": {"schedule": "@daily"},
                "tasks": ["task_a"],
                "status": "deployed",
                "applied_at": "2026-04-22T00:00:00Z",
                "s3_uri": "s3://my-bucket/dags/test.py"
            }
        });
        let parsed: DagState = serde_json::from_value(legacy_json.clone()).unwrap();
        assert!(parsed.aws.is_none(), "missing aws key must default to None");

        // Verify DagName is transparent: JSON stays "dag_name": "test_dag"
        assert_eq!(parsed.dag_name.as_str(), "test_dag");

        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert_eq!(
            reserialized, legacy_json,
            "round-trip must omit aws when None"
        );
    }

    #[test]
    fn dag_state_with_aws() {
        let state = DagState {
            dag_name: DagName::new("test_dag"),
            project: "test".to_string(),
            deployment: sample_deployment(),
            aws: Some(AwsCredentialConfig {
                assume_role: Some("arn:aws:iam::333333333333:role/DagBucket".to_string()),
                ..Default::default()
            }),
        };
        let serialized = serde_json::to_value(&state).unwrap();
        assert_eq!(
            serialized
                .get("aws")
                .and_then(|v| v.get("assume_role"))
                .and_then(|v| v.as_str()),
            Some("arn:aws:iam::333333333333:role/DagBucket")
        );
        let parsed: DagState = serde_json::from_value(serialized).unwrap();
        assert_eq!(
            parsed.aws.as_ref().and_then(|c| c.assume_role.as_deref()),
            Some("arn:aws:iam::333333333333:role/DagBucket")
        );
    }

    #[test]
    fn job_name_serde_transparent_roundtrip() {
        // Verify JobName newtype is transparent: JSON stays "job_name": "my-job"
        let state = JobState {
            job_name: JobName::new("my-job"),
            project: "test-project".to_string(),
            deployment: Deployment {
                env: None,
                config_hash: "hash123".to_string(),
                config: json!({"type": "glue"}),
                status: DeploymentStatus::Generated,
                applied_at: "2026-01-01T00:00:00Z".to_string(),
                resources: vec![],
            },
        };

        let serialized = serde_json::to_value(&state).unwrap();
        // Must be a flat string, not a nested object
        assert_eq!(serialized["job_name"], json!("my-job"));

        let deserialized: JobState = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized.job_name.as_str(), "my-job");
        assert_eq!(deserialized, state);
    }

    #[test]
    fn deployment_status_roundtrip() {
        // Verify DeploymentStatus serializes to lowercase string
        assert_eq!(
            serde_json::to_value(DeploymentStatus::Deployed).unwrap(),
            json!("deployed")
        );
        assert_eq!(
            serde_json::to_value(DeploymentStatus::Generated).unwrap(),
            json!("generated")
        );

        // Verify deserialization from lowercase string
        let deployed: DeploymentStatus = serde_json::from_value(json!("deployed")).unwrap();
        assert_eq!(deployed, DeploymentStatus::Deployed);
        let generated: DeploymentStatus = serde_json::from_value(json!("generated")).unwrap();
        assert_eq!(generated, DeploymentStatus::Generated);
    }

    #[test]
    fn dag_deployment_status_roundtrip() {
        // Verify DagDeploymentStatus serializes to lowercase string
        assert_eq!(
            serde_json::to_value(DagDeploymentStatus::Deployed).unwrap(),
            json!("deployed")
        );
        assert_eq!(
            serde_json::to_value(DagDeploymentStatus::Generated).unwrap(),
            json!("generated")
        );
    }

    #[test]
    fn dag_name_serde_transparent_roundtrip() {
        // Verify DagName newtype is transparent via a full DagState round-trip
        let state = DagState {
            dag_name: DagName::new("test_dag"),
            project: "test".to_string(),
            deployment: sample_deployment(),
            aws: None,
        };

        let serialized = serde_json::to_value(&state).unwrap();
        // Must be a flat string
        assert_eq!(serialized["dag_name"], json!("test_dag"));

        let deserialized: DagState = serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized.dag_name.as_str(), "test_dag");
    }
}
