use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Resource {
    pub r#type: String,
    pub id: String,
    pub provider: String,
}

/// Result of verifying whether a resource still exists in AWS.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceStatus {
    pub resource: Resource,
    pub exists: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Deployment {
    pub env: Option<String>,
    pub config_hash: String,
    pub config: serde_json::Value,
    pub status: String,
    pub applied_at: String,
    pub resources: Vec<Resource>,
}

/// Aggregate view of all per-job state files. Not persisted directly —
/// assembled at runtime by reading individual JobState files.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectState {
    pub project: String,
    pub last_updated: String,
    pub deployments: HashMap<String, Deployment>,
}

/// Per-job state file, stored as <job_name>.json in the state directory/prefix.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobState {
    pub job_name: String,
    pub project: String,
    pub deployment: Deployment,
}

/// Written to <job_name>.json.lock alongside the state file.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LockInfo {
    pub who: String,
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
    /// `"generated"` (local only) or `"deployed"` (uploaded to S3).
    pub status: String,
    pub applied_at: String,
    /// S3 URI where the DAG file was uploaded, if any.
    pub s3_uri: Option<String>,
}

/// Per-DAG state file, stored as `_dag_<dag_name>.json` in the state directory.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DagState {
    pub dag_name: String,
    pub project: String,
    pub deployment: DagDeployment,
    /// AWS credential config resolved at apply time for this DAG's upload
    /// bucket. Persisted so the destroy path (which runs without the DAG's
    /// source dir) can re-authenticate to the same account the upload used.
    /// `Value::Null` when the DAG's apply used the default AWS credential
    /// chain (preserves today's behavior for existing state files via
    /// `#[serde(default)]`).
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub aws: serde_json::Value,
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
            status: "deployed".to_string(),
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
        assert!(parsed.aws.is_null(), "missing aws key must default to Null");
        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert_eq!(
            reserialized, legacy_json,
            "round-trip must omit aws when null"
        );
    }

    #[test]
    fn dag_state_with_aws() {
        let state = DagState {
            dag_name: "test_dag".to_string(),
            project: "test".to_string(),
            deployment: sample_deployment(),
            aws: json!({"assume_role": "arn:aws:iam::333333333333:role/DagBucket"}),
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
            parsed.aws.get("assume_role").and_then(|v| v.as_str()),
            Some("arn:aws:iam::333333333333:role/DagBucket")
        );
    }
}
