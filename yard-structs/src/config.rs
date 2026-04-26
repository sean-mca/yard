use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

fn default_path_buf() -> PathBuf {
    PathBuf::new()
}

/// Discriminator for `JobDefinition.job_type` (TYPE-01). Wire format is the
/// lowercase variant name — `"glue"`, `"emr"`, `"bash"`. Adding a fourth job
/// type requires (1) a new variant here, (2) a `FromStr` arm below, (3) a new
/// provider impl in `yard-core/src/providers/`, and (4) a new validation arm
/// in `yard-core/src/validation/rules.rs`.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum JobType {
    Glue,
    Emr,
    Bash,
}

impl std::fmt::Display for JobType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            JobType::Glue => "glue",
            JobType::Emr => "emr",
            JobType::Bash => "bash",
        };
        f.write_str(s)
    }
}

impl std::str::FromStr for JobType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "glue" => Ok(JobType::Glue),
            "emr" => Ok(JobType::Emr),
            "bash" => Ok(JobType::Bash),
            other => Err(anyhow::anyhow!(
                "invalid job type '{other}' (expected: glue, emr, bash)"
            )),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StateBackend {
    Local {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        key: String,
        /// Optional per-state-backend `aws:` sub-block. Shape parallels the
        /// root `aws:` on `ProjectManifest` — untyped `serde_json::Value`
        /// whose readers use `.get("assume_role").and_then(|v| v.as_str())`,
        /// `.get("session_name")`, `.get("external_id")`. `Value::Null` means
        /// "fall through to `YARD_STATE_AWS_*` envs, then the default AWS
        /// credential provider chain" — keeps today's behavior unchanged
        /// when unset (Phase 9 strictly-additive guarantee).
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        aws: serde_json::Value,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectManifest {
    pub project: String,
    pub state: StateBackend,
    /// Per-provider config, keyed by job type (e.g. "glue", "emr").
    /// Each value is the raw provider config block from yard.yaml.
    #[serde(default)]
    pub providers: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub jobs: HashMap<String, JobDefinition>,
    /// Root-level `aws:` block from yard.yaml. Controls yard's own AWS
    /// credentials (AssumeRole target, session name, external id, region).
    /// Per-job and per-DAG account.yaml `aws:` blocks shallow-override this.
    /// `Value::Null` when not set — providers fall back to the default AWS
    /// credential provider chain.
    #[serde(default)]
    pub aws: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Import {
    pub name: String,
    pub from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Source {
    pub name: String,                   // variable name: produces df_<name>
    pub source_type: String,            // s3, jdbc, catalog, kafka, api
    pub format: Option<String>,         // parquet, csv, json, orc
    pub path: Option<String>,           // s3 path
    pub connection_url: Option<String>, // jdbc url; kafka bootstrap servers
    pub table: Option<String>,          // jdbc or catalog
    pub database: Option<String>,       // catalog
    pub secret_id: Option<String>,      // Secrets Manager secret
    /// "spark" (SparkSession.read) or "glue" (DynamicFrame). Defaults to the
    /// provider-level `default_engine` when unset; "spark" if that's also unset.
    /// Only meaningful for source_types where both paths exist (s3, jdbc).
    #[serde(default)]
    pub engine: Option<String>,
    /// For jdbc+glue: the Glue connector name ("mysql", "postgresql", etc.).
    #[serde(default)]
    pub connection_type: Option<String>,
    /// Kafka topic name.
    #[serde(default)]
    pub topic: Option<String>,
    /// HTTP GET URL for `api` source.
    #[serde(default)]
    pub url: Option<String>,
    /// HTTP headers for `api` source.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Opaque passthrough to `.option()` (spark engine) or `connection_options`
    /// (glue engine). Values are rendered as Python literals.
    #[serde(default)]
    pub options: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Sink {
    pub source: Option<String>, // which df to write (defaults to first/only source)
    pub sink_type: String,      // s3, jdbc, catalog
    pub format: Option<String>, // parquet, csv, json, orc
    pub path: Option<String>,   // s3 path
    pub connection_url: Option<String>, // jdbc
    pub table: Option<String>,  // jdbc or catalog
    pub database: Option<String>, // catalog
    pub secret_id: Option<String>, // Secrets Manager secret
    pub mode: Option<String>,   // overwrite, append, error
    pub partition_by: Vec<String>, // partition columns
    /// For iceberg sinks only: coerce nulls/voids to type-appropriate defaults
    /// before writing (prevents `void`-typed columns from failing the write).
    /// Defaults to true on iceberg. Explicit `false` opts out.
    #[serde(default)]
    pub fill_nulls: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct OrderBySpec {
    pub column: String,
    pub desc: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Transform {
    pub transform_type: String, // filter, sql, drop_columns, rename, select, add_column, join, aggregate, window
    pub source: Option<String>, // which df to operate on (defaults to first/only source)
    pub output: Option<String>, // name for result df (defaults to same as source)
    pub condition: Option<String>, // filter
    pub query: Option<String>,  // sql
    pub columns: Vec<String>,   // drop_columns, select
    pub mapping: HashMap<String, String>, // rename (old -> new)
    pub name: Option<String>,   // add_column, window (new column name)
    pub expression: Option<String>, // add_column, window (window expression)
    // join fields
    pub left: Option<String>,  // join: left df name
    pub right: Option<String>, // join: right df name
    pub on: Option<String>,    // join: column to join on
    pub how: Option<String>,   // join: inner, left, right, outer
    // aggregate fields
    pub group_by: Vec<String>,         // aggregate: grouping columns
    pub aggs: HashMap<String, String>, // aggregate: alias -> expression (e.g. "total" -> "sum(amount)")
    // window fields
    pub partition_by: Vec<String>,  // window: partition columns
    pub order_by: Vec<OrderBySpec>, // window: order spec
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobDefinition {
    pub job_type: JobType,
    pub imports: Vec<Import>,
    pub body: Option<String>,
    /// Path to an external Python file that replaces YARD's generated script entirely.
    pub job_file: Option<String>,
    pub sources: Vec<Source>,
    pub sink: Option<Sink>,
    pub transforms: Vec<Transform>,
    /// Per-job Airflow metadata, parsed from the optional `airflow:` block.
    /// `None` means the job does not participate in any DAG.
    pub airflow: Option<AirflowJobBlock>,
    /// Job-level partition columns for Iceberg sinks. Only "year", "month",
    /// "day" are supported. Codegen derives these from a timestamp column.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Existing timestamp column to derive year/month/day from. Mutually
    /// exclusive with `create_timestamp`.
    #[serde(default)]
    pub partition_timestamp_column: Option<String>,
    /// If true, codegen adds an `ingestion_timestamp = current_timestamp()`
    /// column and derives partitions from it. Mutually exclusive with
    /// `partition_timestamp_column`.
    #[serde(default)]
    pub create_timestamp: bool,
    pub config: serde_json::Value,
    /// Directory containing the job's YAML file. Populated during discovery;
    /// not serialized to state. Used to locate the nearest ancestor `dag.yaml`
    /// when grouping jobs into DAGs.
    #[serde(skip, default = "default_path_buf")]
    pub dir: PathBuf,
    /// Filename-derived base name (e.g. `orders` from `orders.yaml`).
    /// Populated during discovery; used for short-name resolution in
    /// `depends_on`.
    #[serde(skip, default)]
    pub base_name: String,
}

/// Hand-written `Default` because `JobType` deliberately has no `Default` impl
/// (D-08): the only sensible default for a JobDefinition's `job_type` is
/// `JobType::Glue` (most-tested type, least-surprising default for the
/// non-deployable empty default), but a default at the JobType level would
/// invite accidental `JobType::default()` calls in code that should always
/// pick deliberately. The default JobDefinition itself stays non-deployable —
/// empty body, no sources, no sink — same as before this refactor.
impl Default for JobDefinition {
    fn default() -> Self {
        Self {
            job_type: JobType::Glue,
            imports: Vec::new(),
            body: None,
            job_file: None,
            sources: Vec::new(),
            sink: None,
            transforms: Vec::new(),
            airflow: None,
            partition_by: Vec::new(),
            partition_timestamp_column: None,
            create_timestamp: false,
            config: serde_json::Value::Null,
            dir: PathBuf::new(),
            base_name: String::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct YARDContext {
    pub account: serde_json::Value,
    pub region: serde_json::Value,
    pub transforms: serde_json::Value,
    /// Loaded from the optional `dag.yaml` marker file in a job's directory
    /// (or the nearest ancestor). Presence marks the directory as a DAG grouping.
    /// Contents hold DAG-level Airflow config (schedule, default_args, etc).
    pub dag: serde_json::Value,
}

/// Airflow config shared across inheritance layers (yard.yaml, region.yaml,
/// account.yaml, dag.yaml, and the per-job `airflow:` block). Every layer has
/// the same shape; later layers override earlier ones via shallow merge.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct AirflowSection {
    pub schedule: Option<String>,
    pub owner: Option<String>,
    pub retries: Option<i32>,
    pub dags_bucket: Option<String>,
    pub dags_prefix: Option<String>,
    /// Dataset URIs that trigger this DAG. When set, the DAG's schedule
    /// becomes `[Dataset("uri"), ...]` instead of a cron string. Mutually
    /// exclusive with `schedule`.
    #[serde(default)]
    pub triggered_by: Vec<String>,
    /// Optional per-airflow-provider `aws:` sub-block for the DAG upload bucket.
    /// When set, takes priority over the root `aws:` + nearest `account.yaml`
    /// cascade used elsewhere in codegen. Same untyped-`Value` shape as
    /// `StateBackend::S3::aws` and `ProjectManifest::aws`. `Value::Null`
    /// preserves today's account.yaml-cascade behavior.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub aws: serde_json::Value,
}

/// Per-job Airflow metadata lifted out of the `airflow:` block on a job file.
/// Includes the shared [`AirflowSection`] overrides plus job-specific fields
/// like `depends_on`.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct AirflowJobBlock {
    pub depends_on: Vec<String>,
    /// Dataset URIs this task produces. Emitted as `outlets=[Dataset(...)]`
    /// on the Airflow operator so downstream DAGs are triggered on completion.
    #[serde(default)]
    pub produces: Vec<String>,
    #[serde(flatten, default)]
    pub overrides: AirflowSection,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn state_backend_s3_no_aws_roundtrip() {
        // Today's yard.yaml (no `aws:` on state) must still parse and
        // serialize back to the same JSON shape. D-02 strictly additive.
        let input = json!({
            "type": "s3",
            "bucket": "my-bucket",
            "region": "us-east-1",
            "key": "state/"
        });
        let parsed: StateBackend = serde_json::from_value(input.clone()).unwrap();
        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reserialized, input, "aws:null must be skipped on serialize");
    }

    #[test]
    fn state_backend_s3_with_aws() {
        let input = json!({
            "type": "s3",
            "bucket": "my-bucket",
            "region": "us-east-1",
            "key": "state/",
            "aws": {
                "assume_role": "arn:aws:iam::111111111111:role/StateAccess",
                "external_id": "xid-1"
            }
        });
        let parsed: StateBackend = serde_json::from_value(input.clone()).unwrap();
        if let StateBackend::S3 { aws, .. } = &parsed {
            assert_eq!(
                aws.get("assume_role").and_then(|v| v.as_str()),
                Some("arn:aws:iam::111111111111:role/StateAccess")
            );
            assert_eq!(
                aws.get("external_id").and_then(|v| v.as_str()),
                Some("xid-1")
            );
        } else {
            panic!("expected StateBackend::S3");
        }
        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reserialized, input);
    }

    #[test]
    fn state_backend_local_unchanged() {
        let input = json!({ "type": "local", "path": ".yard/state" });
        let parsed: StateBackend = serde_json::from_value(input.clone()).unwrap();
        assert!(matches!(parsed, StateBackend::Local { .. }));
        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reserialized, input, "Local variant has no aws field");
    }

    #[test]
    fn airflow_section_no_aws_roundtrip() {
        let input = json!({
            "schedule": "@daily",
            "owner": "data-eng",
            "retries": 2,
            "dags_bucket": null,
            "dags_prefix": null,
            "triggered_by": []
        });
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        let reserialized = serde_json::to_value(&parsed).unwrap();
        // aws must be absent from serialized output when Null
        assert!(
            reserialized.get("aws").is_none(),
            "aws:null must be skipped on serialize for AirflowSection"
        );
    }

    #[test]
    fn airflow_section_with_aws() {
        let input = json!({
            "schedule": "@daily",
            "aws": {
                "assume_role": "arn:aws:iam::222222222222:role/DagUpload",
                "session_name": "yard-dag"
            }
        });
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert_eq!(
            parsed.aws.get("assume_role").and_then(|v| v.as_str()),
            Some("arn:aws:iam::222222222222:role/DagUpload")
        );
        assert_eq!(
            parsed.aws.get("session_name").and_then(|v| v.as_str()),
            Some("yard-dag")
        );
    }

    // --- JobType (TYPE-01) ---

    #[test]
    fn job_type_serialize_lowercase() {
        assert_eq!(serde_json::to_value(JobType::Glue).unwrap(), json!("glue"));
        assert_eq!(serde_json::to_value(JobType::Emr).unwrap(), json!("emr"));
        assert_eq!(serde_json::to_value(JobType::Bash).unwrap(), json!("bash"));
    }

    #[test]
    fn job_type_deserialize_lowercase() {
        let g: JobType = serde_json::from_value(json!("glue")).unwrap();
        assert_eq!(g, JobType::Glue);
        let e: JobType = serde_json::from_value(json!("emr")).unwrap();
        assert_eq!(e, JobType::Emr);
        let b: JobType = serde_json::from_value(json!("bash")).unwrap();
        assert_eq!(b, JobType::Bash);
    }

    #[test]
    fn job_type_deserialize_unknown_rejects() {
        let err = serde_json::from_value::<JobType>(json!("sprk")).unwrap_err();
        assert!(format!("{err}").contains("unknown variant"), "got: {err}");
    }

    #[test]
    fn job_type_from_str_round_trip() {
        use std::str::FromStr;
        for variant in [JobType::Glue, JobType::Emr, JobType::Bash] {
            let s = variant.to_string();
            let back = JobType::from_str(&s).unwrap();
            assert_eq!(back, variant);
        }
    }

    #[test]
    fn job_type_from_str_invalid() {
        use std::str::FromStr;
        let err = JobType::from_str("sprk").unwrap_err();
        assert!(format!("{err}").contains("invalid job type"), "got: {err}");
    }

    #[test]
    fn job_type_display_matches_wire_format() {
        assert_eq!(format!("{}", JobType::Glue), "glue");
        assert_eq!(format!("{}", JobType::Emr), "emr");
        assert_eq!(format!("{}", JobType::Bash), "bash");
    }
}
