use crate::trigger::Trigger;
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

/// Typed AWS credential configuration (TYPE-02). Replaces the previous
/// `aws: serde_json::Value` blob on `StateBackend::S3`, `AirflowSection`,
/// `ProjectManifest`, and `DagState`.
///
/// Wire format is byte-equal to today's untyped shape: each field uses
/// `#[serde(default, skip_serializing_if = "Option::is_none")]` so absent
/// keys stay absent on round-trip. `None` at the field-set level means
/// "no override — fall through to env vars / default AWS credential
/// provider chain", identical to today's `Value::Null` semantic.
///
/// Provider-specific extension fields (per-provider AWS knobs) keep their
/// `serde_json::Value` envelope inside `JobDefinition.config: Value` per
/// D-14 — this struct covers ONLY the common four fields used at the
/// manifest level.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct AwsCredentialConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

impl AwsCredentialConfig {
    /// Field-by-field shallow merge: each `Some` field in `overlay` wins over
    /// the corresponding field in `self`. Used by the AWS cascade in
    /// `dag_lifecycle::resolve_aws_for_dir` (root yaml ← account.yaml) and
    /// `storage::merge_state_aws_with_env` (yaml ← envs). Mirrors the shape
    /// of `merge_airflow_sections` in yard-core/src/parsing.rs.
    pub fn merge(base: &Self, overlay: &Self) -> Self {
        Self {
            assume_role: overlay
                .assume_role
                .clone()
                .or_else(|| base.assume_role.clone()),
            external_id: overlay
                .external_id
                .clone()
                .or_else(|| base.external_id.clone()),
            session_name: overlay
                .session_name
                .clone()
                .or_else(|| base.session_name.clone()),
            region: overlay.region.clone().or_else(|| base.region.clone()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum StateBackend {
    Local {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        region: String,
        key: String,
        /// Optional per-state-backend `aws:` sub-block (TYPE-02). `None` falls
        /// through to `YARD_STATE_AWS_*` envs, then the default AWS credential
        /// provider chain — preserving today's behavior unchanged when unset
        /// (Phase 9 strictly-additive guarantee).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aws: Option<AwsCredentialConfig>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProjectManifest {
    pub project: String,
    pub state: StateBackend,
    /// Per-provider config, keyed by job type (e.g. "glue", "emr").
    /// Each value is the raw provider config block from yard.yaml.
    #[serde(default)]
    pub providers: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub jobs: HashMap<String, JobDefinition>,
    /// Root-level `aws:` block (TYPE-02). Per-job and per-DAG account.yaml
    /// `aws:` blocks shallow-override this. `None` falls through to the
    /// default AWS credential provider chain.
    ///
    /// Wire-format note: under the prior untyped shape this field had only
    /// `#[serde(default)]` (no `skip_serializing_if`), so unset values
    /// serialized as the literal `"aws": null`. With the typed
    /// `Option<AwsCredentialConfig>` + `skip_serializing_if = "Option::is_none"`
    /// the field is now omitted entirely on serialize when `None` —
    /// intentional alignment with `StateBackend::S3.aws`,
    /// `AirflowSection.aws`, and `DagState.aws`, all of which already
    /// skip on the unset path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsCredentialConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Import {
    pub name: String,
    pub from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
///
/// Phase 28: `triggered_by: Vec<String>` was removed in favor of the typed
/// `trigger: Option<Trigger>` field, and `publishes: Vec<String>` now carries
/// what `triggered_by` used to before the rename. The hand-rolled Deserialize
/// impl below intercepts legacy `triggered_by:` keys and emits an actionable
/// rename-pointer error (D-21).
#[derive(Debug, Serialize, Clone, Default, PartialEq)]
pub struct AirflowSection {
    pub schedule: Option<String>,
    pub owner: Option<String>,
    pub retries: Option<i32>,
    pub dags_bucket: Option<String>,
    pub dags_prefix: Option<String>,
    /// Typed event-driven trigger (S3 file drop, Airflow Dataset, SQS,
    /// manual API, or `all:` / `any:` composite). Mutually exclusive with
    /// `schedule:` — validation enforced in Phase 29. `None` means
    /// "schedule-only DAG" (existing behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<Trigger>,
    /// Dataset URIs published by THIS DAG (DAG-level outlets). Drives
    /// downstream Dataset-triggered DAGs. Empty by default; rendered only
    /// when non-empty. Per-task `publishes:` lives on `AirflowJobBlock`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publishes: Vec<String>,
    /// Optional per-airflow-provider `aws:` sub-block for the DAG upload bucket.
    /// When set, takes priority over the root `aws:` + nearest `account.yaml`
    /// cascade used elsewhere in codegen. `None` preserves today's
    /// account.yaml-cascade behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws: Option<AwsCredentialConfig>,
}

/// Private mirror of `AirflowSection` used by the hand-rolled `Deserialize`
/// impl below. Carries `#[serde(deny_unknown_fields)]` so unknown keys (other
/// than the explicitly-intercepted `triggered_by:` rename pointer) still
/// surface as serde "unknown field" errors. This preserves the
/// `airflow_section_deny_unknown_fields` test contract.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct _AirflowSectionRaw {
    #[serde(default)]
    schedule: Option<String>,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    retries: Option<i32>,
    #[serde(default)]
    dags_bucket: Option<String>,
    #[serde(default)]
    dags_prefix: Option<String>,
    #[serde(default)]
    trigger: Option<Trigger>,
    #[serde(default)]
    publishes: Vec<String>,
    #[serde(default)]
    aws: Option<AwsCredentialConfig>,
}

impl<'de> serde::Deserialize<'de> for AirflowSection {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v: serde_json::Value = serde_json::Value::deserialize(d)?;
        if let Some(obj) = v.as_object()
            && obj.contains_key("triggered_by")
        {
            return Err(serde::de::Error::custom(
                "unknown field 'triggered_by' — use 'trigger: { dataset: { uri: \"...\" } }' instead. \
                 For multiple URIs, use 'trigger: { all: [{ dataset: ... }, ...] }'. See migration guide.",
            ));
        }
        let raw: _AirflowSectionRaw =
            serde_json::from_value(v).map_err(serde::de::Error::custom)?;
        Ok(AirflowSection {
            schedule: raw.schedule,
            owner: raw.owner,
            retries: raw.retries,
            dags_bucket: raw.dags_bucket,
            dags_prefix: raw.dags_prefix,
            trigger: raw.trigger,
            publishes: raw.publishes,
            aws: raw.aws,
        })
    }
}

/// Per-job Airflow metadata lifted out of the `airflow:` block on a job file.
/// Includes the shared [`AirflowSection`] overrides plus job-specific fields
/// like `depends_on`.
///
/// Phase 28: `produces: Vec<String>` was renamed to `publishes: Vec<String>`.
/// The hand-rolled Deserialize impl below intercepts legacy `produces:` keys
/// and emits an actionable rename-pointer error (D-21).
#[derive(Debug, Serialize, Clone, Default, PartialEq)]
pub struct AirflowJobBlock {
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Dataset URIs this task publishes. Emitted as `outlets=[Dataset(...)]`
    /// on the Airflow operator so downstream DAGs are triggered on completion.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publishes: Vec<String>,
    #[serde(flatten, default)]
    pub overrides: AirflowSection,
}

/// Private mirror of `AirflowJobBlock` used by the hand-rolled `Deserialize`
/// impl below. Does NOT carry `#[serde(deny_unknown_fields)]` because
/// `#[serde(flatten)]` is incompatible with the deny gate at the same struct
/// level. Unknown fields not adjacent to `flatten` flow THROUGH the flatten
/// into `_AirflowSectionRaw`'s `deny_unknown_fields`, where they surface as
/// serde "unknown field" errors. The
/// `airflow_job_block_unrelated_unknown_field_still_rejected` test locks
/// this invariant.
#[derive(Deserialize)]
struct _AirflowJobBlockRaw {
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    publishes: Vec<String>,
    #[serde(flatten, default)]
    overrides: AirflowSection,
}

impl<'de> serde::Deserialize<'de> for AirflowJobBlock {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v: serde_json::Value = serde_json::Value::deserialize(d)?;
        if let Some(obj) = v.as_object()
            && obj.contains_key("produces")
        {
            return Err(serde::de::Error::custom(
                "unknown field 'produces' — use 'publishes: [...]' instead. See migration guide.",
            ));
        }
        let raw: _AirflowJobBlockRaw =
            serde_json::from_value(v).map_err(serde::de::Error::custom)?;
        Ok(AirflowJobBlock {
            depends_on: raw.depends_on,
            publishes: raw.publishes,
            overrides: raw.overrides,
        })
    }
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
            let creds = aws.as_ref().expect("aws field must parse to Some");
            assert_eq!(
                creds.assume_role.as_deref(),
                Some("arn:aws:iam::111111111111:role/StateAccess")
            );
            assert_eq!(creds.external_id.as_deref(), Some("xid-1"));
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
            "dags_prefix": null
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
        let creds = parsed.aws.as_ref().expect("aws field must parse to Some");
        assert_eq!(
            creds.assume_role.as_deref(),
            Some("arn:aws:iam::222222222222:role/DagUpload")
        );
        assert_eq!(creds.session_name.as_deref(), Some("yard-dag"));
    }

    // --- AirflowSection.max_active_runs (Phase 30, plan 30-01, D-13 / CONC-01) ---

    #[test]
    fn airflow_section_no_max_active_runs_roundtrip() {
        // PRES-05: when max_active_runs is unset, it must NOT appear in
        // serialized output (skip_serializing_if). Parses to None.
        let input = json!({"schedule": "@daily"});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert_eq!(parsed.max_active_runs, None);
        let reser = serde_json::to_value(&parsed).unwrap();
        assert!(
            reser.get("max_active_runs").is_none(),
            "max_active_runs:None must be skipped on serialize, got: {reser}"
        );
    }

    #[test]
    fn airflow_section_with_max_active_runs() {
        let input = json!({"schedule": "@daily", "max_active_runs": 4});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert_eq!(parsed.max_active_runs, Some(4));
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser.get("max_active_runs"), Some(&json!(4)));
    }

    // --- AwsCredentialConfig (TYPE-02) ---

    #[test]
    fn aws_credential_config_default_round_trip() {
        let creds = AwsCredentialConfig::default();
        let serialized = serde_json::to_value(&creds).unwrap();
        // All None → empty object on serialize.
        assert_eq!(serialized, json!({}));
        let parsed: AwsCredentialConfig = serde_json::from_value(serialized).unwrap();
        assert_eq!(parsed, creds);
    }

    #[test]
    fn aws_credential_config_full_round_trip() {
        let creds = AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::111111111111:role/Foo".to_string()),
            external_id: Some("xid-1".to_string()),
            session_name: Some("yard-test".to_string()),
            region: Some("us-east-1".to_string()),
        };
        let serialized = serde_json::to_value(&creds).unwrap();
        assert_eq!(
            serialized,
            json!({
                "assume_role": "arn:aws:iam::111111111111:role/Foo",
                "external_id": "xid-1",
                "session_name": "yard-test",
                "region": "us-east-1",
            })
        );
        let parsed: AwsCredentialConfig = serde_json::from_value(serialized).unwrap();
        assert_eq!(parsed, creds);
    }

    #[test]
    fn aws_credential_config_partial_skips_none() {
        let creds = AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::111111111111:role/Foo".to_string()),
            external_id: None,
            session_name: None,
            region: None,
        };
        let serialized = serde_json::to_value(&creds).unwrap();
        assert_eq!(
            serialized,
            json!({"assume_role": "arn:aws:iam::111111111111:role/Foo"})
        );
    }

    #[test]
    fn aws_credential_config_merge_overlay_wins() {
        let base = AwsCredentialConfig {
            assume_role: Some("base-role".to_string()),
            external_id: Some("base-eid".to_string()),
            session_name: None,
            region: Some("us-east-1".to_string()),
        };
        let overlay = AwsCredentialConfig {
            assume_role: Some("overlay-role".to_string()),
            external_id: None,
            session_name: Some("overlay-name".to_string()),
            region: None,
        };
        let merged = AwsCredentialConfig::merge(&base, &overlay);
        // overlay Some wins over base Some
        assert_eq!(merged.assume_role.as_deref(), Some("overlay-role"));
        // overlay None falls through to base Some
        assert_eq!(merged.external_id.as_deref(), Some("base-eid"));
        // base None falls through to overlay Some
        assert_eq!(merged.session_name.as_deref(), Some("overlay-name"));
        // overlay None falls through to base Some
        assert_eq!(merged.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn aws_credential_config_merge_both_none_yields_default() {
        let merged =
            AwsCredentialConfig::merge(&AwsCredentialConfig::default(), &AwsCredentialConfig::default());
        assert_eq!(merged, AwsCredentialConfig::default());
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

    // --- deny_unknown_fields (TYPE-03) ---
    //
    // Each of these tests exercises the structural deny gate at the serde-
    // derived deserialize path (storage.rs's state-file persistence flows
    // through these structs). User yard.yaml typo coverage at the manual
    // `Value`-extraction layer in parsing.rs is exercised by the integration
    // test at yard-core/tests/typed_config_validation.rs + the inline tests
    // in yard-core/src/parsing.rs (D-17).

    #[test]
    fn project_manifest_deny_unknown_fields() {
        let input = json!({
            "project": "test",
            "state": {"type": "local", "path": ".yard/state"},
            "providers": {},
            "jobs": {},
            "wat": "this is unknown",
        });
        let err = serde_json::from_value::<ProjectManifest>(input).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown field"), "got: {msg}");
        assert!(msg.contains("wat"), "got: {msg}");
    }

    #[test]
    fn state_backend_s3_deny_unknown_fields() {
        let input = json!({
            "type": "s3", "bucket": "b", "region": "us-east-1", "key": "k/",
            "wat": "unknown"
        });
        let err = serde_json::from_value::<StateBackend>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn state_backend_local_deny_unknown_fields() {
        let input = json!({"type": "local", "path": ".yard/state", "wat": "unknown"});
        let err = serde_json::from_value::<StateBackend>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn airflow_section_deny_unknown_fields() {
        let input = json!({"schedule": "@daily", "wat": 1});
        let err = serde_json::from_value::<AirflowSection>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn source_deny_unknown_fields() {
        let input = json!({
            "name": "foo",
            "source_type": "s3",
            "wat": "unknown"
        });
        let err = serde_json::from_value::<Source>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn sink_deny_unknown_fields() {
        let input = json!({
            "sink_type": "s3",
            "wat": "unknown"
        });
        let err = serde_json::from_value::<Sink>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn transform_deny_unknown_fields() {
        let input = json!({
            "transform_type": "filter",
            "wat": "unknown"
        });
        let err = serde_json::from_value::<Transform>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn job_definition_deny_unknown_fields() {
        let input = json!({
            "job_type": "glue",
            "imports": [],
            "body": null,
            "job_file": null,
            "sources": [],
            "sink": null,
            "transforms": [],
            "airflow": null,
            "config": null,
            "wat": "unknown"
        });
        let err = serde_json::from_value::<JobDefinition>(input).unwrap_err();
        assert!(format!("{err}").contains("unknown field"));
    }

    #[test]
    fn airflow_section_subset_still_parses() {
        // Sanity: deny_unknown_fields rejects unknowns but accepts subsets
        // (skip_serializing_if + #[serde(default)] mean missing keys are fine).
        let input = json!({"schedule": "@hourly"});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert_eq!(parsed.schedule.as_deref(), Some("@hourly"));
        assert!(parsed.owner.is_none());
    }

    // --- Phase 28: typed Trigger model rewire (TRIG-01..TRIG-03) ---

    #[test]
    fn airflow_section_schedule_only_byte_equal_v1_5_fixture() {
        // PRES-05: a representative v1.5 schedule-only DAG (no `trigger:`,
        // no `publishes:`) must round-trip byte-identically post-Phase-28.
        // NOTE: AirflowSection's existing Option fields (schedule, owner,
        // retries, dags_bucket, dags_prefix) do NOT have skip_serializing_if,
        // so unset values serialize as `null` — preserving the v1.5 wire
        // format that wire_format_compat.rs locks. This test mirrors that
        // shape: input includes explicit nulls for the unset Option fields
        // so round-trip is byte-equal.
        let input = json!({
            "schedule": "@daily",
            "owner": "data-eng",
            "retries": 2,
            "dags_bucket": null,
            "dags_prefix": null
        });
        let parsed: AirflowSection = serde_json::from_value(input.clone()).unwrap();
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input, "schedule-only AirflowSection wire format drift");
        // Phase 28 invariants: trigger is None, publishes is empty, both
        // skipped on serialize.
        assert!(parsed.trigger.is_none());
        assert!(parsed.publishes.is_empty());
        assert!(reser.get("trigger").is_none());
        assert!(reser.get("publishes").is_none());
    }

    #[test]
    fn airflow_section_with_trigger_s3_round_trip() {
        let input = json!({
            "schedule": null,
            "owner": null,
            "retries": null,
            "dags_bucket": null,
            "dags_prefix": null,
            "trigger": {"s3": {"bucket": "x", "prefix": "y"}}
        });
        let parsed: AirflowSection = serde_json::from_value(input.clone()).unwrap();
        match &parsed.trigger {
            Some(crate::trigger::Trigger::Single(crate::trigger::SingleSource::S3(s3))) => {
                assert_eq!(s3.bucket, "x");
                assert_eq!(s3.prefix.as_deref(), Some("y"));
                assert!(s3.key.is_none());
            }
            other => panic!("expected Trigger::Single(S3 {{...}}), got {other:?}"),
        }
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn airflow_section_with_trigger_all_dataset_round_trip_sorted() {
        // HASH-02: composite all/any lists serialize in sorted-by-element
        // canonical-JSON-string order regardless of input order.
        let input = json!({
            "trigger": {"all": [
                {"dataset": {"uri": "b"}},
                {"dataset": {"uri": "a"}}
            ]}
        });
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        let reser = serde_json::to_value(&parsed).unwrap();
        // Expect sorted output: a, b. The Option fields serialize as null
        // (no skip_serializing_if on schedule/owner/retries/dags_bucket/
        // dags_prefix per existing v1.5 wire format).
        let expected_sorted = json!({
            "schedule": null,
            "owner": null,
            "retries": null,
            "dags_bucket": null,
            "dags_prefix": null,
            "trigger": {"all": [
                {"dataset": {"uri": "a"}},
                {"dataset": {"uri": "b"}}
            ]}
        });
        assert_eq!(
            reser, expected_sorted,
            "Trigger::All must serialize in sorted order"
        );
    }

    #[test]
    fn airflow_section_with_publishes_round_trip() {
        let input = json!({
            "schedule": "@daily",
            "owner": null,
            "retries": null,
            "dags_bucket": null,
            "dags_prefix": null,
            "publishes": ["s3://x/y", "s3://a/b"]
        });
        let parsed: AirflowSection = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(parsed.publishes, vec!["s3://x/y", "s3://a/b"]);
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input, "publishes preserves input order (no sort)");
    }

    #[test]
    fn airflow_section_publishes_empty_skipped() {
        let input = json!({"schedule": "@daily"});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert!(parsed.publishes.is_empty());
        let reser = serde_json::to_value(&parsed).unwrap();
        assert!(
            reser.get("publishes").is_none(),
            "empty publishes must be skipped on serialize"
        );
    }

    #[test]
    fn airflow_section_trigger_none_skipped() {
        let input = json!({"schedule": "@daily"});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert!(parsed.trigger.is_none());
        let reser = serde_json::to_value(&parsed).unwrap();
        assert!(
            reser.get("trigger").is_none(),
            "None trigger must be skipped on serialize (PRES-05)"
        );
    }

    #[test]
    fn airflow_section_triggered_by_returns_rename_error() {
        let input = json!({"triggered_by": ["s3://x"]});
        let err = serde_json::from_value::<AirflowSection>(input).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown field 'triggered_by'"),
            "got: {msg}"
        );
        assert!(msg.contains("use 'trigger:"), "got: {msg}");
    }

    #[test]
    fn airflow_job_block_produces_returns_rename_error() {
        let input = json!({"depends_on": [], "produces": ["s3://x"]});
        let err = serde_json::from_value::<AirflowJobBlock>(input).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown field 'produces'"), "got: {msg}");
        assert!(msg.contains("use 'publishes:"), "got: {msg}");
    }

    #[test]
    fn airflow_job_block_publishes_round_trip() {
        // The flatten-overrides path means AirflowSection's Option fields
        // (schedule/owner/retries/dags_bucket/dags_prefix) flatten into the
        // AirflowJobBlock JSON shape and serialize as null when unset (no
        // skip_serializing_if on those fields — preserves v1.5 wire format).
        let input = json!({
            "depends_on": ["t1"],
            "publishes": ["s3://x/y"],
            "schedule": null,
            "owner": null,
            "retries": null,
            "dags_bucket": null,
            "dags_prefix": null
        });
        let parsed: AirflowJobBlock = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(parsed.publishes, vec!["s3://x/y"]);
        assert_eq!(parsed.depends_on, vec!["t1"]);
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn airflow_job_block_publishes_empty_skipped() {
        let input = json!({"depends_on": []});
        let parsed: AirflowJobBlock = serde_json::from_value(input).unwrap();
        let reser = serde_json::to_value(&parsed).unwrap();
        assert!(
            reser.get("publishes").is_none(),
            "empty publishes on AirflowJobBlock must be skipped"
        );
    }

    #[test]
    fn airflow_job_block_unrelated_unknown_field_still_rejected() {
        // Locks T-28-01-05: deny_unknown_fields path through _AirflowSectionRaw
        // survives the custom-Deserialize rewrite. Typos at the per-job airflow
        // block scope still surface as serde "unknown field" errors, not silently
        // swallowed by the new custom Deserialize.
        //
        // Note: serde's actual error format uses backticks (`typo_field`),
        // not single quotes. The plan's locked invariant is "the substring
        // 'unknown field' AND the field name 'typo_field' both appear" —
        // we assert both substrings without quote-style coupling. This
        // mirrors the existing `airflow_section_deny_unknown_fields` and
        // `project_manifest_deny_unknown_fields` test idiom.
        let input = json!({"depends_on": [], "typo_field": 1});
        let err = serde_json::from_value::<AirflowJobBlock>(input).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown field"), "got: {msg}");
        assert!(msg.contains("typo_field"), "got: {msg}");
    }
}
