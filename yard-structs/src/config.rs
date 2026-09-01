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
#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum JobType {
    /// AWS Glue ETL job.
    Glue,
    /// AWS EMR step.
    Emr,
    /// Shell/bash script execution.
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
/// D-14 — this struct covers ONLY the common fields used at the manifest
/// level. `aws_conn_id` is an explicit override for the Airflow connection
/// id rendered into Glue tasks; when omitted, yard derives it from
/// `assume_role` or falls back to `aws_default`.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct AwsCredentialConfig {
    /// IAM role ARN to assume (e.g. `"arn:aws:iam::111111111111:role/Foo"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assume_role: Option<String>,
    /// STS external ID for cross-account assume-role.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    /// STS session name for assume-role calls.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_name: Option<String>,
    /// AWS region override (e.g. `"us-east-1"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Airflow connection ID override for Glue tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_conn_id: Option<String>,
}

impl AwsCredentialConfig {
    /// Field-by-field shallow merge: each `Some` field in `overlay` wins over
    /// the corresponding field in `self`. Used by the AWS cascade in
    /// `dag_lifecycle::resolve_aws_for_dir` (root yaml ← account.yaml) and
    /// `storage::merge_state_aws_with_env` (yaml ← envs). Mirrors the shape
    /// of `merge_airflow_sections` in yard-core/src/parsing.rs.
    #[must_use]
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
            aws_conn_id: overlay
                .aws_conn_id
                .clone()
                .or_else(|| base.aws_conn_id.clone()),
        }
    }
}

/// Summary of a discovered environment from the `root/{env}/{region}/**`
/// directory convention (D-05, D-06, D-12, D-13). Carries summary data only
/// — name, optional account_id/role_arn, and per-region summaries. Lives in
/// yard-structs so both yard-core (discovery logic) and yard-server
/// (caching/display) share the same type.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DiscoveredEnvironment {
    /// Logical directory name under the project root (e.g. "production").
    /// This is NOT the AWS account ID — see D-12.
    pub name: String,
    /// AWS account ID extracted from account.yaml, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    /// IAM role ARN from account.yaml `aws.assume_role`, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    /// Per-region summaries within this environment.
    pub regions: Vec<RegionSummary>,
}

/// Summary of a single region within a discovered environment.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RegionSummary {
    /// Region directory name (e.g. "us-east-1").
    pub name: String,
    /// Number of job YAML files in this region directory (excluding reserved
    /// files like yard.yaml, account.yaml, region.yaml, transforms.yaml,
    /// dag.yaml).
    pub job_count: u64,
    /// Number of dag.yaml marker files found in this region directory.
    pub dag_count: u64,
    /// Per-job summaries within this region.
    pub jobs: Vec<JobSummary>,
}

/// Lightweight summary of a single job within a region.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct JobSummary {
    /// Job name derived from the YAML filename (without `.yaml` extension).
    pub name: String,
    /// Job type parsed from the `type:` field in the job YAML.
    pub job_type: JobType,
    /// Raw YAML file content, carried through discovery for detail views.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_yaml: Option<String>,
}

/// Backend for persisting per-job and per-DAG state files.
///
/// # Examples
///
/// ```
/// use yard_structs::StateBackend;
///
/// // Local filesystem backend
/// let local: StateBackend = serde_json::from_value(serde_json::json!({
///     "type": "local",
///     "path": ".yard/state"
/// })).expect("local backend should parse");
///
/// // S3 backend
/// let s3: StateBackend = serde_json::from_value(serde_json::json!({
///     "type": "s3",
///     "bucket": "my-state-bucket",
///     "region": "us-east-1",
///     "key": "state/"
/// })).expect("s3 backend should parse");
/// ```
#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum StateBackend {
    /// Local filesystem backend.
    Local {
        /// Directory path for state files.
        path: PathBuf,
    },
    /// AWS S3 backend.
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// AWS region for the S3 bucket.
        region: String,
        /// S3 key prefix for state files.
        key: String,
        /// Optional per-state-backend `aws:` sub-block (TYPE-02). `None` falls
        /// through to `YARD_STATE_AWS_*` envs, then the default AWS credential
        /// provider chain — preserving today's behavior unchanged when unset
        /// (Phase 9 strictly-additive guarantee).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        aws: Option<AwsCredentialConfig>,
    },
}

/// Top-level project manifest parsed from `yard.yaml`.
///
/// # Examples
///
/// ```no_run
/// use yard_structs::ProjectManifest;
///
/// // ProjectManifest is typically deserialized from yard.yaml via
/// // yard-core's parsing layer. Direct field access:
/// # let manifest: ProjectManifest = todo!();
/// println!("project: {}", manifest.project);
/// println!("jobs: {}", manifest.jobs.len());
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ProjectManifest {
    /// Project name used as a namespace in state storage.
    pub project: String,
    /// State backend configuration (local or S3).
    pub state: StateBackend,
    /// Per-provider config, keyed by job type (e.g. "glue", "emr").
    /// Each value is the raw provider config block from yard.yaml.
    #[serde(default)]
    pub providers: HashMap<String, serde_json::Value>,
    /// Job definitions keyed by fully-qualified job name.
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

/// A Python import statement to inject into generated scripts.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Import {
    /// Module or symbol name to import.
    pub name: String,
    /// Optional `from` module (e.g. `from pyspark.sql import SparkSession`).
    pub from: Option<String>,
}

/// Auth strategy for jdbc source/sink. Wire format is a tagged union on `kind`:
/// `kind: rds_iam` selects RDS IAM auth via `boto3 rds.generate_db_auth_token`.
/// May coexist with `secret_id` — in that case the username comes from the
/// secret and `RdsIamAuth.username` must be unset.
#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JdbcAuth {
    /// RDS IAM authentication via `boto3 rds.generate_db_auth_token`.
    RdsIam(RdsIamAuth),
}

/// Configuration for RDS IAM authentication.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct RdsIamAuth {
    /// DB user. Optional only when `secret_id` is also set on the same
    /// source/sink (in which case the username is read from the secret).
    /// Required otherwise. Setting both is a validation error.
    #[serde(default)]
    pub username: Option<String>,
    /// RDS endpoint hostname.
    pub host: String,
    /// RDS endpoint port.
    pub port: u16,
    /// AWS region for the RDS instance.
    pub region: String,
}

/// A data source definition within a job (e.g. S3, JDBC, Glue Catalog).
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Variable name in generated code; produces `df_<name>`.
    pub name: String,
    /// Source type discriminator: `"s3"`, `"jdbc"`, `"catalog"`, `"kafka"`, `"api"`.
    pub source_type: String,
    /// Data format: `"parquet"`, `"csv"`, `"json"`, `"orc"`.
    #[serde(default)]
    pub format: Option<String>,
    /// S3 path for `source_type: s3`.
    #[serde(default)]
    pub path: Option<String>,
    /// JDBC connection URL or Kafka bootstrap servers.
    #[serde(default)]
    pub connection_url: Option<String>,
    /// Table name for `source_type: jdbc` or `source_type: catalog`.
    #[serde(default)]
    pub table: Option<String>,
    /// Database name for `source_type: catalog`.
    #[serde(default)]
    pub database: Option<String>,
    /// AWS Secrets Manager secret ID for credential lookup.
    #[serde(default)]
    pub secret_id: Option<String>,
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
    /// Auth strategy for `source_type: jdbc`. May coexist with `secret_id`
    /// (username from secret, password from auth flow).
    #[serde(default)]
    pub auth: Option<JdbcAuth>,
}

/// A data sink definition within a job (e.g. S3, JDBC, Glue Catalog).
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Sink {
    /// Which DataFrame to write (defaults to first/only source).
    pub source: Option<String>,
    /// Sink type discriminator: `"s3"`, `"jdbc"`, `"catalog"`.
    pub sink_type: String,
    /// Data format: `"parquet"`, `"csv"`, `"json"`, `"orc"`.
    pub format: Option<String>,
    /// S3 path for `sink_type: s3`.
    pub path: Option<String>,
    /// JDBC connection URL for `sink_type: jdbc`.
    pub connection_url: Option<String>,
    /// Table name for `sink_type: jdbc` or `sink_type: catalog`.
    pub table: Option<String>,
    /// Database name for `sink_type: catalog`.
    pub database: Option<String>,
    /// AWS Secrets Manager secret ID for credential lookup.
    pub secret_id: Option<String>,
    /// Write mode: `"overwrite"`, `"append"`, `"error"`.
    pub mode: Option<String>,
    /// Partition columns for the output.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// JDBC connection type (e.g. `"mysql"`, `"postgresql"`).
    #[serde(default)]
    pub connection_type: Option<String>,
    /// For iceberg sinks only: coerce nulls/voids to type-appropriate defaults
    /// before writing (prevents `void`-typed columns from failing the write).
    /// Defaults to true on iceberg. Explicit `false` opts out.
    #[serde(default)]
    pub fill_nulls: Option<bool>,
    /// Auth strategy for `sink_type: jdbc`. May coexist with `secret_id`
    /// (username from secret, password from auth flow).
    #[serde(default)]
    pub auth: Option<JdbcAuth>,
}

/// Column ordering specification for window transforms.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct OrderBySpec {
    /// Column name to order by.
    pub column: String,
    /// If true, sort descending; otherwise ascending.
    pub desc: bool,
}

/// A data transformation step within a job pipeline.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Transform {
    /// Transform type: `"filter"`, `"sql"`, `"drop_columns"`, `"rename"`,
    /// `"select"`, `"add_column"`, `"join"`, `"aggregate"`, `"window"`.
    pub transform_type: String,
    /// Which DataFrame to operate on (defaults to first/only source).
    pub source: Option<String>,
    /// Name for the result DataFrame (defaults to same as source).
    pub output: Option<String>,
    /// Filter condition expression.
    pub condition: Option<String>,
    /// SQL query string.
    pub query: Option<String>,
    /// Column names for `drop_columns` / `select` transforms.
    #[serde(default)]
    pub columns: Vec<String>,
    /// Rename mapping (old name to new name).
    #[serde(default)]
    pub mapping: HashMap<String, String>,
    /// New column name for `add_column` / `window` transforms.
    pub name: Option<String>,
    /// Expression for `add_column` / `window` transforms.
    pub expression: Option<String>,
    /// Left DataFrame name for `join` transforms.
    pub left: Option<String>,
    /// Right DataFrame name for `join` transforms.
    pub right: Option<String>,
    /// Join column name.
    pub on: Option<String>,
    /// Join type: `"inner"`, `"left"`, `"right"`, `"outer"`.
    pub how: Option<String>,
    /// Grouping columns for `aggregate` transforms.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Aggregation expressions keyed by alias (e.g. `"total"` -> `"sum(amount)"`).
    #[serde(default)]
    pub aggs: HashMap<String, String>,
    /// Partition columns for `window` transforms.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Ordering specification for `window` transforms.
    #[serde(default)]
    pub order_by: Vec<OrderBySpec>,
}

/// Complete definition of a single job parsed from a job YAML file.
///
/// # Examples
///
/// ```no_run
/// use yard_structs::JobDefinition;
///
/// // JobDefinition is typically deserialized from a job YAML file via
/// // yard-core's parsing layer. Direct field access:
/// # let job: JobDefinition = todo!();
/// println!("type: {}", job.job_type);
/// println!("sources: {}", job.sources.len());
/// println!("has sink: {}", job.sink.is_some());
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct JobDefinition {
    /// The provider type for this job.
    pub job_type: JobType,
    /// Additional Python imports to inject into generated scripts.
    #[serde(default)]
    pub imports: Vec<Import>,
    /// Inline Python body to embed in the generated script.
    pub body: Option<String>,
    /// Path to an external Python file that replaces YARD's generated script entirely.
    pub job_file: Option<String>,
    /// Data sources read by this job (S3 paths, catalog tables, etc.).
    #[serde(default)]
    pub sources: Vec<Source>,
    /// Output destination (S3 path, catalog table, JDBC endpoint, etc.).
    pub sink: Option<Sink>,
    /// In-flight data transformations applied between source reads and sink write.
    #[serde(default)]
    pub transforms: Vec<Transform>,
    /// PII entity types to detect and redact in the generated script.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mask_pii: Vec<String>,
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
    /// Provider-specific configuration blob (Glue kwargs, EMR config, etc.).
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
    /// Plugin binary version pinned for this job (e.g. `"0.3.1"`). When set
    /// alongside `plugin_source`, yard downloads and uses the plugin binary
    /// instead of a compiled-in provider.
    #[serde(default)]
    pub plugin_version: Option<String>,
    /// URL template for downloading the plugin binary. Placeholders:
    /// `${name}`, `${version}`, `${os}`, `${arch}`.
    #[serde(default)]
    pub plugin_source: Option<String>,
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
            mask_pii: Vec::new(),
            airflow: None,
            partition_by: Vec::new(),
            partition_timestamp_column: None,
            create_timestamp: false,
            config: serde_json::Value::Null,
            dir: PathBuf::new(),
            base_name: String::new(),
            plugin_version: None,
            plugin_source: None,
        }
    }
}

/// Hierarchical context gathered from the directory tree during config resolution.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct YARDContext {
    /// Account-level config from `account.yaml`.
    pub account: serde_json::Value,
    /// Region-level config from `region.yaml`.
    pub region: serde_json::Value,
    /// Shared transforms from `transforms.yaml`.
    pub transforms: serde_json::Value,
    /// Loaded from the optional `dag.yaml` marker file in a job's directory
    /// (or the nearest ancestor). Presence marks the directory as a DAG grouping.
    /// Contents hold DAG-level Airflow config (schedule, default_args, etc).
    pub dag: serde_json::Value,
}

/// Major Airflow version discriminator for version-aware codegen (VCFG-01).
/// Controls import paths and class names in generated DAG files:
/// V2 emits `Dataset` from `airflow.datasets`, V3 emits `Asset` from
/// `airflow.sdk`. Default is V2 to preserve existing behavior.
///
/// Wire format accepts both string (`"2"`, `"3"`) and integer (`2`, `3`)
/// forms from YAML; serialization always emits string form for stable
/// state hashes (VCFG-02, D-07, D-08).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum AirflowMajorVersion {
    /// Airflow 2.x (Dataset, airflow.datasets, airflow.operators.*).
    #[default]
    V2,
    /// Airflow 3.x (Asset, airflow.sdk, airflow.providers.standard.operators.*).
    V3,
}

impl std::fmt::Display for AirflowMajorVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AirflowMajorVersion::V2 => f.write_str("2"),
            AirflowMajorVersion::V3 => f.write_str("3"),
        }
    }
}

impl serde::Serialize for AirflowMajorVersion {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            AirflowMajorVersion::V2 => s.serialize_str("2"),
            AirflowMajorVersion::V3 => s.serialize_str("3"),
        }
    }
}

impl<'de> serde::Deserialize<'de> for AirflowMajorVersion {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v: serde_json::Value = serde_json::Value::deserialize(d)?;
        match &v {
            serde_json::Value::String(s) => match s.as_str() {
                "2" => Ok(AirflowMajorVersion::V2),
                "3" => Ok(AirflowMajorVersion::V3),
                other => Err(serde::de::Error::custom(format!(
                    "invalid airflow version '{other}' \u{2014} valid: 2 or 3 (string or integer)"
                ))),
            },
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_u64() {
                    match i {
                        2 => Ok(AirflowMajorVersion::V2),
                        3 => Ok(AirflowMajorVersion::V3),
                        _ => Err(serde::de::Error::custom(format!(
                            "invalid airflow version '{i}' \u{2014} valid: 2 or 3 (string or integer)"
                        ))),
                    }
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid airflow version '{n}' \u{2014} valid: 2 or 3 (string or integer)"
                    )))
                }
            }
            _ => Err(serde::de::Error::custom(format!(
                "invalid airflow version '{}' \u{2014} valid: 2 or 3 (string or integer)",
                v
            ))),
        }
    }
}

/// Airflow config shared across inheritance layers (yard.yaml, region.yaml,
/// account.yaml, dag.yaml, and the per-job `airflow:` block). Every layer has
/// the same shape; later layers override earlier ones via shallow merge.
///
/// # Examples
///
/// ```
/// use yard_structs::AirflowSection;
///
/// let section: AirflowSection = serde_json::from_value(serde_json::json!({
///     "schedule": "@daily",
///     "owner": "data-eng",
///     "retries": 2
/// })).expect("airflow section should parse");
/// assert_eq!(section.schedule.as_deref(), Some("@daily"));
/// assert_eq!(section.owner.as_deref(), Some("data-eng"));
/// assert_eq!(section.retries, Some(2));
/// ```
///
/// Phase 28: `triggered_by: Vec<String>` was removed in favor of the typed
/// `trigger: Option<Trigger>` field, and `publishes: Vec<String>` now carries
/// what `triggered_by` used to before the rename. The hand-rolled Deserialize
/// impl below intercepts legacy `triggered_by:` keys and emits an actionable
/// rename-pointer error (D-21).
#[derive(Debug, Serialize, Clone, Default, PartialEq)]
pub struct AirflowSection {
    /// Cron or preset schedule expression for the DAG (e.g. `"@daily"`).
    pub schedule: Option<String>,
    /// DAG owner name rendered in Airflow metadata.
    pub owner: Option<String>,
    /// Number of task-level retries before marking a run as failed.
    pub retries: Option<i32>,
    /// S3 bucket where generated DAG files are uploaded.
    pub dags_bucket: Option<String>,
    /// S3 key prefix within `dags_bucket` for DAG file uploads.
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
    /// CONC-01: DAG-level Airflow knob. None preserves Airflow's default of 16
    /// for schedule-only DAGs. Event-driven DAGs (`trigger.is_some()`) auto-default
    /// to 1 at codegen time when this is None — see triggers.rs::render_trigger.
    /// User override via `airflow.max_active_runs: <N>` always wins.
    /// CONC-02 enforces `>= 1` at validate_dag_full.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_active_runs: Option<u32>,
    /// Major Airflow version for version-aware codegen (VCFG-01/02).
    /// `None` = V2 default. Controls `Dataset` vs `Asset` class names and
    /// import paths in Phase 56.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<AirflowMajorVersion>,
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
    #[serde(default)]
    max_active_runs: Option<u32>,
    #[serde(default)]
    version: Option<AirflowMajorVersion>,
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
            max_active_runs: raw.max_active_runs,
            version: raw.version,
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
    /// Upstream task names this job depends on within its DAG.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Dataset URIs this task publishes. Emitted as `outlets=[Dataset(...)]`
    /// on the Airflow operator so downstream DAGs are triggered on completion.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub publishes: Vec<String>,
    /// Inherited Airflow config overrides (schedule, owner, retries, etc.)
    /// flattened from the parent `AirflowSection` cascade.
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
            aws_conn_id: None,
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
            aws_conn_id: None,
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
            aws_conn_id: None,
        };
        let overlay = AwsCredentialConfig {
            assume_role: Some("overlay-role".to_string()),
            external_id: None,
            session_name: Some("overlay-name".to_string()),
            region: None,
            aws_conn_id: None,
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

    // --- DiscoveredEnvironment / RegionSummary / JobSummary (Phase 40) ---

    #[test]
    fn discovered_environment_serde_round_trip() {
        let env = DiscoveredEnvironment {
            name: "production".to_string(),
            account_id: Some("123456789012".to_string()),
            role_arn: Some("arn:aws:iam::123456789012:role/YardServer".to_string()),
            regions: vec![RegionSummary {
                name: "us-east-1".to_string(),
                job_count: 3,
                dag_count: 1,
                jobs: vec![
                    JobSummary {
                        name: "orders".to_string(),
                        job_type: JobType::Glue,
                        config_yaml: None,
                    },
                    JobSummary {
                        name: "deploy".to_string(),
                        job_type: JobType::Bash,
                        config_yaml: None,
                    },
                ],
            }],
        };
        let serialized = serde_json::to_value(&env).unwrap();
        let deserialized: DiscoveredEnvironment =
            serde_json::from_value(serialized.clone()).unwrap();
        assert_eq!(deserialized, env);
        // Verify structure
        assert_eq!(serialized["name"], "production");
        assert_eq!(serialized["account_id"], "123456789012");
        assert_eq!(serialized["regions"][0]["name"], "us-east-1");
        assert_eq!(serialized["regions"][0]["job_count"], 3);
        assert_eq!(serialized["regions"][0]["dag_count"], 1);
        assert_eq!(serialized["regions"][0]["jobs"][0]["name"], "orders");
        assert_eq!(serialized["regions"][0]["jobs"][0]["job_type"], "glue");
    }

    // --- AirflowMajorVersion (Phase 55, VCFG-01/02/04/05) ---

    #[test]
    fn airflow_major_version_default_is_v2() {
        assert_eq!(AirflowMajorVersion::default(), AirflowMajorVersion::V2);
    }

    #[test]
    fn airflow_major_version_serde_string_round_trip() {
        // D-07/D-08: accepts string "2"/"3", serializes as string
        for (input_str, expected) in [("2", AirflowMajorVersion::V2), ("3", AirflowMajorVersion::V3)] {
            let input = json!(input_str);
            let parsed: AirflowMajorVersion = serde_json::from_value(input).unwrap();
            assert_eq!(parsed, expected);
            let reser = serde_json::to_value(parsed).unwrap();
            assert_eq!(reser, json!(input_str), "must serialize back to string form");
        }
    }

    #[test]
    fn airflow_major_version_serde_integer_round_trip() {
        // D-07: accepts integer 2/3 from YAML
        for (input_int, expected) in [(2, AirflowMajorVersion::V2), (3, AirflowMajorVersion::V3)] {
            let input = json!(input_int);
            let parsed: AirflowMajorVersion = serde_json::from_value(input).unwrap();
            assert_eq!(parsed, expected);
            // D-08: serialization always emits string form
            let reser = serde_json::to_value(parsed).unwrap();
            assert_eq!(reser, json!(input_int.to_string()), "integer input must serialize as string");
        }
    }

    #[test]
    fn airflow_major_version_invalid_string_rejected() {
        // D-09: actionable error for invalid version values
        for invalid in ["4", "foo"] {
            let err = serde_json::from_value::<AirflowMajorVersion>(json!(invalid)).unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.contains(&format!("invalid airflow version '{invalid}'")),
                "error for '{invalid}' must contain the invalid value, got: {msg}"
            );
            assert!(
                msg.contains("valid: 2 or 3 (string or integer)"),
                "error for '{invalid}' must mention valid options, got: {msg}"
            );
        }
    }

    #[test]
    fn airflow_section_version_none_omitted_in_serialization() {
        // VCFG-04: skip_serializing_if = "Option::is_none" preserves wire format
        let input = json!({"schedule": "@daily"});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert_eq!(parsed.version, None);
        let reser = serde_json::to_value(&parsed).unwrap();
        assert!(
            reser.get("version").is_none(),
            "version:None must be skipped on serialize, got: {reser}"
        );
    }

    #[test]
    fn airflow_section_version_v3_round_trips() {
        let input = json!({"schedule": "@daily", "version": "3"});
        let parsed: AirflowSection = serde_json::from_value(input).unwrap();
        assert_eq!(parsed.version, Some(AirflowMajorVersion::V3));
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser.get("version"), Some(&json!("3")));
    }

    #[test]
    fn discovered_environment_optional_fields_skipped() {
        let env = DiscoveredEnvironment {
            name: "dev".to_string(),
            account_id: None,
            role_arn: None,
            regions: vec![],
        };
        let serialized = serde_json::to_value(&env).unwrap();
        assert!(serialized.get("account_id").is_none());
        assert!(serialized.get("role_arn").is_none());
        let deserialized: DiscoveredEnvironment =
            serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized, env);
    }

    // --- mask_pii serde (CFG-01, CFG-02, CFG-03) ---

    #[test]
    fn mask_pii_empty_omitted_from_json() {
        // CFG-02/CFG-03: skip_serializing_if = "Vec::is_empty" must omit
        // the mask_pii key when the vec is empty.
        let job = JobDefinition::default();
        assert!(job.mask_pii.is_empty());
        let serialized = serde_json::to_value(&job).unwrap();
        assert!(
            serialized.get("mask_pii").is_none(),
            "empty mask_pii must be omitted from JSON output, got: {serialized}"
        );
    }

    #[test]
    fn mask_pii_present_round_trips() {
        // CFG-01/CFG-03: a populated mask_pii vec must serialize with the
        // key present and round-trip back to the same elements.
        let job = JobDefinition {
            mask_pii: vec!["USA_SSN".into(), "CREDIT_CARD".into()],
            ..Default::default()
        };
        let serialized = serde_json::to_value(&job).unwrap();
        let mask_pii_val = serialized
            .get("mask_pii")
            .expect("mask_pii key must be present when non-empty");
        assert_eq!(mask_pii_val, &json!(["USA_SSN", "CREDIT_CARD"]));

        // Round-trip: deserialize back and verify
        let deserialized: JobDefinition =
            serde_json::from_value(serialized).unwrap();
        assert_eq!(deserialized.mask_pii.len(), 2);
        assert_eq!(deserialized.mask_pii[0], "USA_SSN");
        assert_eq!(deserialized.mask_pii[1], "CREDIT_CARD");
    }

    #[test]
    fn job_definition_plugin_fields_deserialize() {
        let input = json!({
            "job_type": "glue",
            "config": {},
            "plugin_version": "0.3.1",
            "plugin_source": "https://example.com/${name}-${version}"
        });
        let job: JobDefinition = serde_json::from_value(input).unwrap();
        assert_eq!(job.plugin_version.as_deref(), Some("0.3.1"));
        assert_eq!(
            job.plugin_source.as_deref(),
            Some("https://example.com/${name}-${version}")
        );
    }

    #[test]
    fn job_definition_without_plugin_fields_backward_compat() {
        let input = json!({
            "job_type": "glue",
            "config": {}
        });
        let job: JobDefinition = serde_json::from_value(input).unwrap();
        assert!(job.plugin_version.is_none());
        assert!(job.plugin_source.is_none());
    }

    #[test]
    fn job_definition_default_has_no_plugin_fields() {
        let job = JobDefinition::default();
        assert!(job.plugin_version.is_none());
        assert!(job.plugin_source.is_none());
    }
}
