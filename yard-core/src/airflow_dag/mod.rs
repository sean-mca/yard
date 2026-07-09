//! Airflow DAG codegen: groups jobs by their nearest `dag.yaml` marker,
//! resolves the Airflow config inheritance chain, and renders Python DAG
//! files via Tera.
//!
//! Scope for PR 1b is pure codegen — no S3 upload, no state tracking. The
//! apply/plan wiring lands in PR 1c.

mod collection;
mod connections;
mod generation;
mod helpers;
mod resolve;
mod triggers;
mod version;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use yard_structs::AirflowSection;
use yard_structs::JobState;

// Re-export sub-module items as public API
pub use collection::collect_dags;
pub use connections::{derive_aws_conn_id, required_connections_for_dag};
pub use connections::parse_account_from_role_arn;
pub use generation::generate_dag;
pub use helpers::validate_orphan_airflow_blocks;

// Re-imports only needed by `use super::*` in the test module
#[cfg(test)]
use helpers::sanitize_identifier;
#[cfg(test)]
use std::path::Path;
#[cfg(test)]
use yard_structs::JobDefinition;

const DEFAULT_AWS_CONN_ID: &str = "aws_default";

// CRITICAL: path changed from "templates/airflow_dag.py.tera" to "../templates/airflow_dag.py.tera"
// because mod.rs is now at src/airflow_dag/mod.rs instead of src/airflow_dag.rs
const AIRFLOW_DAG_TEMPLATE: &str = include_str!("../templates/airflow_dag.py.tera");

/// Airflow connection required by a DAG so a task can invoke AWS APIs under a
/// cross-account role. The DAG-codegen layer does not manage connections --
/// this struct is emitted alongside the rendered DAG so operators can set them
/// up in MWAA.
///
/// # Examples
///
/// ```
/// use yard_core::airflow_dag::RequiredConnection;
///
/// let conn = RequiredConnection {
///     conn_id: "yard_123456789012_MyRole".to_string(),
///     role_arn: "arn:aws:iam::123456789012:role/MyRole".to_string(),
/// };
/// assert_eq!(conn.conn_id, "yard_123456789012_MyRole");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequiredConnection {
    /// Airflow connection identifier (e.g. `"aws_conn_123456789012"`).
    pub conn_id: String,
    /// IAM role ARN assumed via this connection for cross-account access.
    pub role_arn: String,
}

/// A DAG resolved from filesystem discovery + validation. Ready for codegen.
///
/// # Examples
///
/// ```no_run
/// use std::collections::BTreeMap;
/// use std::path::PathBuf;
/// use yard_structs::AirflowSection;
/// use yard_core::airflow_dag::ResolvedDag;
///
/// let dag = ResolvedDag {
///     name: "myproj_orders_pipeline".to_string(),
///     dir: PathBuf::from("projects/orders"),
///     config: AirflowSection::default(),
///     tasks: vec!["extract".to_string(), "transform".to_string()],
///     depends_on: BTreeMap::from([
///         ("extract".to_string(), vec![]),
///         ("transform".to_string(), vec!["extract".to_string()]),
///     ]),
/// };
/// assert_eq!(dag.tasks.len(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct ResolvedDag {
    /// Sanitized, project-prefixed DAG id (e.g. `myproj_orders_pipeline`).
    pub name: String,
    /// Directory containing the `dag.yaml` marker file.
    pub dir: PathBuf,
    /// Fully merged DAG-level Airflow config (project → account → region →
    /// dag.yaml → optional single per-task DAG-level override).
    pub config: AirflowSection,
    /// Task ids in topological order. Tie-broken alphabetically for
    /// deterministic output across runs.
    pub tasks: Vec<String>,
    /// Per-task upstream dependencies, sorted. Keys cover every task in
    /// `tasks` (absent entries would be ambiguous).
    pub depends_on: BTreeMap<String, Vec<String>>,
}

/// Extract the persisted Glue script URI for each job from per-job state.
/// Filters `deployment.resources` for `type == "s3_object"` and returns
/// `job_name -> s3_uri`. Jobs without an `s3_object` resource are absent
/// from the output map (caller decides if that's an error per task).
///
/// This is the projection DAG rendering needs to thread per-task script
/// URIs into `generate_dag` (DAG-02); call sites pre-compute it via
/// `dag_lifecycle::load_script_locations_from_storage` (or the public
/// `load_script_locations` backend wrapper for CLI entry points).
#[must_use]
pub(crate) fn script_locations_from_state(
    states: &HashMap<String, JobState>,
) -> HashMap<String, String> {
    states
        .iter()
        .filter_map(|(job_name, state)| {
            state
                .deployment
                .resources
                .iter()
                .find(|r| r.r#type == "s3_object")
                .map(|r| (job_name.clone(), r.id.clone()))
        })
        .collect()
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::validation::validate_python_syntax;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use yard_structs::{
        AirflowJobBlock, AirflowMajorVersion, AwsCredentialConfig, Deployment, DeploymentStatus,
        JobName, JobType, ProjectManifest, Resource, StateBackend,
    };

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Minimal scoped temp dir that cleans up on drop. Avoids pulling in the
    /// `tempfile` crate as a dev-dependency.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("yard_airflow_dag_{}_{}", std::process::id(), n));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            TempDir { path }
        }
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    // ---- Fixtures ----

    fn empty_manifest(name: &str) -> ProjectManifest {
        ProjectManifest {
            project: name.to_string(),
            state: StateBackend::Local {
                path: PathBuf::from(".yard/state"),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: None,
        }
    }

    fn bash_job(command: &str, dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Bash,
            config: json!({"type": "bash", "command": command}),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn glue_job(dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Glue,
            config: json!({
                "type": "glue",
                "role": "arn:aws:iam::123456789:role/TestGlueRole",
            }),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn write_yaml(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    /// Build the minimal on-disk layout required by `resolve::load_context`
    /// (account.yaml + region.yaml must exist somewhere in the ancestry).
    fn setup_project_tree() -> TempDir {
        let tmp = TempDir::new();
        let root = tmp.path();
        write_yaml(&root.join("yard.yaml"), "project: test\n");
        write_yaml(&root.join("account.yaml"), "account:\n  id: \"123\"\n");
        write_yaml(&root.join("region.yaml"), "region:\n  id: us-east-1\n");
        tmp
    }

    // ---- collect_dags: empty / single / multi ----

    #[test]
    fn empty_dag_dir_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let manifest = empty_manifest("test");
        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("no task files"), "got: {err}");
    }

    #[test]
    fn single_bash_task_dag_collects_and_renders() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".to_string(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags.len(), 1);
        assert_eq!(dags[0].name, "test_pipeline");
        assert_eq!(dags[0].tasks, vec!["runit".to_string()]);
        assert_eq!(dags[0].config.schedule.as_deref(), Some("@daily"));

        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("BashOperator"));
        assert!(script.contains("dag_id=\"test_pipeline\""));
        assert!(script.contains("bash_command=\"echo hi\""));
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has a syntax error:\n{script}"
        );
    }

    #[test]
    fn linear_deps_topo_order() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        let mut c = bash_job("echo c", &dag_dir);
        a.airflow = None;
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        c.airflow = Some(AirflowJobBlock {
            depends_on: vec!["b".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);
        manifest.jobs.insert("c".to_string(), c);

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].tasks, vec!["a", "b", "c"]);

        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("t_a >> t_b"));
        assert!(script.contains("t_b >> t_c"));
        assert!(validate_python_syntax(&script).is_none());
    }

    #[test]
    fn fan_out_topo_order_is_deterministic() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@hourly\"\n");

        let mut manifest = empty_manifest("test");
        let a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        let mut c = bash_job("echo c", &dag_dir);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        c.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);
        manifest.jobs.insert("c".to_string(), c);

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].tasks, vec!["a", "b", "c"]);
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("t_a >> t_b"));
        assert!(script.contains("t_a >> t_c"));
        assert!(validate_python_syntax(&script).is_none());
    }

    #[test]
    fn cycle_detection_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["b".to_string()],
            ..Default::default()
        });
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn missing_depends_on_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["ghost".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("a".to_string(), a);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("ghost"), "got: {err}");
    }

    #[test]
    fn cross_dag_depends_on_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_one = root.join("one");
        let dag_two = root.join("two");
        write_yaml(&dag_one.join("dag.yaml"), "schedule: \"@daily\"\n");
        write_yaml(&dag_two.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("a".to_string(), bash_job("echo a", &dag_one));
        let mut b = bash_job("echo b", &dag_two);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("b".to_string(), b);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("cross-DAG"), "got: {err}");
    }

    #[test]
    fn nested_dag_yaml_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let outer = root.join("outer");
        let inner = outer.join("inner");
        write_yaml(&outer.join("dag.yaml"), "schedule: \"@daily\"\n");
        write_yaml(&inner.join("dag.yaml"), "schedule: \"@daily\"\n");

        let manifest = empty_manifest("test");
        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("nested dag.yaml"), "got: {err}");
    }

    #[test]
    fn inheritance_region_overrides_project_default() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        // Overwrite region.yaml with airflow key for override.
        write_yaml(
            &root.join("region.yaml"),
            "region:\n  id: us-east-1\nairflow:\n  schedule: \"@hourly\"\n",
        );
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "owner: data-team\n");

        let mut manifest = empty_manifest("test");
        manifest.providers.insert(
            "airflow".to_string(),
            json!({"schedule": "@daily", "retries": 1}),
        );
        manifest
            .jobs
            .insert("runit".to_string(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].config.schedule.as_deref(), Some("@hourly"));
        assert_eq!(dags[0].config.owner.as_deref(), Some("data-team"));
        assert_eq!(dags[0].config.retries, Some(1));
    }

    #[test]
    fn two_tasks_with_dag_level_overrides_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec![],
            publishes: vec![],
            overrides: AirflowSection {
                schedule: Some("@hourly".to_string()),
                ..Default::default()
            },
        });
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec![],
            publishes: vec![],
            overrides: AirflowSection {
                retries: Some(3),
                ..Default::default()
            },
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("DAG-level overrides"), "got: {err}");
    }

    #[test]
    fn mixed_glue_and_bash_renders() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("orders".to_string(), glue_job(&dag_dir));
        let mut notify = bash_job("echo done", &dag_dir);
        notify.airflow = Some(AirflowJobBlock {
            depends_on: vec!["orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("notify".to_string(), notify);

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("GlueJobOperator"));
        assert!(script.contains("BashOperator"));
        assert!(script.contains("t_orders >> t_notify"));
        assert!(validate_python_syntax(&script).is_none());
    }

    #[test]
    fn dag_name_sanitization_handles_dashes_and_leading_digit() {
        // Directly exercise the helper — we'd need a filesystem dir name
        // containing these characters otherwise.
        assert_eq!(sanitize_identifier("pipe-line"), "pipe_line");
        assert_eq!(sanitize_identifier("9am_etl"), "_9am_etl");
        assert_eq!(sanitize_identifier("order.flow"), "order_flow");
        assert_eq!(sanitize_identifier(""), "_");
    }

    #[test]
    fn self_dependency_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("a".to_string(), a);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("depends on itself"), "got: {err}");
    }

    // ---- Short-name resolution in depends_on ----

    fn prefixed_bash_job(base_name: &str, command: &str, dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Bash,
            config: json!({"type": "bash", "command": command}),
            dir: dir.to_path_buf(),
            base_name: base_name.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn depends_on_resolves_short_name_to_full() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = prefixed_bash_job("orders", "echo a", &dag_dir);
        a.airflow = None;
        let mut b = prefixed_bash_job("shipments", "echo b", &dag_dir);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("sales-orders".to_string(), a);
        manifest.jobs.insert("sales-shipments".to_string(), b);

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].tasks, vec!["sales-orders", "sales-shipments"]);
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("t_sales_orders >> t_sales_shipments"));
    }

    #[test]
    fn depends_on_full_name_still_works() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let a = prefixed_bash_job("orders", "echo a", &dag_dir);
        let mut b = prefixed_bash_job("shipments", "echo b", &dag_dir);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["sales-orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("sales-orders".to_string(), a);
        manifest.jobs.insert("sales-shipments".to_string(), b);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("t_sales_orders >> t_sales_shipments"));
    }

    #[test]
    fn depends_on_ambiguous_short_name_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let a = prefixed_bash_job("orders", "echo a", &dag_dir);
        let b = prefixed_bash_job("orders", "echo b", &dag_dir);
        let mut c = prefixed_bash_job("notify", "echo c", &dag_dir);
        c.airflow = Some(AirflowJobBlock {
            depends_on: vec!["orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("sales-orders".to_string(), a);
        manifest.jobs.insert("billing-orders".to_string(), b);
        manifest.jobs.insert("pipeline-notify".to_string(), c);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("ambiguous"), "got: {err}");
    }

    #[test]
    fn depends_on_self_via_short_name_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = prefixed_bash_job("orders", "echo a", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("sales-orders".to_string(), a);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("depends on itself"), "got: {err}");
    }

    #[test]
    fn bash_command_with_special_chars_escapes_correctly() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert(
            "runit".to_string(),
            bash_job("echo \"hello\\nworld\"", &dag_dir),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    // ---- orphan airflow block validation ----

    #[test]
    fn orphan_airflow_block_detected() {
        let tmp = setup_project_tree();
        let root = tmp.path();

        // dag_dir with a dag.yaml
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        // task_a is inside the DAG dir — fine
        let mut task_a = bash_job("echo a", &dag_dir);
        task_a.airflow = Some(AirflowJobBlock::default());

        // orphan_job has an airflow block but lives outside any DAG dir
        let orphan_dir = root.join("standalone");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        let mut orphan = bash_job("echo orphan", &orphan_dir);
        orphan.airflow = Some(AirflowJobBlock::default());

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("task_a".to_string(), task_a);
        manifest.jobs.insert("orphan_job".to_string(), orphan);

        let dags = collect_dags(root, &manifest).unwrap();
        let errors = validate_orphan_airflow_blocks(&manifest, &dags);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "orphan_job");
    }

    // ---- Cross-account: derive_aws_conn_id ----

    #[test]
    fn derive_aws_conn_id_happy_path() {
        let got = derive_aws_conn_id("arn:aws:iam::222222222222:role/GlueInvoker").unwrap();
        assert_eq!(got, "yard_222222222222_GlueInvoker");
    }

    #[test]
    fn derive_aws_conn_id_sanitizes_role_path() {
        // IAM role paths (slashes) are allowed; we sanitize them for
        // Airflow-friendly conn ids.
        let got = derive_aws_conn_id("arn:aws:iam::111111111111:role/path/to/MyRole").unwrap();
        assert_eq!(got, "yard_111111111111_path_to_MyRole");
    }

    #[test]
    fn derive_aws_conn_id_rejects_non_iam() {
        assert!(derive_aws_conn_id("arn:aws:s3:::my-bucket").is_err());
    }

    #[test]
    fn derive_aws_conn_id_rejects_bad_account() {
        // Short account, non-digit account.
        assert!(derive_aws_conn_id("arn:aws:iam::12345:role/R").is_err());
        assert!(derive_aws_conn_id("arn:aws:iam::abcdefghijkl:role/R").is_err());
    }

    #[test]
    fn derive_aws_conn_id_rejects_missing_role_prefix() {
        assert!(derive_aws_conn_id("arn:aws:iam::222222222222:user/Alice").is_err());
    }

    #[test]
    fn derive_aws_conn_id_rejects_empty_role_name() {
        assert!(derive_aws_conn_id("arn:aws:iam::222222222222:role/").is_err());
    }

    #[test]
    fn derive_aws_conn_id_rejects_garbage() {
        assert!(derive_aws_conn_id("not-an-arn").is_err());
        assert!(derive_aws_conn_id("").is_err());
    }

    // ---- Cross-account: parse_account_from_role_arn (shared helper, D-03) ----

    #[test]
    fn parse_account_from_role_arn_happy_path() {
        let got = connections::parse_account_from_role_arn(
            "arn:aws:iam::222222222222:role/GlueInvoker",
        )
        .unwrap();
        assert_eq!(got, "222222222222");
    }

    #[test]
    fn parse_account_from_role_arn_rejects_non_iam() {
        assert!(
            connections::parse_account_from_role_arn("arn:aws:s3:::my-bucket").is_err()
        );
    }

    #[test]
    fn parse_account_from_role_arn_rejects_bad_account() {
        assert!(
            connections::parse_account_from_role_arn("arn:aws:iam::12345:role/R").is_err()
        );
        assert!(
            connections::parse_account_from_role_arn("arn:aws:iam::abcdefghijkl:role/R")
                .is_err()
        );
    }

    #[test]
    fn parse_account_from_role_arn_rejects_missing_role_prefix() {
        assert!(
            connections::parse_account_from_role_arn("arn:aws:iam::222222222222:user/Alice")
                .is_err()
        );
    }

    #[test]
    fn parse_account_from_role_arn_rejects_empty_role_name() {
        assert!(
            connections::parse_account_from_role_arn("arn:aws:iam::222222222222:role/")
                .is_err()
        );
    }

    #[test]
    fn parse_account_from_role_arn_rejects_garbage() {
        assert!(connections::parse_account_from_role_arn("not-an-arn").is_err());
        assert!(connections::parse_account_from_role_arn("").is_err());
    }

    // ---- Cross-account: render_task picks aws_conn_id per job ----

    fn glue_job_with_assume_role(dir: &Path, role_arn: &str) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Glue,
            config: json!({
                "type": "glue",
                "role": "arn:aws:iam::123456789:role/TestGlueRole",
                "_aws": { "assume_role": role_arn },
            }),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    #[test]
    fn render_task_glue_no_assume_role_uses_default_conn() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("orders".into(), glue_job(&dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("aws_conn_id=\"aws_default\""));
        assert!(!script.contains("Required Airflow connections"));
    }

    #[test]
    fn render_task_glue_cross_account_uses_derived_conn() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig { assume_role: Some("arn:aws:iam::111111111111:role/OperatorA".to_string()), ..Default::default() });
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_assume_role(&dag_dir, "arn:aws:iam::222222222222:role/GlueInvoker"),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("aws_conn_id=\"yard_222222222222_GlueInvoker\""));
        assert!(script.contains("Required Airflow connections"));
        assert!(script.contains(
            "yard_222222222222_GlueInvoker  ->  arn:aws:iam::222222222222:role/GlueInvoker"
        ));
    }

    fn glue_job_with_aws_overrides(dir: &Path, overrides: Value) -> JobDefinition {
        JobDefinition {
            job_type: JobType::Glue,
            config: json!({
                "type": "glue",
                "role": "arn:aws:iam::123456789:role/TestGlueRole",
                "_aws": overrides,
            }),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    #[test]
    fn render_task_glue_explicit_aws_conn_id_short_circuits_derive() {
        // Cascaded `_aws.aws_conn_id` is the highest-precedence override on the
        // Glue task path: even when assume_role would otherwise produce a
        // derived connection id, the explicit value wins verbatim.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::111111111111:role/OperatorA".to_string()),
            ..Default::default()
        });
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_aws_overrides(
                &dag_dir,
                json!({
                    "assume_role": "arn:aws:iam::222222222222:role/GlueInvoker",
                    "aws_conn_id": "my_explicit_conn",
                }),
            ),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();

        assert!(
            script.contains("aws_conn_id=\"my_explicit_conn\""),
            "explicit cascaded aws_conn_id must win over derived value: {script}"
        );
        assert!(
            !script.contains("aws_conn_id=\"yard_222222222222_GlueInvoker\""),
            "derived value must not appear when explicit override is set: {script}"
        );
    }

    #[test]
    fn render_task_glue_explicit_aws_conn_id_without_assume_role() {
        // Same-account case + explicit aws_conn_id: explicit wins over the
        // aws_default fallback that resolve_task_aws_conn_id would otherwise
        // emit for a job with no assume_role.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_aws_overrides(&dag_dir, json!({"aws_conn_id": "team_glue_conn"})),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();

        assert!(
            script.contains("aws_conn_id=\"team_glue_conn\""),
            "explicit aws_conn_id must win over aws_default: {script}"
        );
        assert!(
            !script.contains("aws_conn_id=\"aws_default\""),
            "aws_default must not appear when explicit override is set: {script}"
        );
    }

    #[test]
    fn render_dag_trigger_default_aws_conn_id_from_root_aws_field() {
        // DAG-trigger path: explicit manifest.aws.aws_conn_id wins over the
        // assume_role-derived default at generation.rs's default_aws_conn_id
        // resolver. Verifies precedence step 2 of the new ladder.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  s3:\n    bucket: b\n    prefix: \"p/\"\n",
        );

        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::123456789012:role/MyRole".to_string()),
            aws_conn_id: Some("dag_root_explicit_conn".to_string()),
            ..Default::default()
        });
        manifest
            .jobs
            .insert("ingest".into(), bash_job("echo ingest", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("aws_conn_id=\"dag_root_explicit_conn\""),
            "S3 sensor must use explicit manifest.aws.aws_conn_id: {script}"
        );
        assert!(
            !script.contains("aws_conn_id=\"yard_123456789012_MyRole\""),
            "derived value must not appear when explicit override is set: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    // ---- Phase 15 DAG-03 regression: both new kwargs render correctly ----

    #[test]
    fn render_task_glue_same_account_emits_role_and_script() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("orders".to_string(), glue_job(&dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();

        // DAG-01: iam_role_arn from config.role (full ARN, verbatim per D-13)
        assert!(
            script.contains("iam_role_arn=\"arn:aws:iam::123456789:role/TestGlueRole\""),
            "expected iam_role_arn kwarg from config.role, got:\n{script}"
        );
        // DAG-02: script_location from the script_locations map
        assert!(
            script.contains("script_location=\"s3://bucket/scripts/orders.py\""),
            "expected script_location kwarg from script_locations map, got:\n{script}"
        );
    }

    #[test]
    fn render_task_glue_cross_account_emits_role_and_script() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig { assume_role: Some("arn:aws:iam::111111111111:role/OperatorA".to_string()), ..Default::default() });
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_assume_role(&dag_dir, "arn:aws:iam::222222222222:role/GlueInvoker"),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        // D-11: distinct cross-account bucket URI — proves render is indifferent
        // to which account uploaded the script.
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://cross-acct-bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();

        // DAG-01: per-job execution role from config.role — NOT the assume_role ARN.
        // The assume_role drives aws_conn_id, not iam_role_arn.
        assert!(
            script.contains("iam_role_arn=\"arn:aws:iam::123456789:role/TestGlueRole\""),
            "expected iam_role_arn kwarg from per-job config.role, got:\n{script}"
        );
        // DAG-02 + D-11: distinct cross-account bucket URI
        assert!(
            script.contains("script_location=\"s3://cross-acct-bucket/scripts/orders.py\""),
            "expected cross-account script_location kwarg, got:\n{script}"
        );
        // D-12: aws_conn_id still emitted under the new kwarg order — proves the
        // reorder did not drop the cross-account connection contract.
        assert!(
            script.contains("aws_conn_id=\"yard_222222222222_GlueInvoker\""),
            "expected cross-account aws_conn_id, got:\n{script}"
        );
    }

    // ---- Phase 15 DAG-04 regression: D-06/D-07 error-path wording ----

    #[test]
    fn render_task_glue_missing_script_uri_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("orders".to_string(), glue_job(&dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        // Empty script_locations: role check passes (fixture sets role), then
        // script URI lookup fails per D-05 ordering -> D-07 error surfaces.
        let script_locations: HashMap<String, String> = HashMap::new();
        let err = generate_dag(&manifest, &dags[0], &script_locations).unwrap_err();
        // `{err:#}` renders the full anyhow chain with outer `with_context` joined.
        let chain = format!("{err:#}");
        assert!(
            chain.contains("task 'orders'"),
            "expected 'task \\'orders\\'' prefix from with_context, got: {chain}"
        );
        assert!(
            chain.contains("persisted script URI"),
            "expected D-07 'persisted script URI' wording, got: {chain}"
        );
    }

    #[test]
    fn render_task_glue_missing_role_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        // Inline fixture: Glue task whose config does NOT contain `role`.
        // Do not modify the shared `glue_job` fixture — other tests depend on
        // it having a role.
        manifest.jobs.insert(
            "orders".to_string(),
            JobDefinition {
                job_type: JobType::Glue,
                config: json!({
                    "type": "glue",
                    // role intentionally omitted
                }),
                dir: dag_dir.to_path_buf(),
                ..Default::default()
            },
        );

        let dags = collect_dags(root, &manifest).unwrap();
        // Populated script_locations so the role check (D-05 first) is the
        // failing predicate — proving ordering contract.
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let err = generate_dag(&manifest, &dags[0], &script_locations).unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("task 'orders'"),
            "expected 'task \\'orders\\'' prefix from with_context, got: {chain}"
        );
        assert!(
            chain.contains("'config.role'"),
            "expected D-06 'config.role' wording, got: {chain}"
        );
    }

    #[test]
    fn render_task_glue_same_account_role_uses_default_conn() {
        // Job declares an assume_role that matches the project root — no
        // cross-account boundary, so no derived conn_id and no docstring.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let root_arn = "arn:aws:iam::111111111111:role/OperatorA";
        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig { assume_role: Some(root_arn.to_string()), ..Default::default() });
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_assume_role(&dag_dir, root_arn),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("aws_conn_id=\"aws_default\""));
        assert!(!script.contains("Required Airflow connections"));
    }

    #[test]
    fn required_connections_deduplicates_across_tasks() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let role_b = "arn:aws:iam::222222222222:role/GlueInvoker";
        let role_c = "arn:aws:iam::333333333333:role/GlueInvoker";
        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig { assume_role: Some("arn:aws:iam::111111111111:role/OperatorA".to_string()), ..Default::default() });
        manifest
            .jobs
            .insert("orders".into(), glue_job_with_assume_role(&dag_dir, role_b));
        manifest.jobs.insert(
            "shipments".into(),
            glue_job_with_assume_role(&dag_dir, role_b),
        );
        manifest.jobs.insert(
            "billing".into(),
            glue_job_with_assume_role(&dag_dir, role_c),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let conns = required_connections_for_dag(&manifest, &dags[0]).unwrap();
        assert_eq!(conns.len(), 2);
        // Deterministic (BTreeMap ordering).
        assert_eq!(conns[0].conn_id, "yard_222222222222_GlueInvoker");
        assert_eq!(conns[0].role_arn, role_b);
        assert_eq!(conns[1].conn_id, "yard_333333333333_GlueInvoker");
        assert_eq!(conns[1].role_arn, role_c);
    }

    #[test]
    fn required_connections_ignores_bash_tasks() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("run".into(), bash_job("echo hi", &dag_dir));
        let dags = collect_dags(root, &manifest).unwrap();
        assert!(
            required_connections_for_dag(&manifest, &dags[0])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn generate_dag_fails_on_malformed_assume_role() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_assume_role(&dag_dir, "garbage"),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        // Supply a populated script_locations so the new D-05/D-06/D-07
        // checks pass (role is set in the fixture; URI now present), letting
        // the original "malformed role ARN" error from resolve_task_aws_conn_id
        // surface unchanged.
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let err = generate_dag(&manifest, &dags[0], &script_locations).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("malformed role ARN"), "error was: {msg}");
    }

    #[test]
    fn no_orphan_when_all_in_dag() {
        let tmp = setup_project_tree();
        let root = tmp.path();

        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut task_a = bash_job("echo a", &dag_dir);
        task_a.airflow = Some(AirflowJobBlock::default());

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("task_a".to_string(), task_a);

        let dags = collect_dags(root, &manifest).unwrap();
        let errors = validate_orphan_airflow_blocks(&manifest, &dags);
        assert!(errors.is_empty());
    }

    // ---- Airflow Datasets ----

    #[test]
    fn task_with_publishes_emits_outlets() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut job = glue_job(&dag_dir);
        job.airflow = Some(AirflowJobBlock {
            publishes: vec!["s3://warehouse/sales/orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("orders".into(), job);

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("from airflow.datasets import Dataset"));
        assert!(script.contains("outlets=[Dataset(\"s3://warehouse/sales/orders\")]"));
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn task_without_publishes_omits_outlets() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("orders".into(), glue_job(&dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> = [(
            "orders".to_string(),
            "s3://bucket/scripts/orders.py".to_string(),
        )]
        .into_iter()
        .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(!script.contains("outlets"));
        assert!(!script.contains("Dataset"));
    }

    #[test]
    fn dag_trigger_datasets_emits_schedule_list() {
        // Phase 30 plan 30-01 (DS-02) updated: homogeneous all-Datasets now
        // renders the Airflow 2.9 native `&` chain instead of Phase 28's
        // interim flat list. URIs alpha-sorted per D-11.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  all:\n    - dataset:\n        uri: s3://warehouse/sales/orders\n    - dataset:\n        uri: s3://warehouse/sales/shipments\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("from airflow.datasets import Dataset"));
        assert!(
            script.contains(
                "schedule=(Dataset(\"s3://warehouse/sales/orders\") & Dataset(\"s3://warehouse/sales/shipments\"))"
            ),
            "DS-02 expects native `&` chain, alpha-sorted: {script}"
        );
        assert!(!script.contains("@daily"));
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn dag_schedule_only_omits_max_active_runs_pres_02_invariant() {
        // PRES-02: schedule-only DAGs must render WITHOUT a max_active_runs=
        // line (Airflow's implicit default of 16 applies by absence). Plan
        // 30-04 wires CONC-01 auto-default-to-1 for trigger DAGs only.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(
            !script.contains("max_active_runs="),
            "schedule-only DAG must NOT render max_active_runs= line: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn dag_trigger_datasets_homogeneous_all_emits_amp_chain() {
        // DS-02: trigger: { all: [dataset, dataset] } -> schedule=(Dataset(a) & Dataset(b)).
        // URIs out-of-order in YAML; alpha-sort kicks in (D-11).
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  all:\n    - dataset:\n        uri: s3://warehouse/zzz\n    - dataset:\n        uri: s3://warehouse/aaa\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("from airflow.datasets import Dataset"));
        assert!(
            script.contains(
                "schedule=(Dataset(\"s3://warehouse/aaa\") & Dataset(\"s3://warehouse/zzz\"))"
            ),
            "expected alpha-sorted & chain: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn dag_trigger_datasets_homogeneous_any_emits_pipe_chain() {
        // DS-03: trigger: { any: [dataset, dataset] } -> schedule=(Dataset(a) | Dataset(b)).
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  any:\n    - dataset:\n        uri: s3://warehouse/zzz\n    - dataset:\n        uri: s3://warehouse/aaa\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(
            script.contains(
                "schedule=(Dataset(\"s3://warehouse/aaa\") | Dataset(\"s3://warehouse/zzz\"))"
            ),
            "expected alpha-sorted | chain: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn dag_trigger_dataset_single_element_all_renders_same_as_bare_single_dataset() {
        // D-12 end-to-end: { all: [dataset(x)] } and { dataset(x) } render
        // byte-identical Python because Trigger::Serialize collapses on the
        // hash side AND render_trigger normalizes on the codegen side.
        let tmp_a = setup_project_tree();
        let dir_a = tmp_a.path().join("pa");
        write_yaml(
            &dir_a.join("dag.yaml"),
            "trigger:\n  all:\n    - dataset:\n        uri: s3://warehouse/foo\n",
        );
        let mut manifest_a = empty_manifest("test");
        manifest_a
            .jobs
            .insert("agg".into(), bash_job("echo a", &dir_a));
        let dags_a = collect_dags(tmp_a.path(), &manifest_a).unwrap();
        let script_a = generate_dag(&manifest_a, &dags_a[0], &HashMap::new()).unwrap();

        let tmp_b = setup_project_tree();
        let dir_b = tmp_b.path().join("pa");
        write_yaml(
            &dir_b.join("dag.yaml"),
            "trigger:\n  dataset:\n    uri: s3://warehouse/foo\n",
        );
        let mut manifest_b = empty_manifest("test");
        manifest_b
            .jobs
            .insert("agg".into(), bash_job("echo a", &dir_b));
        let dags_b = collect_dags(tmp_b.path(), &manifest_b).unwrap();
        let script_b = generate_dag(&manifest_b, &dags_b[0], &HashMap::new()).unwrap();

        assert_eq!(
            script_a, script_b,
            "single-element all must render byte-identical to bare-single Dataset (D-12)"
        );
    }

    // --- Phase 30 plan 30-02: end-to-end S3 sensor render fixtures (S3-01..S3-04) ---

    #[test]
    fn dag_trigger_s3_emits_deferrable_sensor() {
        // S3-01 end-to-end: trigger: { s3: { bucket, prefix } } with one
        // bash task. Verify deterministic task_id, knob defaults, and the
        // _yard_wait_s3 >> t_<root> edge wire through generation.rs +
        // template render. Generated Python must parse cleanly.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  s3:\n    bucket: mybucket\n    prefix: \"input/\"\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("ingest".into(), bash_job("echo ingest", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor"),
            "expected S3KeySensor import line: {script}"
        );
        assert!(
            script.contains("_yard_wait_s3 = S3KeySensor("),
            "expected _yard_wait_s3 task assignment: {script}"
        );
        assert!(
            script.contains("task_id=\"_yard_wait_s3\""),
            "expected deterministic task_id: {script}"
        );
        assert!(
            script.contains("bucket_name=\"mybucket\""),
            "expected bucket_name kwarg: {script}"
        );
        assert!(
            script.contains("bucket_key=\"input/\""),
            "expected bucket_key from prefix: {script}"
        );
        assert!(script.contains("poke_interval=60"), "default knob: {script}");
        assert!(script.contains("timeout=86400"), "default knob: {script}");
        assert!(
            script.contains("deferrable=True"),
            "S3-03 default: {script}"
        );
        assert!(
            script.contains("_yard_wait_s3 >> t_ingest"),
            "expected sensor edge to root task: {script}"
        );
        assert!(
            script.contains("schedule=None"),
            "S3 sensor-driven DAG must render schedule=None: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_s3_with_user_knob_overrides_renders_overrides() {
        // S3-02 + S3-03 end-to-end: poke_interval, timeout, deferrable=false
        // overrides propagate through to the rendered Python verbatim.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  s3:\n    bucket: b\n    prefix: \"p/\"\n    poke_interval: 120\n    timeout: 3600\n    deferrable: false\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("ingest".into(), bash_job("echo ingest", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("poke_interval=120"),
            "user override propagates: {script}"
        );
        assert!(
            script.contains("timeout=3600"),
            "user override propagates: {script}"
        );
        assert!(
            script.contains("deferrable=False"),
            "S3-03 legacy escape hatch: {script}"
        );
        assert!(
            !script.contains("deferrable=True"),
            "must not also emit deferrable=True: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_s3_inherits_aws_conn_id_from_assume_role() {
        // S3-04 end-to-end: when manifest.aws.assume_role is set, the S3
        // sensor task inherits the derived aws_conn_id via the same
        // derive_aws_conn_id plumbing that powers Glue tasks. No per-trigger
        // override means the DAG-level default wins.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  s3:\n    bucket: b\n    prefix: \"p/\"\n",
        );

        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::123456789012:role/MyRole".to_string()),
            ..Default::default()
        });
        manifest
            .jobs
            .insert("ingest".into(), bash_job("echo ingest", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("aws_conn_id=\"yard_123456789012_MyRole\""),
            "S3 sensor must inherit DAG-level aws_conn_id from assume_role: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    // --- Phase 30 plan 30-03: end-to-end SQS sensor render fixtures (SQS-01, SQS-02) ---

    #[test]
    fn dag_trigger_sqs_emits_long_poll_sensor() {
        // SQS-01 + SQS-02 end-to-end: trigger: { sqs: { queue_url } } with one
        // bash task. Verify deterministic task_id, knob defaults (long-poll
        // wait_time_seconds=20 saves SQS API costs), and the
        // _yard_wait_sqs >> t_<root> edge wire through generation.rs +
        // template render. Generated Python must parse cleanly.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  sqs:\n    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("from airflow.providers.amazon.aws.sensors.sqs import SqsSensor"),
            "expected SqsSensor import line: {script}"
        );
        assert!(
            script.contains("_yard_wait_sqs = SqsSensor("),
            "expected _yard_wait_sqs task assignment: {script}"
        );
        assert!(
            script.contains("task_id=\"_yard_wait_sqs\""),
            "expected deterministic task_id: {script}"
        );
        assert!(
            script.contains(
                "sqs_queue=\"https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\""
            ),
            "expected sqs_queue kwarg: {script}"
        );
        assert!(
            script.contains("wait_time_seconds=20"),
            "long-poll default knob: {script}"
        );
        assert!(script.contains("max_messages=5"), "default knob: {script}");
        assert!(
            script.contains("delete_message_on_reception=True"),
            "default knob: {script}"
        );
        assert!(
            script.contains("deferrable=True"),
            "SQS deferrable default: {script}"
        );
        assert!(
            script.contains("_yard_wait_sqs >> t_worker"),
            "expected sensor edge to root task: {script}"
        );
        assert!(
            script.contains("schedule=None"),
            "SQS sensor-driven DAG must render schedule=None: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_sqs_with_user_knob_overrides_renders_overrides() {
        // SQS-02 end-to-end: wait_time_seconds, max_messages,
        // delete_message_on_reception overrides propagate through to the
        // rendered Python verbatim. delete=false renders Python's `False`.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  sqs:\n    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\n    wait_time_seconds: 10\n    max_messages: 1\n    delete_message_on_reception: false\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("wait_time_seconds=10"),
            "user override propagates: {script}"
        );
        assert!(
            script.contains("max_messages=1"),
            "user override propagates: {script}"
        );
        assert!(
            script.contains("delete_message_on_reception=False"),
            "user override propagates as Python False: {script}"
        );
        assert!(
            !script.contains("delete_message_on_reception=True"),
            "must not also emit True: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_sqs_inherits_aws_conn_id_from_assume_role() {
        // SQS aws_conn_id end-to-end: SqsTrigger has no per-trigger
        // override field, so the DAG-level default from
        // manifest.aws.assume_role flows in via derive_aws_conn_id —
        // same plumbing that powers Glue tasks and the S3 sensor.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  sqs:\n    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\n",
        );

        let mut manifest = empty_manifest("test");
        manifest.aws = Some(AwsCredentialConfig {
            assume_role: Some("arn:aws:iam::123456789012:role/MyRole".to_string()),
            ..Default::default()
        });
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("aws_conn_id=\"yard_123456789012_MyRole\""),
            "SQS sensor must inherit DAG-level aws_conn_id from assume_role: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    // ---- Phase 30 plan 30-04: API trigger end-to-end ----

    #[test]
    fn dag_trigger_api_emits_schedule_none_with_header_docstring() {
        // API-01..API-03 end-to-end: trigger: { api: { description } }
        // emits schedule=None plus a header docstring with curl/CLI snippets,
        // $AIRFLOW_URL placeholder, and the description verbatim. CONC-01:
        // any trigger DAG renders max_active_runs=1 by default.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  api:\n    description: Manual replay\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("schedule=None"),
            "API trigger renders schedule=None: {script}"
        );
        assert!(
            script.contains("# Manual replay"),
            "description must appear in header: {script}"
        );
        assert!(
            script.contains("$AIRFLOW_URL"),
            "header must use $AIRFLOW_URL placeholder: {script}"
        );
        assert!(
            script.contains("curl -X POST"),
            "header must include curl snippet: {script}"
        );
        assert!(
            script.contains("airflow dags trigger"),
            "header must include CLI snippet: {script}"
        );
        assert!(
            script.contains("max_active_runs=1"),
            "CONC-01 default for trigger DAG: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_heterogeneous_all_emits_join_operator() {
        // DS-04 + D-09 end-to-end: trigger: { all: [s3, sqs] } emits both
        // sensors, _yard_join EmptyOperator with trigger_rule="all_success",
        // sensor->join->root edges, EmptyOperator import, and CONC-01
        // max_active_runs=1.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  all:\n    - s3:\n        bucket: mybucket\n        prefix: input/\n    - sqs:\n        queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("_yard_wait_s3 = S3KeySensor("),
            "S3 sensor task: {script}"
        );
        assert!(
            script.contains("_yard_wait_sqs = SqsSensor("),
            "SQS sensor task: {script}"
        );
        assert!(
            script.contains("_yard_join = EmptyOperator("),
            "_yard_join task: {script}"
        );
        assert!(
            script.contains("task_id=\"_yard_join\""),
            "_yard_join task_id literal: {script}"
        );
        assert!(
            script.contains("trigger_rule=\"all_success\""),
            "_yard_join trigger_rule: {script}"
        );
        assert!(
            script.contains("_yard_wait_s3 >> _yard_join"),
            "S3 -> _yard_join edge: {script}"
        );
        assert!(
            script.contains("_yard_wait_sqs >> _yard_join"),
            "SQS -> _yard_join edge: {script}"
        );
        assert!(
            script.contains("_yard_join >> t_worker"),
            "_yard_join -> root edge: {script}"
        );
        assert!(
            script.contains("from airflow.operators.empty import EmptyOperator"),
            "EmptyOperator import: {script}"
        );
        assert!(
            script.contains("max_active_runs=1"),
            "CONC-01 default: {script}"
        );
        assert!(
            script.contains("schedule=None"),
            "no Datasets => schedule=None: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_max_active_runs_user_override_wins() {
        // CONC-01 user-override-wins end-to-end: airflow.max_active_runs: 4 set
        // alongside trigger: { s3: ... }. User value 4 wins over CONC-01
        // auto-default of 1.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "max_active_runs: 4\ntrigger:\n  s3:\n    bucket: mybucket\n    prefix: input/\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("max_active_runs=4"),
            "user override (4) must beat CONC-01 auto-default (1): {script}"
        );
        assert!(
            !script.contains("max_active_runs=1"),
            "CONC-01 auto-default must not also leak when user overrides: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_schedule_only_with_explicit_max_active_runs_renders_user_value() {
        // PRES-02 + CONC-01 user-override: schedule-only DAG with explicit
        // airflow.max_active_runs: 8 — render the user value. Without the
        // explicit field, schedule-only DAGs render no max_active_runs= line
        // at all (locked by `dag_schedule_only_omits_max_active_runs_pres_02_invariant`
        // from plan 30-01).
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\nmax_active_runs: 8\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("max_active_runs=8"),
            "explicit user value renders: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_api_with_payload_schema_documents_fields() {
        // API-02 doc-only end-to-end: payload_schema fields appear in the
        // header docstring. No runtime enforcement — just docs for the user
        // assembling the curl/CLI invocation.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  api:\n    payload_schema:\n      customer_id: string\n      event_id: string\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("customer_id"),
            "payload_schema field customer_id must appear in header: {script}"
        );
        assert!(
            script.contains("event_id"),
            "payload_schema field event_id must appear in header: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn trigger_overrides_inherited_schedule() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\ntrigger:\n  dataset:\n    uri: s3://warehouse/foo\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("task".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("schedule=[Dataset(\"s3://warehouse/foo\")]"));
        assert!(!script.contains("@daily"));
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn multiple_publishes_on_one_task() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut job = bash_job("echo done", &dag_dir);
        job.airflow = Some(AirflowJobBlock {
            publishes: vec![
                "s3://warehouse/a".to_string(),
                "s3://warehouse/b".to_string(),
            ],
            ..Default::default()
        });
        manifest.jobs.insert("multi".into(), job);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(
            script
                .contains("outlets=[Dataset(\"s3://warehouse/a\"), Dataset(\"s3://warehouse/b\")]")
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    // ---- D-09: script_locations_from_state helper ----

    #[test]
    fn script_locations_from_state_extracts_s3_object_ids() {
        let mut states: HashMap<String, JobState> = HashMap::new();
        for name in ["alpha", "beta"] {
            states.insert(
                name.to_string(),
                JobState {
                    job_name: JobName::new(name),
                    project: "test".to_string(),
                    deployment: Deployment {
                        env: None,
                        config_hash: String::new(),
                        config: serde_json::Value::Null,
                        status: DeploymentStatus::Deployed,
                        applied_at: String::new(),
                        resources: vec![Resource {
                            r#type: "s3_object".to_string(),
                            id: format!("s3://bucket/{name}.py"),
                            provider: "glue".to_string(),
                        }],
                    },
                },
            );
        }
        let out = script_locations_from_state(&states);
        assert_eq!(out.len(), 2);
        assert_eq!(
            out.get("alpha").map(String::as_str),
            Some("s3://bucket/alpha.py")
        );
        assert_eq!(
            out.get("beta").map(String::as_str),
            Some("s3://bucket/beta.py")
        );
    }

    #[test]
    fn script_locations_from_state_skips_jobs_without_s3_object() {
        let mut states: HashMap<String, JobState> = HashMap::new();
        // Job A: has s3_object
        states.insert(
            "a".to_string(),
            JobState {
                job_name: JobName::new("a"),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: DeploymentStatus::Deployed,
                    applied_at: String::new(),
                    resources: vec![Resource {
                        r#type: "s3_object".to_string(),
                        id: "s3://bucket/a.py".to_string(),
                        provider: "glue".to_string(),
                    }],
                },
            },
        );
        // Job B: only glue_job, no s3_object
        states.insert(
            "b".to_string(),
            JobState {
                job_name: JobName::new("b"),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: DeploymentStatus::Deployed,
                    applied_at: String::new(),
                    resources: vec![Resource {
                        r#type: "glue_job".to_string(),
                        id: "b_name".to_string(),
                        provider: "glue".to_string(),
                    }],
                },
            },
        );
        // Job C: empty resources
        states.insert(
            "c".to_string(),
            JobState {
                job_name: JobName::new("c"),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: DeploymentStatus::Deployed,
                    applied_at: String::new(),
                    resources: vec![],
                },
            },
        );
        let out = script_locations_from_state(&states);
        assert_eq!(out.len(), 1);
        assert_eq!(out.get("a").map(String::as_str), Some("s3://bucket/a.py"));
        assert!(!out.contains_key("b"));
        assert!(!out.contains_key("c"));
    }

    #[test]
    fn script_locations_from_state_picks_s3_object_not_glue_job() {
        let mut states: HashMap<String, JobState> = HashMap::new();
        states.insert(
            "my_job".to_string(),
            JobState {
                job_name: JobName::new("my_job"),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: DeploymentStatus::Deployed,
                    applied_at: String::new(),
                    resources: vec![
                        // glue_job FIRST — filter is by type, not position
                        Resource {
                            r#type: "glue_job".to_string(),
                            id: "my_glue_job_name".to_string(),
                            provider: "glue".to_string(),
                        },
                        Resource {
                            r#type: "s3_object".to_string(),
                            id: "s3://bucket/script.py".to_string(),
                            provider: "glue".to_string(),
                        },
                    ],
                },
            },
        );
        let out = script_locations_from_state(&states);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out.get("my_job").map(String::as_str),
            Some("s3://bucket/script.py")
        );
        // Explicitly prove the helper did NOT pick the glue_job id:
        assert_ne!(
            out.get("my_job").map(String::as_str),
            Some("my_glue_job_name")
        );
    }

    #[test]
    fn script_locations_from_state_empty_input() {
        let states: HashMap<String, JobState> = HashMap::new();
        let out = script_locations_from_state(&states);
        assert!(out.is_empty());
    }

    // ---- Phase 32 (PUB-01): DAG-level `publishes:` synthesizes _yard_publish ----

    #[test]
    fn dag_with_publishes_emits_yard_publish_task() {
        // PUB-01: AirflowSection.publishes (DAG-level) renders a synthetic
        // _yard_publish EmptyOperator with alpha-sorted Dataset outlets, wired
        // downstream of every leaf task via `[leaf_a, leaf_b] >> _yard_publish`.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\npublishes:\n  - s3://warehouse/sales/orders\n  - s3://warehouse/sales/processed\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("orders".into(), bash_job("echo orders", &dag_dir));
        manifest
            .jobs
            .insert("shipments".into(), bash_job("echo shipments", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("_yard_publish = EmptyOperator("),
            "expected synthetic _yard_publish task body: {script}"
        );
        assert!(
            script.contains("        task_id=\"_yard_publish\","),
            "expected verbatim task_id line: {script}"
        );
        assert!(
            script.contains(
                "outlets=[Dataset(\"s3://warehouse/sales/orders\"), Dataset(\"s3://warehouse/sales/processed\")]"
            ),
            "expected alpha-sorted outlets list: {script}"
        );
        // Leaf order is task-iteration order — accept either ordering of t_orders/t_shipments.
        let order_a = script.contains("[t_orders, t_shipments] >> _yard_publish");
        let order_b = script.contains("[t_shipments, t_orders] >> _yard_publish");
        assert!(
            order_a || order_b,
            "expected fan-in deps line covering both leaves: {script}"
        );
        assert!(
            script.contains("from airflow.operators.empty import EmptyOperator"),
            "expected EmptyOperator import: {script}"
        );
        assert!(
            script.contains("from airflow.datasets import Dataset"),
            "expected Dataset import: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn yard_publish_outlets_alpha_sorted() {
        // D-04: outlets URIs are alpha-sorted independent of declaration order.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\npublishes:\n  - s3://b\n  - s3://a\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("only".into(), bash_job("echo only", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("outlets=[Dataset(\"s3://a\"), Dataset(\"s3://b\")]"),
            "expected alpha-sorted outlets regardless of declaration order: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn yard_publish_skipped_when_publishes_empty() {
        // D-05 / PRES-02: schedule-only DAG with no publishes renders byte-identical
        // — no _yard_publish, no EmptyOperator import.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            !script.contains("_yard_publish"),
            "schedule-only DAG must NOT contain _yard_publish: {script}"
        );
        assert!(
            !script.contains("from airflow.operators.empty import EmptyOperator"),
            "schedule-only DAG must NOT import EmptyOperator: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn yard_publish_single_leaf_emits_list_form() {
        // RESEARCH Pattern 1: single-leaf form is uniform list form
        // `[t_only] >> _yard_publish` (locked for grep-uniformity).
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\npublishes:\n  - s3://a\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("only".into(), bash_job("echo only", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("[t_only] >> _yard_publish"),
            "expected single-leaf list form `[t_only] >> _yard_publish`: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn yard_publish_with_chain_picks_terminal_leaf() {
        // Leaf detection: in a chain `t_a -> t_b`, only `t_b` is a leaf
        // (`t_a` appears in t_b's depends_on).
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\npublishes:\n  - s3://x\n",
        );

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("a".into(), bash_job("echo a", &dag_dir));
        let mut b = bash_job("echo b", &dag_dir);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("b".into(), b);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("[t_b] >> _yard_publish"),
            "expected only terminal leaf (`t_b`) wired to _yard_publish: {script}"
        );
        assert!(
            !script.contains("[t_a, t_b]") && !script.contains("[t_b, t_a]"),
            "non-leaf `t_a` must NOT appear in fan-in: {script}"
        );
        assert!(
            !script.contains("[t_a] >> _yard_publish"),
            "non-leaf `t_a` must NOT be the leaf: {script}"
        );
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    // --- Phase 32 plan 32-03: VERSION_BANNER + per-source backfill caveats (DOC-05) ---

    #[test]
    fn version_banner_renders_on_event_driven_dag_render() {
        // DOC-05 / D-11 / D-14b: an event-driven DAG (any trigger:* set) must
        // render the fixed Airflow-version-contract banner inside the
        // header docstring block. Banner flows through the existing
        // {{ trigger_header_block }} insertion in airflow_dag.py.tera —
        // NO template change.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "trigger:\n  dataset:\n    uri: s3://x\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        // All four banner content lines must be present verbatim.
        assert!(
            script.contains("# Airflow version contract:"),
            "banner header line missing: {script}"
        );
        assert!(
            script.contains("#   - apache-airflow >= 2.9"),
            "banner apache-airflow line missing: {script}"
        );
        assert!(
            script.contains("#   - apache-airflow-providers-amazon >= 8.13.0"),
            "banner providers-amazon line missing: {script}"
        );
        assert!(
            script.contains("#   - aiobotocore >= 2.1.1"),
            "banner aiobotocore line missing: {script}"
        );
        assert!(
            script.contains("#   - Triggerer process required"),
            "banner Triggerer line missing: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "rendered DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn version_banner_absent_on_schedule_only_dag_render() {
        // PRES-02 / D-12 byte-id guard: a schedule-only DAG must render WITHOUT
        // the banner (and without any per-source block). This is the regression
        // gate for the 20+ pre-Phase-32 schedule-only fixtures.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            !script.contains("# Airflow version contract:"),
            "schedule-only DAG must NOT render banner header (PRES-02): {script}"
        );
        assert!(
            !script.contains("apache-airflow >= 2.9"),
            "schedule-only DAG must NOT render any banner content (PRES-02): {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "rendered schedule-only DAG has syntax error:\n{script}"
        );
    }

    // --- Phase 56 plan 03: V3 integration tests (end-to-end codegen pipeline) ---
    // Each test writes version: "3" into dag.yaml and asserts V3 output
    // (Asset class, providers-standard imports, AF3 banner). Negative assertions
    // confirm no V2 strings leak (Pitfall 1 from RESEARCH.md).

    #[test]
    fn dag_trigger_single_dataset_v3_emits_asset() {
        // D-01/D-03: single dataset trigger with version: "3" emits Asset
        // class and from airflow.sdk import. No V2 strings may leak.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\ntrigger:\n  dataset:\n    uri: s3://warehouse/foo\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("from airflow.sdk import Asset"),
            "V3 must emit Asset import: {script}"
        );
        assert!(
            script.contains("schedule=[Asset(\"s3://warehouse/foo\")]"),
            "V3 single dataset must emit Asset class name: {script}"
        );
        // Negative: no V2 leak
        assert!(
            !script.contains("from airflow.datasets import Dataset"),
            "V3 must NOT contain V2 Dataset import: {script}"
        );
        assert!(
            !script.contains("Dataset("),
            "V3 must NOT contain Dataset( class usage: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 single dataset DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_homogeneous_all_datasets_v3_emits_asset_amp_chain() {
        // D-03: all:[dataset, dataset] with version "3" emits Asset & chain.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\ntrigger:\n  all:\n    - dataset:\n        uri: s3://warehouse/zzz\n    - dataset:\n        uri: s3://warehouse/aaa\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains(
                "schedule=(Asset(\"s3://warehouse/aaa\") & Asset(\"s3://warehouse/zzz\"))"
            ),
            "V3 homogeneous all must emit alpha-sorted Asset & chain: {script}"
        );
        assert!(
            script.contains("from airflow.sdk import Asset"),
            "V3 must emit Asset import: {script}"
        );
        assert!(
            !script.contains("Dataset"),
            "V3 must NOT contain Dataset: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 homogeneous all DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_homogeneous_any_datasets_v3_emits_asset_pipe_chain() {
        // D-03: any:[dataset, dataset] with version "3" emits Asset | chain.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\ntrigger:\n  any:\n    - dataset:\n        uri: s3://warehouse/zzz\n    - dataset:\n        uri: s3://warehouse/aaa\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains(
                "schedule=(Asset(\"s3://warehouse/aaa\") | Asset(\"s3://warehouse/zzz\"))"
            ),
            "V3 homogeneous any must emit alpha-sorted Asset | chain: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 homogeneous any DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_single_bash_task_v3_emits_providers_standard_import() {
        // OPIM-01: version "3" schedule-only DAG with one bash task emits
        // providers-standard BashOperator import. No V2 import leak.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\nschedule: \"@daily\"\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains(
                "from airflow.providers.standard.operators.bash import BashOperator"
            ),
            "V3 must emit providers-standard BashOperator import: {script}"
        );
        assert!(
            !script.contains("from airflow.operators.bash import BashOperator"),
            "V3 must NOT contain V2 BashOperator import: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 schedule-only bash DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_heterogeneous_all_v3_emits_providers_standard_empty_op() {
        // OPIM-02: version "3" + heterogeneous all:[s3, sqs] emits
        // providers-standard EmptyOperator import. No V2 import leak.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\ntrigger:\n  all:\n    - s3:\n        bucket: mybucket\n        prefix: input/\n    - sqs:\n        queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("worker".into(), bash_job("echo work", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains(
                "from airflow.providers.standard.operators.empty import EmptyOperator"
            ),
            "V3 must emit providers-standard EmptyOperator import: {script}"
        );
        assert!(
            !script.contains("from airflow.operators.empty import EmptyOperator"),
            "V3 must NOT contain V2 EmptyOperator import: {script}"
        );
        assert!(
            script.contains(
                "from airflow.providers.standard.operators.bash import BashOperator"
            ),
            "V3 bash task must also use providers-standard BashOperator: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 heterogeneous all DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_with_publishes_v3_emits_asset_outlets() {
        // PUB-02 + ASSET-02: version "3" + per-task publishes emits Asset
        // class name in outlets. No Dataset leak.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\nschedule: \"@daily\"\n",
        );

        let mut manifest = empty_manifest("test");
        let mut job = bash_job("echo done", &dag_dir);
        job.airflow = Some(AirflowJobBlock {
            publishes: vec!["s3://warehouse/orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("pub_task".into(), job);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("outlets=[Asset(\"s3://warehouse/orders\")]"),
            "V3 per-task publishes must emit Asset outlets: {script}"
        );
        assert!(
            !script.contains("Dataset("),
            "V3 must NOT contain Dataset( class usage: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 per-task publishes DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_with_dag_level_publishes_v3_emits_asset_in_yard_publish() {
        // PUB-01 + ASSET-02: version "3" + DAG-level publishes emits Asset
        // class in _yard_publish outlets + providers-standard EmptyOperator.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\nschedule: \"@daily\"\npublishes:\n  - s3://a\n  - s3://b\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("t1".into(), bash_job("echo t1", &dag_dir));
        manifest
            .jobs
            .insert("t2".into(), bash_job("echo t2", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            script.contains("outlets=[Asset(\"s3://a\"), Asset(\"s3://b\")]"),
            "V3 DAG-level publishes must emit Asset outlets (alpha-sorted): {script}"
        );
        assert!(
            script.contains(
                "from airflow.providers.standard.operators.empty import EmptyOperator"
            ),
            "V3 _yard_publish must use providers-standard EmptyOperator: {script}"
        );
        assert!(
            script.contains("from airflow.sdk import Asset"),
            "V3 publishes must import Asset: {script}"
        );
        assert!(
            !script.contains("from airflow.datasets import Dataset"),
            "V3 must NOT contain V2 Dataset import: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 DAG-level publishes DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_trigger_v3_event_driven_emits_v3_banner() {
        // BANNER-02: version "3" event-driven DAG emits the AF3 banner.
        // No V2 banner content (no "2.9", no "aiobotocore", no "Triggerer").
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\ntrigger:\n  dataset:\n    uri: s3://x\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        // AF3 banner lines
        assert!(
            script.contains("# Airflow version contract:"),
            "V3 banner header line must be present: {script}"
        );
        assert!(
            script.contains("#   - apache-airflow >= 3.0"),
            "V3 banner must include apache-airflow >= 3.0: {script}"
        );
        assert!(
            script.contains("#   - apache-airflow-providers-amazon >= 9.0.0"),
            "V3 banner must include providers-amazon >= 9.0.0: {script}"
        );
        assert!(
            script.contains("#   - apache-airflow-providers-standard"),
            "V3 banner must include providers-standard: {script}"
        );
        // Negative: no V2 banner content
        assert!(
            !script.contains("apache-airflow >= 2.9"),
            "V3 must NOT contain V2 airflow >= 2.9 banner: {script}"
        );
        assert!(
            !script.contains("aiobotocore"),
            "V3 must NOT contain aiobotocore: {script}"
        );
        assert!(
            !script.contains("Triggerer"),
            "V3 must NOT contain Triggerer: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 event-driven DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_v3_schedule_only_still_no_banner() {
        // PRES-02: schedule-only DAGs never get banner regardless of version.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\nschedule: \"@daily\"\n",
        );

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        assert!(
            !script.contains("# Airflow version contract:"),
            "V3 schedule-only DAG must NOT render banner (PRES-02): {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 schedule-only DAG has syntax error:\n{script}"
        );
    }

    #[test]
    fn dag_v3_no_v2_import_strings_leak() {
        // Kitchen-sink negative leak test: version "3" with every emission site
        // active at once (trigger with Datasets + S3 + SQS, DAG-level publishes,
        // per-task publishes, bash task). No V2 import strings may appear.
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "version: \"3\"\ntrigger:\n  all:\n    - dataset:\n        uri: s3://warehouse/foo\n    - s3:\n        bucket: mybucket\n        prefix: input/\n    - sqs:\n        queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\npublishes:\n  - s3://warehouse/output_a\n  - s3://warehouse/output_b\n",
        );

        let mut manifest = empty_manifest("test");
        let mut job = bash_job("echo work", &dag_dir);
        job.airflow = Some(AirflowJobBlock {
            publishes: vec!["s3://warehouse/per_task_out".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("worker".into(), job);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();

        // Positive: V3 strings present
        assert!(
            script.contains("from airflow.sdk import Asset"),
            "V3 must emit Asset import: {script}"
        );
        assert!(
            script.contains(
                "from airflow.providers.standard.operators.bash import BashOperator"
            ),
            "V3 must emit providers-standard BashOperator: {script}"
        );
        assert!(
            script.contains(
                "from airflow.providers.standard.operators.empty import EmptyOperator"
            ),
            "V3 must emit providers-standard EmptyOperator: {script}"
        );
        assert!(
            script.contains("Asset("),
            "V3 must contain Asset( class usage: {script}"
        );

        // Negative: no V2 strings anywhere
        assert!(
            !script.contains("from airflow.datasets import Dataset"),
            "V3 kitchen-sink must NOT contain V2 Dataset import: {script}"
        );
        assert!(
            !script.contains("from airflow.operators.bash import BashOperator"),
            "V3 kitchen-sink must NOT contain V2 BashOperator import: {script}"
        );
        assert!(
            !script.contains("from airflow.operators.empty import EmptyOperator"),
            "V3 kitchen-sink must NOT contain V2 EmptyOperator import: {script}"
        );
        assert!(
            !script.contains("Dataset("),
            "V3 kitchen-sink must NOT contain Dataset( class usage: {script}"
        );
        assert!(
            validate_python_syntax(&script).is_none(),
            "V3 kitchen-sink DAG has syntax error:\n{script}"
        );
    }

    // --- Phase 56 plan 03 Task 2: Wire-format regression tests (TEST-03) ---
    // Verifies V2-default byte-identical output: omitting version field produces
    // the same rendered Python as explicit version: "2".
    // TEST-04 (asset round-trip) covered in yard-structs/src/config.rs -- Phase 55 D-17.

    #[test]
    fn dag_v2_default_byte_identical_to_pre_phase_56() {
        // TEST-03: schedule-only DAG -- no version field vs explicit version: "2"
        // must produce byte-identical rendered Python. Proves adding the version
        // field with default V2 does not change output.
        let tmp_none = setup_project_tree();
        let root_none = tmp_none.path();
        let dag_dir_none = root_none.join("pipeline");
        write_yaml(
            &dag_dir_none.join("dag.yaml"),
            "schedule: \"@daily\"\n",
        );
        let mut manifest_none = empty_manifest("test");
        manifest_none
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir_none));
        let dags_none = collect_dags(root_none, &manifest_none).unwrap();
        let script_no_version =
            generate_dag(&manifest_none, &dags_none[0], &HashMap::new()).unwrap();

        let tmp_v2 = setup_project_tree();
        let root_v2 = tmp_v2.path();
        let dag_dir_v2 = root_v2.join("pipeline");
        write_yaml(
            &dag_dir_v2.join("dag.yaml"),
            "version: \"2\"\nschedule: \"@daily\"\n",
        );
        let mut manifest_v2 = empty_manifest("test");
        manifest_v2
            .jobs
            .insert("runit".into(), bash_job("echo hi", &dag_dir_v2));
        let dags_v2 = collect_dags(root_v2, &manifest_v2).unwrap();
        let script_explicit_v2 =
            generate_dag(&manifest_v2, &dags_v2[0], &HashMap::new()).unwrap();

        assert_eq!(
            script_no_version, script_explicit_v2,
            "schedule-only DAG: no-version and explicit V2 must be byte-identical"
        );
    }

    #[test]
    fn dag_v2_event_driven_byte_identical_to_pre_phase_56() {
        // TEST-03 (event-driven variant): dataset-triggered DAG with no version
        // field vs explicit version: "2" must produce byte-identical rendered
        // Python. Proves event-driven DAGs (banner, imports, class names) are
        // unchanged when the version field defaults to V2.
        let tmp_none = setup_project_tree();
        let root_none = tmp_none.path();
        let dag_dir_none = root_none.join("pipeline");
        write_yaml(
            &dag_dir_none.join("dag.yaml"),
            "trigger:\n  dataset:\n    uri: s3://warehouse/foo\n",
        );
        let mut manifest_none = empty_manifest("test");
        manifest_none
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir_none));
        let dags_none = collect_dags(root_none, &manifest_none).unwrap();
        let script_no_version =
            generate_dag(&manifest_none, &dags_none[0], &HashMap::new()).unwrap();

        let tmp_v2 = setup_project_tree();
        let root_v2 = tmp_v2.path();
        let dag_dir_v2 = root_v2.join("pipeline");
        write_yaml(
            &dag_dir_v2.join("dag.yaml"),
            "version: \"2\"\ntrigger:\n  dataset:\n    uri: s3://warehouse/foo\n",
        );
        let mut manifest_v2 = empty_manifest("test");
        manifest_v2
            .jobs
            .insert("agg".into(), bash_job("echo agg", &dag_dir_v2));
        let dags_v2 = collect_dags(root_v2, &manifest_v2).unwrap();
        let script_explicit_v2 =
            generate_dag(&manifest_v2, &dags_v2[0], &HashMap::new()).unwrap();

        assert_eq!(
            script_no_version, script_explicit_v2,
            "event-driven DAG: no-version and explicit V2 must be byte-identical"
        );
    }
}
