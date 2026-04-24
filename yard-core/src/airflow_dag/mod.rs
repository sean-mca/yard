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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::PathBuf;
use yard_structs::AirflowSection;
use yard_structs::JobState;

// Re-export sub-module items as public API
pub use collection::collect_dags;
pub use connections::{derive_aws_conn_id, required_connections_for_dag};
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
/// cross-account role. The DAG-codegen layer does not manage connections —
/// this struct is emitted alongside the rendered DAG so operators can set them
/// up in MWAA.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RequiredConnection {
    pub conn_id: String,
    pub role_arn: String,
}

/// A DAG resolved from filesystem discovery + validation. Ready for codegen.
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
//
// Plan 15-01 (DAG-02) lands this helper independently of its consumers.
// Plan 15-02 wires `dag_lifecycle::apply_dags` and `show::show_dag` to call
// it; until then, the non-test lib has no caller and `-D dead_code`
// (implied by `-D warnings`) would fail CI. Remove this allow when
// Plan 15-02 merges.
#[allow(dead_code)]
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
mod tests {
    use super::*;
    use crate::validation::validate_python_syntax;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use yard_structs::{AirflowJobBlock, Deployment, ProjectManifest, Resource, StateBackend};

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
            aws: serde_json::Value::Null,
        }
    }

    fn bash_job(command: &str, dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: "bash".to_string(),
            config: json!({"type": "bash", "command": command}),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn glue_job(dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
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
            produces: vec![],
            overrides: AirflowSection {
                schedule: Some("@hourly".to_string()),
                ..Default::default()
            },
        });
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec![],
            produces: vec![],
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
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
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
            job_type: "bash".to_string(),
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
        manifest
            .jobs
            .insert("sales-orders".to_string(), a);
        manifest
            .jobs
            .insert("sales-shipments".to_string(), b);

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
        manifest
            .jobs
            .insert("sales-orders".to_string(), a);
        manifest
            .jobs
            .insert("sales-shipments".to_string(), b);

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
        manifest
            .jobs
            .insert("sales-orders".to_string(), a);
        manifest
            .jobs
            .insert("billing-orders".to_string(), b);
        manifest
            .jobs
            .insert("pipeline-notify".to_string(), c);

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
        manifest
            .jobs
            .insert("sales-orders".to_string(), a);

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
        manifest
            .jobs
            .insert("task_a".to_string(), task_a);
        manifest
            .jobs
            .insert("orphan_job".to_string(), orphan);

        let dags = collect_dags(root, &manifest).unwrap();
        let errors = validate_orphan_airflow_blocks(&manifest, &dags);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "orphan_job");
    }

    // ---- Cross-account: derive_aws_conn_id ----

    #[test]
    fn derive_aws_conn_id_happy_path() {
        let got =
            derive_aws_conn_id("arn:aws:iam::222222222222:role/GlueInvoker").unwrap();
        assert_eq!(got, "yard_222222222222_GlueInvoker");
    }

    #[test]
    fn derive_aws_conn_id_sanitizes_role_path() {
        // IAM role paths (slashes) are allowed; we sanitize them for
        // Airflow-friendly conn ids.
        let got =
            derive_aws_conn_id("arn:aws:iam::111111111111:role/path/to/MyRole").unwrap();
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
        assert!(
            derive_aws_conn_id("arn:aws:iam::222222222222:user/Alice").is_err()
        );
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

    // ---- Cross-account: render_task picks aws_conn_id per job ----

    fn glue_job_with_assume_role(dir: &Path, role_arn: &str) -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
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
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
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
        manifest.aws = json!({"assume_role": "arn:aws:iam::111111111111:role/OperatorA"});
        manifest.jobs.insert(
            "orders".into(),
            glue_job_with_assume_role(
                &dag_dir,
                "arn:aws:iam::222222222222:role/GlueInvoker",
            ),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
                .into_iter()
                .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("aws_conn_id=\"yard_222222222222_GlueInvoker\""));
        assert!(script.contains("Required Airflow connections"));
        assert!(script.contains("yard_222222222222_GlueInvoker  ->  arn:aws:iam::222222222222:role/GlueInvoker"));
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
        manifest.aws = json!({"assume_role": root_arn});
        manifest
            .jobs
            .insert("orders".into(), glue_job_with_assume_role(&dag_dir, root_arn));

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
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
        manifest.aws = json!({"assume_role": "arn:aws:iam::111111111111:role/OperatorA"});
        manifest
            .jobs
            .insert("orders".into(), glue_job_with_assume_role(&dag_dir, role_b));
        manifest
            .jobs
            .insert("shipments".into(), glue_job_with_assume_role(&dag_dir, role_b));
        manifest
            .jobs
            .insert("billing".into(), glue_job_with_assume_role(&dag_dir, role_c));

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
        manifest.jobs.insert("run".into(), bash_job("echo hi", &dag_dir));
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
        manifest
            .jobs
            .insert("orders".into(), glue_job_with_assume_role(&dag_dir, "garbage"));

        let dags = collect_dags(root, &manifest).unwrap();
        // Supply a populated script_locations so the new D-05/D-06/D-07
        // checks pass (role is set in the fixture; URI now present), letting
        // the original "malformed role ARN" error from resolve_task_aws_conn_id
        // surface unchanged.
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
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
        manifest
            .jobs
            .insert("task_a".to_string(), task_a);

        let dags = collect_dags(root, &manifest).unwrap();
        let errors = validate_orphan_airflow_blocks(&manifest, &dags);
        assert!(errors.is_empty());
    }

    // ---- Airflow Datasets ----

    #[test]
    fn task_with_produces_emits_outlets() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut job = glue_job(&dag_dir);
        job.airflow = Some(AirflowJobBlock {
            produces: vec!["s3://warehouse/sales/orders".to_string()],
            ..Default::default()
        });
        manifest.jobs.insert("orders".into(), job);

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
                .into_iter()
                .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(script.contains("from airflow.datasets import Dataset"));
        assert!(script.contains("outlets=[Dataset(\"s3://warehouse/sales/orders\")]"));
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn task_without_produces_omits_outlets() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("orders".into(), glue_job(&dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script_locations: HashMap<String, String> =
            [("orders".to_string(), "s3://bucket/scripts/orders.py".to_string())]
                .into_iter()
                .collect();
        let script = generate_dag(&manifest, &dags[0], &script_locations).unwrap();
        assert!(!script.contains("outlets"));
        assert!(!script.contains("Dataset"));
    }

    #[test]
    fn dag_triggered_by_datasets_emits_schedule_list() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "triggered_by:\n  - s3://warehouse/sales/orders\n  - s3://warehouse/sales/shipments\n",
        );

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("agg".into(), bash_job("echo agg", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("from airflow.datasets import Dataset"));
        assert!(script.contains(
            "schedule=[Dataset(\"s3://warehouse/sales/orders\"), Dataset(\"s3://warehouse/sales/shipments\")]"
        ));
        assert!(!script.contains("@daily"));
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn triggered_by_overrides_inherited_schedule() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(
            &dag_dir.join("dag.yaml"),
            "schedule: \"@daily\"\ntriggered_by:\n  - s3://warehouse/foo\n",
        );

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert("task".into(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains("schedule=[Dataset(\"s3://warehouse/foo\")]"));
        assert!(!script.contains("@daily"));
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    #[test]
    fn multiple_produces_on_one_task() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut job = bash_job("echo done", &dag_dir);
        job.airflow = Some(AirflowJobBlock {
            produces: vec![
                "s3://warehouse/a".to_string(),
                "s3://warehouse/b".to_string(),
            ],
            ..Default::default()
        });
        manifest.jobs.insert("multi".into(), job);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0], &HashMap::new()).unwrap();
        assert!(script.contains(
            "outlets=[Dataset(\"s3://warehouse/a\"), Dataset(\"s3://warehouse/b\")]"
        ));
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
                    job_name: name.to_string(),
                    project: "test".to_string(),
                    deployment: Deployment {
                        env: None,
                        config_hash: String::new(),
                        config: serde_json::Value::Null,
                        status: "deployed".to_string(),
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
        assert_eq!(out.get("alpha").map(String::as_str), Some("s3://bucket/alpha.py"));
        assert_eq!(out.get("beta").map(String::as_str), Some("s3://bucket/beta.py"));
    }

    #[test]
    fn script_locations_from_state_skips_jobs_without_s3_object() {
        let mut states: HashMap<String, JobState> = HashMap::new();
        // Job A: has s3_object
        states.insert(
            "a".to_string(),
            JobState {
                job_name: "a".to_string(),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: "deployed".to_string(),
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
                job_name: "b".to_string(),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: "deployed".to_string(),
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
                job_name: "c".to_string(),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: "deployed".to_string(),
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
                job_name: "my_job".to_string(),
                project: "test".to_string(),
                deployment: Deployment {
                    env: None,
                    config_hash: String::new(),
                    config: serde_json::Value::Null,
                    status: "deployed".to_string(),
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
        assert_eq!(out.get("my_job").map(String::as_str), Some("s3://bucket/script.py"));
        // Explicitly prove the helper did NOT pick the glue_job id:
        assert_ne!(out.get("my_job").map(String::as_str), Some("my_glue_job_name"));
    }

    #[test]
    fn script_locations_from_state_empty_input() {
        let states: HashMap<String, JobState> = HashMap::new();
        let out = script_locations_from_state(&states);
        assert!(out.is_empty());
    }
}
