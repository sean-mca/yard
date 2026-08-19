//! Shared fixture helpers for `--target` integration tests.
//!
//! Reusable by Phase 12 (`target_integration.rs`) and Phase 13 (`plan_target_integration.rs`).
//! Uses a hand-rolled `TempDir` — no `tempfile` crate (CLAUDE.md Minimal Deps +
//! yard-core/src/airflow_dag/mod.rs:82-106 precedent).

#![allow(dead_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
// `dead_code` is allow-listed at the module level because individual tests use
// different subsets of the fixture helpers. Without this, `cargo test` warns for
// every helper that the current test binary doesn't touch, and those warnings
// are upgraded to errors under the workspace `-D warnings` clippy gate. This is
// a test-harness-only file, not production code, so the allow is scoped here
// exclusively.

use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use yard_structs::{
    Deployment, JobDefinition, JobType, ProjectManifest, ProjectState, StateBackend,
};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Minimal scoped temp dir that cleans up on drop. Avoids pulling in the
/// `tempfile` crate as a dev-dependency (see yard-core/src/airflow_dag/mod.rs:82-106).
pub struct TempDir {
    pub path: PathBuf,
}

impl TempDir {
    pub fn new() -> Self {
        let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir()
            .join(format!("yard_target_{}_{}", std::process::id(), n));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Write a file, creating parent dirs as needed.
pub fn write_yaml(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, body).unwrap();
}

/// Build an empty initial state (no prior deployments) — `apply` will see
/// every job as a fresh Create.
pub fn empty_state() -> ProjectState {
    ProjectState {
        project: "test".to_string(),
        last_updated: String::new(),
        deployments: HashMap::new(),
    }
}

/// Minimal bash job — task-only (no provider wiring required even when dry_run=false,
/// though Phase 12 tests always pass dry_run=true).
fn bash_job(command: &str, dir: &Path) -> JobDefinition {
    JobDefinition {
        job_type: JobType::Bash,
        config: json!({"type": "bash", "command": command}),
        dir: dir.to_path_buf(),
        ..Default::default()
    }
}

/// Returned by `build_target_matrix_project` — bundles the tmpdir (so the caller
/// can control lifetime), the built manifest, and the job/DAG names the tests assert on.
pub struct TargetMatrixProject {
    pub tmp: TempDir,
    pub manifest: ProjectManifest,
    /// Job that lives INSIDE dag_a (TGT-06 row 1 targets this).
    pub job_in_dag: String,
    /// Job that lives OUTSIDE any DAG (TGT-06 row 2 targets this).
    pub job_outside: String,
    /// DAG name (first DAG) — TGT-06 row 3 targets this. `dag_a` in the
    /// on-disk layout, but the resolved DAG name is prefixed by the project
    /// name (per airflow_dag::collection — e.g. "test_dag_a").
    pub dag_a_name: String,
    /// Second DAG name — must remain untouched when targeting job-in-dag_a.
    pub dag_b_name: String,
}

/// Build a project that exercises the full TGT-06 matrix:
///
/// ```text
/// tmpdir/
///   yard.yaml
///   account.yaml
///   region.yaml
///   job_outside/config.yaml          # loose job, no DAG
///   dag_a/
///     dag.yaml
///     job_a1/config.yaml             # job inside dag_a
///     job_a2/config.yaml             # job inside dag_a
///   dag_b/
///     dag.yaml
///     job_b1/config.yaml             # job inside dag_b (must be untouched
///                                    # when targeting job_a1 — the TGT-01 regression)
/// ```
///
/// Returns the tmpdir + manifest + the names the tests need.
pub fn build_target_matrix_project() -> TargetMatrixProject {
    let tmp = TempDir::new();
    let root = tmp.path().to_path_buf();

    // Minimal context files expected by resolve::load_context analogues.
    write_yaml(&root.join("yard.yaml"), "project: test\n");
    write_yaml(&root.join("account.yaml"), "account:\n  id: \"123\"\n");
    write_yaml(&root.join("region.yaml"), "region:\n  id: us-east-1\n");

    // job_outside — loose job, no DAG ancestor.
    let job_outside_dir = root.join("job_outside");
    write_yaml(
        &job_outside_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo outside\"\n",
    );

    // dag_a with 2 jobs inside.
    let dag_a_dir = root.join("dag_a");
    write_yaml(&dag_a_dir.join("dag.yaml"), "schedule: \"@daily\"\n");
    let job_a1_dir = dag_a_dir.join("job_a1");
    let job_a2_dir = dag_a_dir.join("job_a2");
    write_yaml(
        &job_a1_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo a1\"\n",
    );
    write_yaml(
        &job_a2_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo a2\"\n",
    );

    // dag_b with 1 job inside — this is the "unrelated DAG" for TGT-01.
    let dag_b_dir = root.join("dag_b");
    write_yaml(&dag_b_dir.join("dag.yaml"), "schedule: \"@daily\"\n");
    let job_b1_dir = dag_b_dir.join("job_b1");
    write_yaml(
        &job_b1_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo b1\"\n",
    );

    // Assemble the ProjectManifest with all 4 jobs and the DAGs inferred on-disk.
    let state_dir = root.join(".yard/state");
    let mut jobs = HashMap::new();
    jobs.insert(
        "job_outside".to_string(),
        bash_job("echo outside", &job_outside_dir),
    );
    jobs.insert("job_a1".to_string(), bash_job("echo a1", &job_a1_dir));
    jobs.insert("job_a2".to_string(), bash_job("echo a2", &job_a2_dir));
    jobs.insert("job_b1".to_string(), bash_job("echo b1", &job_b1_dir));

    let manifest = ProjectManifest {
        project: "test".to_string(),
        state: StateBackend::Local { path: state_dir },
        providers: HashMap::new(),
        jobs,
        aws: None,
    };

    // DAG names are computed by airflow_dag::collection as `<project>_<dirname>`.
    // Confirmed at yard-core/src/airflow_dag/mod.rs:189 in the existing test:
    // `assert_eq!(dags[0].name, "test_pipeline")` for dag dir `pipeline`.
    TargetMatrixProject {
        tmp,
        manifest,
        job_in_dag: "job_a1".to_string(),
        job_outside: "job_outside".to_string(),
        dag_a_name: "test_dag_a".to_string(),
        dag_b_name: "test_dag_b".to_string(),
    }
}

/// Returned by `build_dir_scoped_project` — bundles the tmpdir, manifest, and
/// job names for `--dir` integration tests.
pub struct DirScopedProject {
    pub tmp: TempDir,
    pub manifest: ProjectManifest,
    pub sub_a_job: String,
    pub sub_b_job: String,
    pub root_job: String,
}

/// Build a project with jobs in separate subdirectories for `--dir` tests.
///
/// ```text
/// tmpdir/
///   yard.yaml
///   account.yaml
///   region.yaml
///   sub_a/job_alpha/config.yaml
///   sub_b/job_beta/config.yaml
///   root_level/job_gamma/config.yaml
/// ```
pub fn build_dir_scoped_project() -> DirScopedProject {
    let tmp = TempDir::new();
    let root = tmp.path().to_path_buf();

    write_yaml(&root.join("yard.yaml"), "project: test\n");
    write_yaml(&root.join("account.yaml"), "account:\n  id: \"123\"\n");
    write_yaml(&root.join("region.yaml"), "region:\n  id: us-east-1\n");

    let alpha_dir = root.join("sub_a").join("job_alpha");
    write_yaml(
        &alpha_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo alpha\"\n",
    );

    let beta_dir = root.join("sub_b").join("job_beta");
    write_yaml(
        &beta_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo beta\"\n",
    );

    let gamma_dir = root.join("root_level").join("job_gamma");
    write_yaml(
        &gamma_dir.join("config.yaml"),
        "type: bash\ncommand: \"echo gamma\"\n",
    );

    let state_dir = root.join(".yard/state");
    let mut jobs = HashMap::new();
    jobs.insert("job_alpha".to_string(), bash_job("echo alpha", &alpha_dir));
    jobs.insert("job_beta".to_string(), bash_job("echo beta", &beta_dir));
    jobs.insert("job_gamma".to_string(), bash_job("echo gamma", &gamma_dir));

    let manifest = ProjectManifest {
        project: "test".to_string(),
        state: StateBackend::Local { path: state_dir },
        providers: HashMap::new(),
        jobs,
        aws: None,
    };

    DirScopedProject {
        tmp,
        manifest,
        sub_a_job: "job_alpha".to_string(),
        sub_b_job: "job_beta".to_string(),
        root_job: "job_gamma".to_string(),
    }
}

/// Prevent the compiler from warning about the unused `Deployment` import — it's
/// pulled in for Phase 13 reuse (state-preload helpers that will be added there).
#[allow(dead_code)]
fn _force_deployment_import() -> Option<Deployment> {
    None
}
