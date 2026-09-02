#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Phase 13 regression tests for `yard plan --target <name>`.
//!
//! These call `yard_core::plan` directly with `StateBackend::Local` pointing
//! at a tmpdir. No AWS, no ministack -- runs in default `cargo test --workspace`
//! (NOT `--ignored`).

mod common;

use common::{build_target_matrix_project, empty_state};

/// Plan with a specific job target returns only that job's diff.
#[tokio::test]
async fn plan_target_job_returns_only_that_job() {
    let project = build_target_matrix_project();
    let target = Some(project.job_in_dag.clone());

    let result = yard_core::plan(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("plan failed: {e}"));

    assert_eq!(result.job_diffs.len(), 1);
    assert_eq!(result.job_diffs[0].name, project.job_in_dag);
}

/// Plan targeting a job outside any DAG.
#[tokio::test]
async fn plan_target_job_outside_dag() {
    let project = build_target_matrix_project();
    let target = Some(project.job_outside.clone());

    let result = yard_core::plan(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("plan failed on loose-job target: {e}"));

    assert_eq!(result.job_diffs.len(), 1);
    assert_eq!(result.job_diffs[0].name, project.job_outside);
}

/// `plan --target typo` where `typo` matches no job must return Err.
#[tokio::test]
async fn plan_target_nonexistent_name_hard_errors() {
    let project = build_target_matrix_project();
    let target = Some("definitely_not_a_job".to_string());

    let result = yard_core::plan(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        target,
    )
    .await;

    assert!(result.is_err(), "expected Err for unknown target, got Ok");
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("not found"), "got: {msg}");
    assert!(
        msg.contains("definitely_not_a_job"),
        "got: {msg}"
    );
}
