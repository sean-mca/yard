#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Phase 12 regression tests for `yard apply --target <name>`.
//!
//! These call `yard_core::orchestrate::apply` directly with `dry_run = true`
//! and `StateBackend::Local` pointing at a tmpdir. No AWS, no ministack --
//! runs in default `cargo test --workspace` (NOT `--ignored`).

mod common;

use common::{build_target_matrix_project, empty_state};

/// Targeting a specific job applies only that job.
#[tokio::test]
async fn target_job_applies_only_that_job() {
    let project = build_target_matrix_project();
    let target = Some(project.job_in_dag.clone());

    let result = yard_core::apply(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        true,
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("apply failed: {e}"));

    assert_eq!(result.created, vec![project.job_in_dag.clone()]);
    assert!(result.modified.is_empty());
    assert!(result.deleted.is_empty());
}

/// Targeting a job outside any DAG succeeds.
#[tokio::test]
async fn target_job_outside_dag() {
    let project = build_target_matrix_project();
    let target = Some(project.job_outside.clone());

    let result = yard_core::apply(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        true,
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("apply failed on loose-job target: {e}"));

    assert_eq!(result.created, vec![project.job_outside.clone()]);
}

/// `apply --target typo` where `typo` matches no job must return Err.
#[tokio::test]
async fn target_nonexistent_name_hard_errors() {
    let project = build_target_matrix_project();
    let target = Some("definitely_not_a_job".to_string());

    let result = yard_core::apply(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        true,
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
