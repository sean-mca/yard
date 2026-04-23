//! Phase 13 regression tests for `yard plan --target <name>`.
//!
//! These call `yard_core::plan` directly with `StateBackend::Local` pointing
//! at a tmpdir. No AWS, no ministack — runs in default `cargo test --workspace`
//! (NOT `--ignored`).
//!
//! Locks the TGT-07 matrix (mirror of TGT-06):
//!   1. job-in-DAG target          — D-09 parity with apply
//!   2. job-outside-DAG target     — orphan-validation must run on full manifest
//!   3. DAG-name target            — D-09 / D-10 (only DAG diff surfaces)
//!   4. nonexistent target         — D-04 hard error

mod common;

use common::{build_target_matrix_project, empty_state};

/// TGT-07 row 1.
///
/// Project has two DAGs (dag_a, dag_b). `plan --target <job-in-dag_a>` must:
///   - NOT error on orphan-airflow validation for dag_b (parity with apply's TGT-01)
///   - Return a PlanResult with exactly one job_diff for job_a1
///   - Return an empty dag_diffs vec (DAG phase not surfaced when target is a job)
#[tokio::test]
async fn plan_target_job_in_dag_does_not_show_unrelated_dag() {
    let project = build_target_matrix_project();
    let target = Some(project.job_in_dag.clone());

    let result = yard_core::plan(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("plan failed with unrelated DAG present: {e}"));

    assert_eq!(result.job_diffs.len(), 1);
    assert_eq!(result.job_diffs[0].name, project.job_in_dag);
    assert!(
        result.dag_diffs.is_empty(),
        "expected empty dag_diffs when target is a job, got: {:?}",
        result.dag_diffs.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
}

/// TGT-07 row 2.
///
/// Project has a loose job + a DAG. Targeting the loose job must:
///   - Succeed without spurious orphan-airflow errors (full manifest still flows
///     into `validate_orphan_airflow_blocks` — D-10 invariant from Phase 12)
///   - Return exactly one job_diff for the loose job
///   - Return empty dag_diffs
#[tokio::test]
async fn plan_target_job_outside_dag_skips_dag_phase() {
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
    assert!(result.dag_diffs.is_empty());
}

/// TGT-07 row 3 + D-09 parity.
///
/// Targeting a DAG name must:
///   - Succeed (target exists — D-04 does NOT fire)
///   - Return empty job_diffs
///   - Return dag_diffs with dag_a_name present AND dag_b_name NOT present
///
/// Stronger than Phase 12 row 3: plan is read-only so the positive assertion
/// (dag_a in dag_diffs) is trustworthy — the diff vec is a direct return.
#[tokio::test]
async fn plan_target_dag_name_shows_only_that_dag() {
    let project = build_target_matrix_project();
    let target = Some(project.dag_a_name.clone());

    let result = yard_core::plan(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("plan failed on DAG-name target: {e}"));

    assert!(
        result.job_diffs.is_empty(),
        "expected zero job diffs for DAG target, got: {:?}",
        result.job_diffs.iter().map(|d| &d.name).collect::<Vec<_>>()
    );

    let dag_names: Vec<&str> = result.dag_diffs.iter().map(|d| d.name.as_str()).collect();
    assert!(
        dag_names.contains(&project.dag_a_name.as_str()),
        "expected dag_a in dag_diffs, got: {:?}",
        dag_names
    );
    assert!(
        !dag_names.contains(&project.dag_b_name.as_str()),
        "unrelated DAG {} leaked into dag_diffs: {:?}",
        project.dag_b_name,
        dag_names
    );
}

/// TGT-07 row 4 + D-04.
///
/// `plan --target typo` where `typo` matches no job and no DAG must:
///   - Return Err (no silent "No changes. Infrastructure is up to date.")
///   - Error message must contain both "not found" and the target name
#[tokio::test]
async fn plan_target_nonexistent_name_hard_errors() {
    let project = build_target_matrix_project();
    let target = Some("definitely_not_a_job_or_dag".to_string());

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
        msg.contains("definitely_not_a_job_or_dag"),
        "got: {msg}"
    );
}
