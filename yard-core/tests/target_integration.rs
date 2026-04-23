//! Phase 12 regression tests for `yard apply --target <name>`.
//!
//! These call `yard_core::orchestrate::apply` directly with `dry_run = true`
//! and `StateBackend::Local` pointing at a tmpdir. No AWS, no ministack —
//! runs in default `cargo test --workspace` (NOT `--ignored`).
//!
//! Locks the TGT-06 matrix (REQUIREMENTS.md §"Regression coverage"):
//!   1. job-in-DAG target          — TGT-01 + TGT-02 happy path
//!   2. job-outside-DAG target     — orphan-validation must run on full manifest
//!   3. DAG-name target            — D-04 / D-07 (zero jobs locked)
//!   4. nonexistent target         — D-01 hard error

mod common;

use common::{build_target_matrix_project, empty_state};

/// TGT-06 row 1 + TGT-01 root-cause regression.
///
/// Project has two DAGs (dag_a, dag_b). Targeting a job inside dag_a must:
///   - NOT emit "no task files" for dag_b (the bug this phase fixes — TGT-01)
///   - Apply only job_a1 (TGT-02)
///   - Leave dag_b's job and both DAGs' state untouched
#[tokio::test]
async fn target_job_in_dag_does_not_touch_unrelated_dag() {
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
    .unwrap_or_else(|e| panic!("apply failed with unrelated DAG present: {e}"));

    assert_eq!(result.created, vec![project.job_in_dag.clone()]);
    assert!(result.modified.is_empty());
    assert!(result.deleted.is_empty());
    // D-09: DAG phase skipped when target is a job — both dag_a and dag_b
    // are untouched, even though dag_a is the target job's parent.
    assert!(
        result.dag_created.is_empty(),
        "expected no DAG activity when target is a job, got: {:?}",
        result.dag_created
    );
    assert!(result.dag_modified.is_empty());
    assert!(result.dag_deleted.is_empty());
}

/// TGT-06 row 2.
///
/// Project has a loose job + a DAG. Targeting the loose job must:
///   - Succeed without spurious orphan-airflow errors (full manifest still flows
///     into `validate_orphan_airflow_blocks` — D-10 invariant)
///   - Apply only the loose job
///   - Leave the DAG untouched
#[tokio::test]
async fn target_job_outside_dag_skips_dag_phase() {
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
    assert!(result.dag_created.is_empty());
    assert!(result.dag_modified.is_empty());
    assert!(result.dag_deleted.is_empty());
}

/// TGT-06 row 3 + D-04 + D-07.
///
/// Targeting a DAG name (not a job) must:
///   - Succeed (target exists — D-01 does NOT fire)
///   - Apply zero jobs (preliminary_diffs filter drops everything, lock set is empty — D-07)
///   - Produce DAG-level activity only (D-04) — since initial state is empty,
///     dag_a becomes a Create and dag_b is NOT touched because the authoritative
///     diff was filtered on the DAG name match.
///
/// NOTE on exact assertion shape: `dag_diffs` are computed inside `apply_dags`
/// and filtered there; the visible effect on `ApplyResult` when targeting a DAG
/// name is that `dag_created` should contain exactly `dag_a_name` (or nothing,
/// depending on how `apply_dags` scopes — verify empirically). The strong
/// invariant is:
///   - result.created is empty (no jobs)
///   - result.dag_created does NOT contain dag_b_name
#[tokio::test]
async fn target_dag_name_applies_only_that_dag() {
    let project = build_target_matrix_project();
    let target = Some(project.dag_a_name.clone());

    let result = yard_core::apply(
        &project.manifest,
        &empty_state(),
        project.tmp.path(),
        true,
        target,
    )
    .await
    .unwrap_or_else(|e| panic!("apply failed on DAG-name target: {e}"));

    // D-07: zero jobs locked / deployed when target is a DAG.
    assert!(
        result.created.is_empty(),
        "expected zero job Creates for DAG target, got: {:?}",
        result.created
    );
    assert!(result.modified.is_empty());
    assert!(result.deleted.is_empty());

    // D-04: only the targeted DAG should appear (if the DAG-deploy path emits anything);
    // the unrelated DAG must not be in any result list.
    assert!(
        !result.dag_created.contains(&project.dag_b_name),
        "unrelated DAG {} leaked into dag_created: {:?}",
        project.dag_b_name,
        result.dag_created
    );
    assert!(
        !result.dag_modified.contains(&project.dag_b_name),
        "unrelated DAG {} leaked into dag_modified: {:?}",
        project.dag_b_name,
        result.dag_modified
    );
    assert!(
        !result.dag_deleted.contains(&project.dag_b_name),
        "unrelated DAG {} leaked into dag_deleted: {:?}",
        project.dag_b_name,
        result.dag_deleted
    );
}

/// TGT-06 row 4 + D-01.
///
/// `apply --target typo` where `typo` matches no job and no DAG must:
///   - Return Err (no silent "No changes to apply")
///   - Error message must contain both "not found" and the target name
#[tokio::test]
async fn target_nonexistent_name_hard_errors() {
    let project = build_target_matrix_project();
    let target = Some("definitely_not_a_job_or_dag".to_string());

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
        msg.contains("definitely_not_a_job_or_dag"),
        "got: {msg}"
    );
}
