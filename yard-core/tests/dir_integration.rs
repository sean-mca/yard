//! Integration tests for `--dir` directory-scoped filtering.
//!
//! Verifies that `filter_manifest_by_dir` correctly scopes plan, apply,
//! destroy, and validate operations to a subdirectory, and that edge cases
//! (nonexistent, outside root, zero-match, file path) produce clear errors.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{build_dir_scoped_project, empty_state};
use std::path::Path;
use yard_core::resolve::filter_manifest_by_dir;

#[tokio::test]
async fn plan_dir_filters_to_subdirectory() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();

    let filtered = filter_manifest_by_dir(&p.manifest, Path::new("sub_a"), root).unwrap();
    assert_eq!(filtered.manifest.jobs.len(), 1);
    assert!(filtered.manifest.jobs.contains_key(&p.sub_a_job));
    assert!(filtered.display_path.ends_with("sub_a/"));

    let result = yard_core::plan(&filtered.manifest, &empty_state(), root, None)
        .await
        .unwrap();
    assert_eq!(result.job_diffs.len(), 1);
    assert_eq!(result.job_diffs[0].name, p.sub_a_job);
}

#[tokio::test]
async fn plan_dir_no_jobs_in_subdir_errors() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();
    std::fs::create_dir_all(root.join("empty_dir")).unwrap();

    let result = filter_manifest_by_dir(&p.manifest, Path::new("empty_dir"), root);
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("no jobs found under"), "got: {msg}");
}

#[tokio::test]
async fn plan_dir_nonexistent_path_errors() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();

    let result = filter_manifest_by_dir(&p.manifest, Path::new("does_not_exist"), root);
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("directory not found"), "got: {msg}");
}

#[tokio::test]
async fn plan_dir_outside_root_errors() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();
    let outside = std::env::temp_dir();

    let result = filter_manifest_by_dir(&p.manifest, &outside, root);
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("outside the project root"), "got: {msg}");
}

#[tokio::test]
async fn plan_dir_file_path_errors() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();
    let file = root.join("not_a_dir.txt");
    std::fs::write(&file, "hello").unwrap();

    let result = filter_manifest_by_dir(&p.manifest, &file, root);
    assert!(result.is_err());
    let msg = format!("{}", result.unwrap_err());
    assert!(msg.contains("expected a directory"), "got: {msg}");
}

#[tokio::test]
async fn apply_dir_filters_to_subdirectory() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();

    let filtered = filter_manifest_by_dir(&p.manifest, Path::new("sub_a"), root).unwrap();
    let result = yard_core::apply(&filtered.manifest, &empty_state(), root, true, None)
        .await
        .unwrap();

    assert!(
        result.created.contains(&p.sub_a_job),
        "expected job_alpha in created: {:?}",
        result.created
    );
    assert!(
        !result.created.contains(&p.sub_b_job),
        "job_beta should not be in created"
    );
    assert!(
        !result.created.contains(&p.root_job),
        "job_gamma should not be in created"
    );
}

#[tokio::test]
async fn destroy_dir_filters_to_subdirectory() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();

    let filtered = filter_manifest_by_dir(&p.manifest, Path::new("sub_a"), root).unwrap();
    let mut names: Vec<&String> = filtered.manifest.jobs.keys().collect();
    names.sort();

    assert_eq!(names.len(), 1);
    assert_eq!(names[0], &p.sub_a_job);

    for name in &names {
        let destroyed = yard_core::destroy_job(
            &p.manifest.state,
            &p.manifest.providers,
            name,
            root,
            true,
        )
        .await
        .unwrap();
        assert!(!destroyed, "no state exists, so destroy returns false");
    }
}

#[tokio::test]
async fn validate_dir_filters_to_subdirectory() {
    let p = build_dir_scoped_project();
    let root = p.tmp.path();

    let filtered = filter_manifest_by_dir(&p.manifest, Path::new("sub_a"), root).unwrap();
    assert_eq!(filtered.manifest.jobs.len(), 1);

    for (name, job_def) in &filtered.manifest.jobs {
        let errors = yard_core::validation::validate_job_full(name, job_def);
        assert!(
            errors.is_empty(),
            "expected valid bash job, got errors: {errors:?}"
        );
    }
}
