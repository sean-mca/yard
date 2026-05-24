//! Shared display helper for `apply` and `plan` commands.
//!
//! Owns the formatted plan-diff output (header, target subheader, per-diff
//! lines). Empty-state messaging is the caller's responsibility — this helper
//! assumes at least one diff in either `job_diffs` or `dag_diffs` (per
//! CONTEXT D-16: "No changes to apply." / "No changes. Infrastructure is up
//! to date." stay per-command).
//!
//! Lives in yard-cli (not yard-core) because it depends on
//! `yard-cli/src/utils.rs` color helpers which are TTY-aware and driven by
//! the process-wide COLOR_MODE atomic — terminal-presentation code belongs
//! in the CLI layer (CONTEXT D-14).
//!
//! Deterministic Modify rendering: `yard_structs::DiffType::Modify::changes` is
//! a `BTreeMap<String, (String, String)>` (Phase 28 / D-16), so iteration is
//! sorted by key without per-site sort logic. Direct `for (key, val) in changes`
//! is correct here.

use crate::utils::{bold, color_create, color_delete, color_modify};
use std::io;
use yard_structs::{DagDiff, DiffType, JobDiff};

/// Format the plan diff body for `apply` and `plan` commands.
///
/// Writes the `--- Plan for {project} ---` header, optional
/// `(targeting: {name})\n` subheader, and per-diff lines into `out`.
/// The empty-state branch (no jobs, no dags) is the caller's
/// responsibility — this helper assumes at least one diff present.
///
/// `Modify { changes }` entries iterate in key-sorted order via the BTreeMap
/// field type (Phase 28 / D-16).
pub fn print_plan_summary(
    out: &mut impl io::Write,
    project_name: &str,
    target: Option<&str>,
    job_diffs: &[JobDiff],
    dag_diffs: &[DagDiff],
) -> io::Result<()> {
    writeln!(
        out,
        "{}",
        bold(&format!("--- Plan for {} ---", project_name))
    )?;
    if let Some(name) = target {
        writeln!(out, "(targeting: {})\n", name)?;
    } else {
        writeln!(out)?;
    }

    for diff in job_diffs {
        match &diff.diff_type {
            DiffType::Create => {
                writeln!(
                    out,
                    "{}",
                    color_create(&format!("  + Create job [{}]", diff.name))
                )?;
            }
            DiffType::Modify { changes } => {
                writeln!(
                    out,
                    "{}",
                    color_modify(&format!("  ~ Modify job [{}]", diff.name))
                )?;
                for (key, (old, new)) in changes {
                    writeln!(out, "      {} : {} -> {}", key, old, new)?;
                }
            }
            DiffType::Delete => {
                writeln!(
                    out,
                    "{}",
                    color_delete(&format!("  - Delete job [{}]", diff.name))
                )?;
            }
            _ => {
                writeln!(out, "  ? Changed job [{}]", diff.name)?;
            }
        }
    }

    for diff in dag_diffs {
        match &diff.diff_type {
            DiffType::Create => {
                writeln!(
                    out,
                    "{}",
                    color_create(&format!("  + Create DAG [{}]", diff.name))
                )?;
            }
            DiffType::Modify { changes } => {
                writeln!(
                    out,
                    "{}",
                    color_modify(&format!("  ~ Modify DAG [{}]", diff.name))
                )?;
                for (key, (old, new)) in changes {
                    writeln!(out, "      {} : {} -> {}", key, old, new)?;
                }
            }
            DiffType::Delete => {
                writeln!(
                    out,
                    "{}",
                    color_delete(&format!("  - Delete DAG [{}]", diff.name))
                )?;
            }
            _ => {
                writeln!(out, "  ? Changed DAG [{}]", diff.name)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use yard_structs::{DagDiff, DiffType, JobDiff};

    // Tiny inline fixture builders. Every field of JobDiff / DagDiff is
    // initialized explicitly because yard_structs does not derive `Default`
    // on these types (revision Blocker 3 fix). The helper does NOT consume
    // `old_hash` / `new_hash` — values are stable test fixtures chosen for
    // grep-stability and documentation, not behavior.
    fn job_create(name: &str) -> JobDiff {
        JobDiff {
            name: name.to_string(),
            diff_type: DiffType::Create,
            old_hash: None,
            new_hash: Some("new".to_string()),
        }
    }
    fn job_modify(name: &str, changes: Vec<(&str, &str, &str)>) -> JobDiff {
        let mut map: BTreeMap<String, (String, String)> = BTreeMap::new();
        for (k, old, new) in changes {
            map.insert(k.to_string(), (old.to_string(), new.to_string()));
        }
        JobDiff {
            name: name.to_string(),
            diff_type: DiffType::Modify { changes: map },
            old_hash: Some("old".to_string()),
            new_hash: Some("new".to_string()),
        }
    }
    fn job_delete(name: &str) -> JobDiff {
        JobDiff {
            name: name.to_string(),
            diff_type: DiffType::Delete,
            old_hash: Some("old".to_string()),
            new_hash: None,
        }
    }
    fn dag_create(name: &str) -> DagDiff {
        DagDiff {
            name: name.to_string(),
            diff_type: DiffType::Create,
            old_hash: None,
            new_hash: Some("new".to_string()),
        }
    }
    fn dag_modify(name: &str, changes: Vec<(&str, &str, &str)>) -> DagDiff {
        let mut map: BTreeMap<String, (String, String)> = BTreeMap::new();
        for (k, old, new) in changes {
            map.insert(k.to_string(), (old.to_string(), new.to_string()));
        }
        DagDiff {
            name: name.to_string(),
            diff_type: DiffType::Modify { changes: map },
            old_hash: Some("old".to_string()),
            new_hash: Some("new".to_string()),
        }
    }
    fn dag_delete(name: &str) -> DagDiff {
        DagDiff {
            name: name.to_string(),
            diff_type: DiffType::Delete,
            old_hash: Some("old".to_string()),
            new_hash: None,
        }
    }

    fn run(target: Option<&str>, jobs: &[JobDiff], dags: &[DagDiff]) -> String {
        let mut buf: Vec<u8> = Vec::new();
        print_plan_summary(&mut buf, "myproj", target, jobs, dags).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn create_job_only() {
        let s = run(None, &[job_create("loader")], &[]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  + Create job [loader]\n"
        );
    }

    #[test]
    fn modify_job_with_multiple_changes_sorted_by_key() {
        // BTreeMap iterates entries by key: memory < script. This assertion
        // would have been flaky against the pre-refactor plan.rs/apply.rs
        // (HashMap iter order randomized per run); under Phase 28's
        // BTreeMap-typed `DiffType::Modify::changes` (D-16) iteration is
        // sorted at the type level.
        let s = run(
            None,
            &[job_modify(
                "loader",
                vec![("script", "a.py", "b.py"), ("memory", "1g", "2g")],
            )],
            &[],
        );
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  ~ Modify job [loader]\n      memory : 1g -> 2g\n      script : a.py -> b.py\n"
        );
    }

    #[test]
    fn delete_job() {
        let s = run(None, &[job_delete("stale")], &[]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  - Delete job [stale]\n"
        );
    }

    #[test]
    fn create_dag() {
        let s = run(None, &[], &[dag_create("pipeline")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  + Create DAG [pipeline]\n"
        );
    }

    #[test]
    fn modify_dag() {
        let s = run(
            None,
            &[],
            &[dag_modify("pipeline", vec![("schedule", "@hourly", "@daily")])],
        );
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  ~ Modify DAG [pipeline]\n      schedule : @hourly -> @daily\n"
        );
    }

    #[test]
    fn delete_dag() {
        let s = run(None, &[], &[dag_delete("old_pipeline")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  - Delete DAG [old_pipeline]\n"
        );
    }

    #[test]
    fn job_and_dag_mix_no_target() {
        let s = run(
            None,
            &[job_create("loader")],
            &[dag_create("pipeline")],
        );
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  + Create job [loader]\n  + Create DAG [pipeline]\n"
        );
    }

    #[test]
    fn job_only_with_target_filter() {
        let s = run(Some("loader"), &[job_create("loader")], &[]);
        // `writeln!(out, "(targeting: {})\n", name)` emits "(targeting: loader)\n"
        // from the format string's trailing \n, plus another "\n" from writeln
        // itself → byte sequence "(targeting: loader)\n\n". Mirrors the
        // pre-refactor `println!("(targeting: {})\n", name)` byte output.
        assert_eq!(
            s,
            "--- Plan for myproj ---\n(targeting: loader)\n\n  + Create job [loader]\n"
        );
    }

    #[test]
    fn dag_only_with_target_filter() {
        let s = run(Some("pipeline"), &[], &[dag_create("pipeline")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n(targeting: pipeline)\n\n  + Create DAG [pipeline]\n"
        );
    }
}
