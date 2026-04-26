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
//! Deterministic stabilization (Phase 20 / Plan 04 revision Blocker 1):
//! `yard_structs::DiffType::Modify` holds `changes: HashMap<String, (String,
//! String)>`. HashMap iteration is unordered. The pre-helper code in plan.rs
//! and apply.rs iterated this HashMap directly, producing non-deterministic
//! line ordering between runs for multi-key Modify diffs. This helper sorts
//! `changes` entries by key before iterating — closing the latent ordering
//! bug. SC #3's "byte-identical output" guarantee thus becomes
//! "byte-identical for the previously deterministic subset, AND deterministic
//! across runs for the previously non-deterministic Modify rendering".

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
/// `Modify { changes }` entries are sorted by key before printing so output
/// is deterministic across runs (HashMap iteration would otherwise be
/// unordered — see module-level rustdoc).
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
                // Deterministic stabilization (revision Blocker 1): sort by key
                // before iterating; HashMap iteration order is otherwise unspecified.
                let mut sorted: Vec<(&String, &(String, String))> =
                    changes.iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(b.0));
                for (key, (old, new)) in sorted {
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
                // Same sort-by-key transform as the job Modify branch above.
                let mut sorted: Vec<(&String, &(String, String))> =
                    changes.iter().collect();
                sorted.sort_by(|a, b| a.0.cmp(b.0));
                for (key, (old, new)) in sorted {
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
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;
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
        let mut map: HashMap<String, (String, String)> = HashMap::new();
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
        let mut map: HashMap<String, (String, String)> = HashMap::new();
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
        // Helper sorts HashMap entries by key: memory < script.
        // This assertion would have been flaky against the pre-refactor
        // plan.rs/apply.rs (HashMap iter order randomized per run); the
        // helper's sort step makes it stable. (Revision Blocker 1.)
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
