//! Shared display helper for `apply` and `plan` commands.
//!
//! Owns the formatted plan-diff output (header, target subheader, per-diff
//! lines). Empty-state messaging is the caller's responsibility -- this helper
//! assumes at least one diff in `job_diffs` (per CONTEXT D-16).
//!
//! Deterministic Modify rendering: `yard_structs::DiffType::Modify::changes` is
//! a `BTreeMap<String, (String, String)>` (Phase 28 / D-16), so iteration is
//! sorted by key without per-site sort logic.

use crate::utils::{bold, color_create, color_delete, color_modify};
use std::io;
use yard_structs::{DiffType, JobDiff};

/// Format the plan diff body for `apply` and `plan` commands.
///
/// Writes the `--- Plan for {project} ---` header, optional
/// `(targeting: {name})\n` subheader, and per-diff lines into `out`.
///
/// # Errors
///
/// Returns an error if writing to the output sink fails.
pub fn print_plan_summary(
    out: &mut impl io::Write,
    project_name: &str,
    target: Option<&str>,
    dir_scope: Option<&str>,
    job_diffs: &[JobDiff],
) -> io::Result<()> {
    writeln!(
        out,
        "{}",
        bold(&format!("--- Plan for {} ---", project_name))
    )?;
    if let Some(name) = target {
        writeln!(out, "(targeting: {})\n", name)?;
    } else if let Some(scope) = dir_scope {
        writeln!(out, "(scoped to: {})\n", scope)?;
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

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use yard_structs::{DiffType, JobDiff};

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

    fn run(
        target: Option<&str>,
        dir_scope: Option<&str>,
        jobs: &[JobDiff],
    ) -> String {
        crate::utils::disable_color();
        let mut buf: Vec<u8> = Vec::new();
        print_plan_summary(&mut buf, "myproj", target, dir_scope, jobs).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn create_job_only() {
        let s = run(None, None, &[job_create("loader")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  + Create job [loader]\n"
        );
    }

    #[test]
    fn modify_job_with_multiple_changes_sorted_by_key() {
        let s = run(
            None,
            None,
            &[job_modify(
                "loader",
                vec![("script", "a.py", "b.py"), ("memory", "1g", "2g")],
            )],
        );
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  ~ Modify job [loader]\n      memory : 1g -> 2g\n      script : a.py -> b.py\n"
        );
    }

    #[test]
    fn delete_job() {
        let s = run(None, None, &[job_delete("stale")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  - Delete job [stale]\n"
        );
    }

    #[test]
    fn job_only_with_target_filter() {
        let s = run(Some("loader"), None, &[job_create("loader")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n(targeting: loader)\n\n  + Create job [loader]\n"
        );
    }

    #[test]
    fn dir_scope_shows_scoped_to() {
        let s = run(None, Some("staging/us-east-1/"), &[job_create("loader")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n(scoped to: staging/us-east-1/)\n\n  + Create job [loader]\n"
        );
    }

    #[test]
    fn dir_scope_none_unchanged() {
        let s = run(None, None, &[job_create("loader")]);
        assert_eq!(
            s,
            "--- Plan for myproj ---\n\n  + Create job [loader]\n"
        );
    }
}
