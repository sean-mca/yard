use anyhow::Result;
use std::collections::BTreeSet;
use yard_structs::ProjectManifest;

use super::ResolvedDag;

/// Sanitize a string into a Python identifier fragment: keep `[A-Za-z0-9_]`,
/// replace everything else with `_`, and prepend `_` if the first char is a
/// digit. Used for DAG names and task variable names.
pub(super) fn sanitize_identifier(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, c) in s.chars().enumerate() {
        let ok = c.is_ascii_alphanumeric() || c == '_';
        if i == 0 && c.is_ascii_digit() {
            out.push('_');
        }
        out.push(if ok { c } else { '_' });
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

pub(super) fn python_var_name(task_id: &str) -> String {
    format!("t_{}", sanitize_identifier(task_id))
}

/// JSON strings are a subset of valid Python string literals, so we piggy-back
/// on serde_json to produce a correctly-escaped double-quoted literal.
pub(super) fn python_string_literal(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// Validate that no job has an `airflow:` block while living outside any DAG
/// directory. Such blocks are meaningless without a DAG context.
pub(super) fn validate_orphan_airflow_blocks(
    manifest: &ProjectManifest,
    dags: &[ResolvedDag],
) -> Vec<(String, String)> {
    // Collect all job names that participate in at least one DAG
    let dag_tasks: BTreeSet<&str> = dags
        .iter()
        .flat_map(|d| d.tasks.iter().map(|s| s.as_str()))
        .collect();

    let mut errors = Vec::new();
    for (job_name, job_def) in &manifest.jobs {
        if job_def.airflow.is_some() && !dag_tasks.contains(job_name.as_str()) {
            errors.push((
                job_name.clone(),
                format!(
                    "Job \"{job_name}\" has an airflow: block but is not inside a DAG directory \
                     (no ancestor dag.yaml found). Remove the airflow: block or add a dag.yaml."
                ),
            ));
        }
    }
    errors
}
