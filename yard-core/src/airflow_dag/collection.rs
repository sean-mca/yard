use anyhow::{Context as AnyhowContext, Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use yard_structs::{AirflowSection, JobDefinition, ProjectManifest};

use super::helpers::sanitize_identifier;
use super::resolve::{resolve_all_depends_on, topo_sort};
use super::ResolvedDag;

use crate::{merge_airflow_sections, parse_airflow_section};

/// Walk `root_dir`, find every `dag.yaml` marker, group jobs into DAGs, and
/// return fully resolved DAGs in deterministic order (by directory path).
///
/// Errors on:
/// - Nested `dag.yaml` files (one DAG dir inside another)
/// - Empty DAG directories (marker present, no task files)
/// - `depends_on` references to missing or cross-DAG tasks
/// - DAG-level override fields (schedule/retries/etc.) declared on more than
///   one task in the same DAG
/// - Dependency cycles
pub(super) fn collect_dags(root_dir: &Path, manifest: &ProjectManifest) -> Result<Vec<ResolvedDag>> {
    let mut dag_dirs = find_dag_marker_dirs(root_dir)?;
    dag_dirs.sort();

    // Nested dag.yaml is a hard error. Check all pairs -- dag_dirs is small.
    for (i, outer) in dag_dirs.iter().enumerate() {
        for inner in &dag_dirs[i + 1..] {
            if is_strict_ancestor(outer, inner) {
                return Err(anyhow!(
                    "nested dag.yaml: '{}' is inside another DAG at '{}'",
                    inner.display(),
                    outer.display()
                ));
            }
            if is_strict_ancestor(inner, outer) {
                return Err(anyhow!(
                    "nested dag.yaml: '{}' is inside another DAG at '{}'",
                    outer.display(),
                    inner.display()
                ));
            }
        }
    }

    // Assign each job to its nearest ancestor DAG dir (if any). A job with no
    // ancestor dag.yaml is not in any DAG and is ignored here.
    let dag_dir_set: BTreeSet<PathBuf> = dag_dirs.iter().cloned().collect();
    let mut dag_to_jobs: BTreeMap<PathBuf, Vec<(String, &JobDefinition)>> = BTreeMap::new();
    for d in &dag_dirs {
        dag_to_jobs.insert(d.clone(), Vec::new());
    }

    for (name, job) in &manifest.jobs {
        if let Some(dir) = nearest_ancestor_in(&job.dir, &dag_dir_set) {
            dag_to_jobs
                .get_mut(&dir)
                .expect("dag_to_jobs missing key")
                .push((name.clone(), job));
        }
    }

    let mut resolved = Vec::with_capacity(dag_dirs.len());
    let project_prefix = sanitize_identifier(&manifest.project);

    for dag_dir in &dag_dirs {
        let mut jobs = dag_to_jobs.remove(dag_dir).unwrap_or_default();
        if jobs.is_empty() {
            return Err(anyhow!(
                "dag.yaml at '{}' has no task files",
                dag_dir.display()
            ));
        }
        // Deterministic task order for everything downstream.
        jobs.sort_by(|a, b| a.0.cmp(&b.0));

        let task_ids: BTreeSet<String> = jobs.iter().map(|(n, _)| n.clone()).collect();

        let resolved_deps = resolve_all_depends_on(&jobs, &task_ids, manifest, dag_dir)?;
        enforce_single_dag_level_override(&jobs, dag_dir)?;

        let dag_config = resolve_dag_airflow_config(manifest, dag_dir, &jobs)?;
        let (sorted_tasks, deps_map) = topo_sort(&jobs, &resolved_deps)?;

        let dir_name = dag_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("dag");
        let dag_name = format!("{}_{}", project_prefix, sanitize_identifier(dir_name));

        resolved.push(ResolvedDag {
            name: dag_name,
            dir: dag_dir.clone(),
            config: dag_config,
            tasks: sorted_tasks,
            depends_on: deps_map,
        });
    }

    Ok(resolved)
}

fn find_dag_marker_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_name() == "dag.yaml" && entry.file_type().is_file() {
            let parent = entry
                .path()
                .parent()
                .ok_or_else(|| anyhow!("dag.yaml has no parent: {}", entry.path().display()))?
                .to_path_buf();
            dirs.push(parent);
        }
    }
    Ok(dirs)
}

fn is_strict_ancestor(ancestor: &Path, descendant: &Path) -> bool {
    descendant != ancestor && descendant.starts_with(ancestor)
}

fn nearest_ancestor_in(start: &Path, set: &BTreeSet<PathBuf>) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        if set.contains(&current) {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn section_has_dag_level_fields(s: &AirflowSection) -> bool {
    s.schedule.is_some()
        || s.owner.is_some()
        || s.retries.is_some()
        || s.dags_bucket.is_some()
        || s.dags_prefix.is_some()
}

fn enforce_single_dag_level_override(
    jobs: &[(String, &JobDefinition)],
    dag_dir: &Path,
) -> Result<()> {
    let mut seen: Option<&str> = None;
    for (name, job) in jobs {
        let Some(block) = &job.airflow else {
            continue;
        };
        if section_has_dag_level_fields(&block.overrides) {
            if let Some(prev) = seen {
                return Err(anyhow!(
                    "DAG at '{}' has DAG-level overrides declared on multiple tasks: '{}' and '{}' — \
                     at most one task may declare schedule/retries/owner/dags_bucket/dags_prefix",
                    dag_dir.display(),
                    prev,
                    name
                ));
            }
            seen = Some(name.as_str());
        }
    }
    Ok(())
}

fn resolve_dag_airflow_config(
    manifest: &ProjectManifest,
    dag_dir: &Path,
    jobs: &[(String, &JobDefinition)],
) -> Result<AirflowSection> {
    let project_level = manifest
        .providers
        .get("airflow")
        .map(parse_airflow_section)
        .unwrap_or_default();

    let ctx = crate::resolve::load_context(dag_dir)
        .with_context(|| format!("Failed to load context for DAG at {}", dag_dir.display()))?;

    let account_level = ctx
        .account
        .get("airflow")
        .map(parse_airflow_section)
        .unwrap_or_default();
    let region_level = ctx
        .region
        .get("airflow")
        .map(parse_airflow_section)
        .unwrap_or_default();
    // dag.yaml fields sit at the top level of the file (it IS the airflow section).
    let dag_level = parse_airflow_section(&ctx.dag);

    let mut merged = merge_airflow_sections(&project_level, &account_level);
    merged = merge_airflow_sections(&merged, &region_level);
    merged = merge_airflow_sections(&merged, &dag_level);

    for (_, job) in jobs {
        if let Some(block) = &job.airflow {
            merged = merge_airflow_sections(&merged, &block.overrides);
        }
    }

    Ok(merged)
}
