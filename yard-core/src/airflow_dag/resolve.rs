use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use yard_structs::{JobDefinition, ProjectManifest};

/// Kahn's algorithm with stable (alphabetical) tie-breaking for deterministic
/// output. Returns the sorted task list and a per-task upstream map.
pub(super) type TopoResult = (Vec<String>, BTreeMap<String, Vec<String>>);

/// Build a lookup from short name (base_name) to full job name for all tasks
/// in this DAG. Returns entries only where base_name differs from the full name.
fn build_short_name_index(jobs: &[(String, &JobDefinition)]) -> HashMap<String, Vec<String>> {
    let mut index: HashMap<String, Vec<String>> = HashMap::new();
    for (full_name, job) in jobs {
        if !job.base_name.is_empty() && job.base_name != *full_name {
            index
                .entry(job.base_name.clone())
                .or_default()
                .push(full_name.clone());
        }
    }
    index
}

/// Resolve a single depends_on reference to a full task name.
fn resolve_dep(
    dep: &str,
    task_name: &str,
    task_ids: &BTreeSet<String>,
    short_index: &HashMap<String, Vec<String>>,
    manifest: &ProjectManifest,
    dag_dir: &Path,
) -> Result<String> {
    if dep == task_name {
        return Err(anyhow!(
            "task '{}' in DAG at '{}' depends on itself",
            task_name,
            dag_dir.display()
        ));
    }
    if task_ids.contains(dep) {
        return Ok(dep.to_string());
    }
    if let Some(matches) = short_index.get(dep) {
        let filtered: Vec<&String> = matches
            .iter()
            .filter(|m| task_ids.contains(m.as_str()))
            .collect();
        return match filtered.len() {
            1 => {
                let resolved = filtered[0];
                if resolved == task_name {
                    return Err(anyhow!(
                        "task '{}' in DAG at '{}' depends on itself (via short name '{}')",
                        task_name,
                        dag_dir.display(),
                        dep
                    ));
                }
                Ok(resolved.clone())
            }
            0 => Err(anyhow!(
                "task '{}' in DAG at '{}' depends_on '{}', which is not a task in this DAG",
                task_name,
                dag_dir.display(),
                dep
            )),
            _ => Err(anyhow!(
                "task '{}' in DAG at '{}' depends_on '{}' which is ambiguous — matches: {}. \
                 Use the full name to disambiguate.",
                task_name,
                dag_dir.display(),
                dep,
                filtered
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        };
    }
    if manifest.jobs.contains_key(dep) {
        Err(anyhow!(
            "task '{}' in DAG at '{}' has cross-DAG depends_on '{}' — \
             cross-DAG dependencies are not supported",
            task_name,
            dag_dir.display(),
            dep
        ))
    } else {
        Err(anyhow!(
            "task '{}' in DAG at '{}' depends_on '{}', which is not a task in this DAG",
            task_name,
            dag_dir.display(),
            dep
        ))
    }
}

/// Resolve and validate all depends_on references, returning a map of
/// full_name -> resolved upstream full names.
pub(super) fn resolve_all_depends_on(
    jobs: &[(String, &JobDefinition)],
    task_ids: &BTreeSet<String>,
    manifest: &ProjectManifest,
    dag_dir: &Path,
) -> Result<BTreeMap<String, Vec<String>>> {
    let short_index = build_short_name_index(jobs);
    let mut resolved: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, job) in jobs {
        let Some(block) = &job.airflow else {
            resolved.insert(name.clone(), Vec::new());
            continue;
        };
        let mut deps = Vec::new();
        for dep in &block.depends_on {
            deps.push(resolve_dep(
                dep,
                name,
                task_ids,
                &short_index,
                manifest,
                dag_dir,
            )?);
        }
        deps.sort();
        deps.dedup();
        resolved.insert(name.clone(), deps);
    }
    Ok(resolved)
}

pub(super) fn topo_sort(
    jobs: &[(String, &JobDefinition)],
    deps: &BTreeMap<String, Vec<String>>,
) -> Result<TopoResult> {
    let all_tasks: BTreeSet<String> = jobs.iter().map(|(n, _)| n.clone()).collect();

    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    for name in &all_tasks {
        in_degree.insert(name.clone(), deps.get(name).map_or(0, |d| d.len()));
    }

    let mut downstream: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, ds) in deps.iter() {
        for d in ds {
            downstream.entry(d.clone()).or_default().push(name.clone());
        }
    }
    for v in downstream.values_mut() {
        v.sort();
    }

    // Ready set kept sorted so pop_front is always the alphabetically smallest.
    let mut ready: Vec<String> = in_degree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| n.clone())
        .collect();
    ready.sort();

    let mut sorted = Vec::with_capacity(all_tasks.len());
    while !ready.is_empty() {
        let n = ready.remove(0);
        sorted.push(n.clone());
        if let Some(children) = downstream.get(&n) {
            for child in children {
                if let Some(deg) = in_degree.get_mut(child) {
                    *deg -= 1;
                    if *deg == 0 {
                        ready.push(child.clone());
                    }
                }
            }
        }
        ready.sort();
    }

    if sorted.len() != all_tasks.len() {
        let mut remaining: Vec<String> = all_tasks
            .iter()
            .filter(|n| !sorted.contains(n))
            .cloned()
            .collect();
        remaining.sort();
        return Err(anyhow!(
            "cycle detected in DAG dependencies involving: {}",
            remaining.join(", ")
        ));
    }

    Ok((sorted, deps.clone()))
}
