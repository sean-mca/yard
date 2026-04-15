//! Airflow DAG codegen: groups jobs by their nearest `dag.yaml` marker,
//! resolves the Airflow config inheritance chain, and renders Python DAG
//! files via Tera.
//!
//! Scope for PR 1b is pure codegen — no S3 upload, no state tracking. The
//! apply/plan wiring lands in PR 1c.

use anyhow::{Context as AnyhowContext, Result, anyhow};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use tera::{Context, Tera};
use yard_structs::{AirflowSection, JobDefinition, ProjectManifest};

use crate::{is_task_only, merge_airflow_sections, parse_airflow_section};

const AIRFLOW_DAG_TEMPLATE: &str = include_str!("templates/airflow_dag.py.tera");

/// A DAG resolved from filesystem discovery + validation. Ready for codegen.
#[derive(Debug, Clone)]
pub struct ResolvedDag {
    /// Sanitized, project-prefixed DAG id (e.g. `myproj_orders_pipeline`).
    pub name: String,
    /// Directory containing the `dag.yaml` marker file.
    pub dir: PathBuf,
    /// Fully merged DAG-level Airflow config (project → account → region →
    /// dag.yaml → optional single per-task DAG-level override).
    pub config: AirflowSection,
    /// Task ids in topological order. Tie-broken alphabetically for
    /// deterministic output across runs.
    pub tasks: Vec<String>,
    /// Per-task upstream dependencies, sorted. Keys cover every task in
    /// `tasks` (absent entries would be ambiguous).
    pub depends_on: BTreeMap<String, Vec<String>>,
}

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
pub fn collect_dags(root_dir: &Path, manifest: &ProjectManifest) -> Result<Vec<ResolvedDag>> {
    let mut dag_dirs = find_dag_marker_dirs(root_dir)?;
    dag_dirs.sort();

    // Nested dag.yaml is a hard error. Check all pairs — dag_dirs is small.
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

        validate_depends_on(&jobs, &task_ids, manifest, dag_dir)?;
        enforce_single_dag_level_override(&jobs, dag_dir)?;

        let dag_config = resolve_dag_airflow_config(manifest, dag_dir, &jobs)?;
        let (sorted_tasks, deps_map) = topo_sort(&jobs)?;

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

/// Render a resolved DAG into an Airflow Python file.
pub fn generate_dag(manifest: &ProjectManifest, dag: &ResolvedDag) -> Result<String> {
    let mut tera = Tera::default();
    tera.add_raw_template("airflow_dag", AIRFLOW_DAG_TEMPLATE)?;

    // Collect the job_type used by each task so we can pick operator classes.
    let mut task_types: Vec<(String, String, &JobDefinition)> = Vec::with_capacity(dag.tasks.len());
    for task_id in &dag.tasks {
        let job = manifest.jobs.get(task_id).ok_or_else(|| {
            anyhow!(
                "DAG '{}' references task '{}' that is not in the manifest",
                dag.name,
                task_id
            )
        })?;
        task_types.push((task_id.clone(), job.job_type.clone(), job));
    }

    // Imports block: one line per distinct operator needed.
    let mut needs_bash = false;
    let mut needs_glue = false;
    for (_, ty, _) in &task_types {
        match ty.as_str() {
            "bash" => needs_bash = true,
            "glue" => needs_glue = true,
            other => {
                return Err(anyhow!(
                    "DAG '{}' task '{}': job type '{}' is not supported in Airflow codegen yet",
                    dag.name,
                    task_types
                        .iter()
                        .find(|(_, t, _)| t == other)
                        .map(|(n, _, _)| n.as_str())
                        .unwrap_or("?"),
                    other
                ));
            }
        }
    }
    let mut import_lines = Vec::new();
    if needs_bash {
        import_lines.push("from airflow.operators.bash import BashOperator".to_string());
    }
    if needs_glue {
        import_lines.push(
            "from airflow.providers.amazon.aws.operators.glue import GlueJobOperator".to_string(),
        );
    }
    let imports_block = import_lines.join("\n");

    // default_args dict. Only include fields we actually have.
    let default_args = render_default_args(&dag.config);

    // schedule expression as a Python literal.
    let schedule = match &dag.config.schedule {
        Some(s) => python_string_literal(s),
        None => "None".to_string(),
    };

    // Task definitions, one per line, indented one level for inside `with DAG:`.
    let mut task_lines = Vec::new();
    for (task_id, job_type, job) in &task_types {
        task_lines.push(render_task(task_id, job_type, job)?);
    }
    let tasks_block = task_lines.join("\n");

    // Dependency wiring at module level (outside the `with` block), one edge
    // per line: `t_up >> t_down`. Simple and easy to read; richer grouping
    // can come in a later PR.
    let mut dep_lines = Vec::new();
    for task_id in &dag.tasks {
        if let Some(upstreams) = dag.depends_on.get(task_id) {
            for up in upstreams {
                dep_lines.push(format!(
                    "{} >> {}",
                    python_var_name(up),
                    python_var_name(task_id)
                ));
            }
        }
    }
    let deps_block = if dep_lines.is_empty() {
        "# No task dependencies".to_string()
    } else {
        dep_lines.join("\n")
    };

    let mut ctx = Context::new();
    ctx.insert("dag_name", &dag.name);
    ctx.insert("imports_block", &imports_block);
    ctx.insert("default_args", &default_args);
    ctx.insert("schedule", &schedule);
    ctx.insert("tasks_block", &tasks_block);
    ctx.insert("deps_block", &deps_block);

    tera.render("airflow_dag", &ctx)
        .context("Failed to render Airflow DAG template")
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

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

fn validate_depends_on(
    jobs: &[(String, &JobDefinition)],
    task_ids: &BTreeSet<String>,
    manifest: &ProjectManifest,
    dag_dir: &Path,
) -> Result<()> {
    for (name, job) in jobs {
        let Some(block) = &job.airflow else {
            continue;
        };
        for dep in &block.depends_on {
            if dep == name {
                return Err(anyhow!(
                    "task '{}' in DAG at '{}' depends on itself",
                    name,
                    dag_dir.display()
                ));
            }
            if !task_ids.contains(dep) {
                return if manifest.jobs.contains_key(dep) {
                    Err(anyhow!(
                        "task '{}' in DAG at '{}' has cross-DAG depends_on '{}' — \
                         cross-DAG dependencies are not supported",
                        name,
                        dag_dir.display(),
                        dep
                    ))
                } else {
                    Err(anyhow!(
                        "task '{}' in DAG at '{}' depends_on '{}', which is not a task in this DAG",
                        name,
                        dag_dir.display(),
                        dep
                    ))
                };
            }
        }
    }
    Ok(())
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

/// Kahn's algorithm with stable (alphabetical) tie-breaking for deterministic
/// output. Returns the sorted task list and a per-task upstream map.
type TopoResult = (Vec<String>, BTreeMap<String, Vec<String>>);

fn topo_sort(jobs: &[(String, &JobDefinition)]) -> Result<TopoResult> {
    let all_tasks: BTreeSet<String> = jobs.iter().map(|(n, _)| n.clone()).collect();

    let mut deps: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, job) in jobs {
        let mut d: Vec<String> = job
            .airflow
            .as_ref()
            .map(|a| a.depends_on.clone())
            .unwrap_or_default();
        d.sort();
        d.dedup();
        deps.insert(name.clone(), d);
    }

    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    for name in &all_tasks {
        in_degree.insert(name.clone(), deps.get(name).map_or(0, |d| d.len()));
    }

    let mut downstream: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (name, ds) in &deps {
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

    Ok((sorted, deps))
}

fn render_default_args(cfg: &AirflowSection) -> String {
    let mut entries: Vec<String> = Vec::new();
    if let Some(owner) = &cfg.owner {
        entries.push(format!("    \"owner\": {},", python_string_literal(owner)));
    }
    if let Some(retries) = cfg.retries {
        entries.push(format!("    \"retries\": {},", retries));
    }
    if entries.is_empty() {
        "{}".to_string()
    } else {
        format!("{{\n{}\n}}", entries.join("\n"))
    }
}

fn render_task(task_id: &str, job_type: &str, job: &JobDefinition) -> Result<String> {
    let var = python_var_name(task_id);
    let tid = python_string_literal(task_id);
    match job_type {
        "bash" => {
            let cmd = job
                .config
                .get("command")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("bash task '{task_id}' is missing 'command'"))?;
            Ok(format!(
                "    {var} = BashOperator(\n        task_id={tid},\n        bash_command={cmd_lit},\n    )",
                var = var,
                tid = tid,
                cmd_lit = python_string_literal(cmd),
            ))
        }
        "glue" => Ok(format!(
            "    {var} = GlueJobOperator(\n        task_id={tid},\n        job_name={jn},\n        aws_conn_id=\"aws_default\",\n    )",
            var = var,
            tid = tid,
            jn = python_string_literal(task_id),
        )),
        other if is_task_only(other) => Err(anyhow!(
            "task-only job type '{other}' is not yet supported in Airflow codegen"
        )),
        other => Err(anyhow!(
            "job type '{other}' is not supported in Airflow codegen yet"
        )),
    }
}

/// Sanitize a string into a Python identifier fragment: keep `[A-Za-z0-9_]`,
/// replace everything else with `_`, and prepend `_` if the first char is a
/// digit. Used for DAG names and task variable names.
fn sanitize_identifier(s: &str) -> String {
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

fn python_var_name(task_id: &str) -> String {
    format!("t_{}", sanitize_identifier(task_id))
}

/// JSON strings are a subset of valid Python string literals, so we piggy-back
/// on serde_json to produce a correctly-escaped double-quoted literal.
fn python_string_literal(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string())
}

/// Validate that no job has an `airflow:` block while living outside any DAG
/// directory. Such blocks are meaningless without a DAG context.
pub fn validate_orphan_airflow_blocks(
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

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::validate_python_syntax;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use yard_structs::{AirflowJobBlock, ProjectManifest, StateBackend};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// Minimal scoped temp dir that cleans up on drop. Avoids pulling in the
    /// `tempfile` crate as a dev-dependency.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("yard_airflow_dag_{}_{}", std::process::id(), n));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            TempDir { path }
        }
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    // ---- Fixtures ----

    fn empty_manifest(name: &str) -> ProjectManifest {
        ProjectManifest {
            project: name.to_string(),
            state: StateBackend::Local {
                path: PathBuf::from(".yard/state"),
            },
            providers: HashMap::new(),
            jobs: HashMap::new(),
            aws: serde_json::Value::Null,
        }
    }

    fn bash_job(command: &str, dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: "bash".to_string(),
            config: json!({"type": "bash", "command": command}),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn glue_job(dir: &Path) -> JobDefinition {
        JobDefinition {
            job_type: "glue".to_string(),
            config: json!({
                "type": "glue",
                "role": "arn:aws:iam::123456789:role/TestGlueRole",
            }),
            dir: dir.to_path_buf(),
            ..Default::default()
        }
    }

    fn write_yaml(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, body).unwrap();
    }

    /// Build the minimal on-disk layout required by `resolve::load_context`
    /// (account.yaml + region.yaml must exist somewhere in the ancestry).
    fn setup_project_tree() -> TempDir {
        let tmp = TempDir::new();
        let root = tmp.path();
        write_yaml(&root.join("yard.yaml"), "project: test\n");
        write_yaml(&root.join("account.yaml"), "account:\n  id: \"123\"\n");
        write_yaml(&root.join("region.yaml"), "region:\n  id: us-east-1\n");
        tmp
    }

    // ---- collect_dags: empty / single / multi ----

    #[test]
    fn empty_dag_dir_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let manifest = empty_manifest("test");
        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("no task files"), "got: {err}");
    }

    #[test]
    fn single_bash_task_dag_collects_and_renders() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("runit".to_string(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags.len(), 1);
        assert_eq!(dags[0].name, "test_pipeline");
        assert_eq!(dags[0].tasks, vec!["runit".to_string()]);
        assert_eq!(dags[0].config.schedule.as_deref(), Some("@daily"));

        let script = generate_dag(&manifest, &dags[0]).unwrap();
        assert!(script.contains("BashOperator"));
        assert!(script.contains("dag_id=\"test_pipeline\""));
        assert!(script.contains("bash_command=\"echo hi\""));
        assert!(
            validate_python_syntax(&script).is_none(),
            "generated DAG has a syntax error:\n{script}"
        );
    }

    #[test]
    fn linear_deps_topo_order() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        let mut c = bash_job("echo c", &dag_dir);
        a.airflow = None;
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            overrides: Default::default(),
        });
        c.airflow = Some(AirflowJobBlock {
            depends_on: vec!["b".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);
        manifest.jobs.insert("c".to_string(), c);

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].tasks, vec!["a", "b", "c"]);

        let script = generate_dag(&manifest, &dags[0]).unwrap();
        assert!(script.contains("t_a >> t_b"));
        assert!(script.contains("t_b >> t_c"));
        assert!(validate_python_syntax(&script).is_none());
    }

    #[test]
    fn fan_out_topo_order_is_deterministic() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@hourly\"\n");

        let mut manifest = empty_manifest("test");
        let a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        let mut c = bash_job("echo c", &dag_dir);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            overrides: Default::default(),
        });
        c.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);
        manifest.jobs.insert("c".to_string(), c);

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].tasks, vec!["a", "b", "c"]);
        let script = generate_dag(&manifest, &dags[0]).unwrap();
        assert!(script.contains("t_a >> t_b"));
        assert!(script.contains("t_a >> t_c"));
        assert!(validate_python_syntax(&script).is_none());
    }

    #[test]
    fn cycle_detection_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["b".to_string()],
            overrides: Default::default(),
        });
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn missing_depends_on_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["ghost".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("a".to_string(), a);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("ghost"), "got: {err}");
    }

    #[test]
    fn cross_dag_depends_on_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_one = root.join("one");
        let dag_two = root.join("two");
        write_yaml(&dag_one.join("dag.yaml"), "schedule: \"@daily\"\n");
        write_yaml(&dag_two.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("a".to_string(), bash_job("echo a", &dag_one));
        let mut b = bash_job("echo b", &dag_two);
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("b".to_string(), b);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("cross-DAG"), "got: {err}");
    }

    #[test]
    fn nested_dag_yaml_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let outer = root.join("outer");
        let inner = outer.join("inner");
        write_yaml(&outer.join("dag.yaml"), "schedule: \"@daily\"\n");
        write_yaml(&inner.join("dag.yaml"), "schedule: \"@daily\"\n");

        let manifest = empty_manifest("test");
        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("nested dag.yaml"), "got: {err}");
    }

    #[test]
    fn inheritance_region_overrides_project_default() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        // Overwrite region.yaml with airflow key for override.
        write_yaml(
            &root.join("region.yaml"),
            "region:\n  id: us-east-1\nairflow:\n  schedule: \"@hourly\"\n",
        );
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "owner: data-team\n");

        let mut manifest = empty_manifest("test");
        manifest.providers.insert(
            "airflow".to_string(),
            json!({"schedule": "@daily", "retries": 1}),
        );
        manifest
            .jobs
            .insert("runit".to_string(), bash_job("echo hi", &dag_dir));

        let dags = collect_dags(root, &manifest).unwrap();
        assert_eq!(dags[0].config.schedule.as_deref(), Some("@hourly"));
        assert_eq!(dags[0].config.owner.as_deref(), Some("data-team"));
        assert_eq!(dags[0].config.retries, Some(1));
    }

    #[test]
    fn two_tasks_with_dag_level_overrides_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        let mut b = bash_job("echo b", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec![],
            overrides: AirflowSection {
                schedule: Some("@hourly".to_string()),
                ..Default::default()
            },
        });
        b.airflow = Some(AirflowJobBlock {
            depends_on: vec![],
            overrides: AirflowSection {
                retries: Some(3),
                ..Default::default()
            },
        });
        manifest.jobs.insert("a".to_string(), a);
        manifest.jobs.insert("b".to_string(), b);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("DAG-level overrides"), "got: {err}");
    }

    #[test]
    fn mixed_glue_and_bash_renders() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("orders".to_string(), glue_job(&dag_dir));
        let mut notify = bash_job("echo done", &dag_dir);
        notify.airflow = Some(AirflowJobBlock {
            depends_on: vec!["orders".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("notify".to_string(), notify);

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0]).unwrap();
        assert!(script.contains("GlueJobOperator"));
        assert!(script.contains("BashOperator"));
        assert!(script.contains("t_orders >> t_notify"));
        assert!(validate_python_syntax(&script).is_none());
    }

    #[test]
    fn dag_name_sanitization_handles_dashes_and_leading_digit() {
        // Directly exercise the helper — we'd need a filesystem dir name
        // containing these characters otherwise.
        assert_eq!(sanitize_identifier("pipe-line"), "pipe_line");
        assert_eq!(sanitize_identifier("9am_etl"), "_9am_etl");
        assert_eq!(sanitize_identifier("order.flow"), "order_flow");
        assert_eq!(sanitize_identifier(""), "_");
    }

    #[test]
    fn self_dependency_errors() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        let mut a = bash_job("echo a", &dag_dir);
        a.airflow = Some(AirflowJobBlock {
            depends_on: vec!["a".to_string()],
            overrides: Default::default(),
        });
        manifest.jobs.insert("a".to_string(), a);

        let err = collect_dags(root, &manifest).unwrap_err().to_string();
        assert!(err.contains("depends on itself"), "got: {err}");
    }

    #[test]
    fn bash_command_with_special_chars_escapes_correctly() {
        let tmp = setup_project_tree();
        let root = tmp.path();
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut manifest = empty_manifest("test");
        manifest.jobs.insert(
            "runit".to_string(),
            bash_job("echo \"hello\\nworld\"", &dag_dir),
        );

        let dags = collect_dags(root, &manifest).unwrap();
        let script = generate_dag(&manifest, &dags[0]).unwrap();
        assert!(validate_python_syntax(&script).is_none(), "{script}");
    }

    // ---- orphan airflow block validation ----

    #[test]
    fn orphan_airflow_block_detected() {
        let tmp = setup_project_tree();
        let root = tmp.path();

        // dag_dir with a dag.yaml
        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        // task_a is inside the DAG dir — fine
        let mut task_a = bash_job("echo a", &dag_dir);
        task_a.airflow = Some(AirflowJobBlock::default());

        // orphan_job has an airflow block but lives outside any DAG dir
        let orphan_dir = root.join("standalone");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        let mut orphan = bash_job("echo orphan", &orphan_dir);
        orphan.airflow = Some(AirflowJobBlock::default());

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("task_a".to_string(), task_a);
        manifest
            .jobs
            .insert("orphan_job".to_string(), orphan);

        let dags = collect_dags(root, &manifest).unwrap();
        let errors = validate_orphan_airflow_blocks(&manifest, &dags);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, "orphan_job");
    }

    #[test]
    fn no_orphan_when_all_in_dag() {
        let tmp = setup_project_tree();
        let root = tmp.path();

        let dag_dir = root.join("pipeline");
        write_yaml(&dag_dir.join("dag.yaml"), "schedule: \"@daily\"\n");

        let mut task_a = bash_job("echo a", &dag_dir);
        task_a.airflow = Some(AirflowJobBlock::default());

        let mut manifest = empty_manifest("test");
        manifest
            .jobs
            .insert("task_a".to_string(), task_a);

        let dags = collect_dags(root, &manifest).unwrap();
        let errors = validate_orphan_airflow_blocks(&manifest, &dags);
        assert!(errors.is_empty());
    }
}
