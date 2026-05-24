use anyhow::{Context as AnyhowContext, Result, anyhow};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Write;
use tera::{Context, Tera};
use yard_structs::{AirflowSection, JobDefinition, JobType, ProjectManifest};

use super::AIRFLOW_DAG_TEMPLATE;
use super::ResolvedDag;
use super::connections::{derive_aws_conn_id, required_connections_for_dag, resolve_task_aws_conn_id};
use super::helpers::{python_string_literal, python_var_name};
use super::triggers::{self, TriggerRender};

/// Render a resolved DAG into an Airflow Python file.
pub fn generate_dag(
    manifest: &ProjectManifest,
    dag: &ResolvedDag,
    script_locations: &HashMap<String, String>,
) -> Result<String> {
    let mut tera = Tera::default();
    tera.add_raw_template("airflow_dag", AIRFLOW_DAG_TEMPLATE)?;

    // Collect the job_type used by each task so we can pick operator classes.
    let mut task_types: Vec<(String, JobType, &JobDefinition)> = Vec::with_capacity(dag.tasks.len());
    for task_id in &dag.tasks {
        let job = manifest.jobs.get(task_id).ok_or_else(|| {
            anyhow!(
                "DAG '{}' references task '{}' that is not in the manifest",
                dag.name,
                task_id
            )
        })?;
        task_types.push((task_id.clone(), job.job_type, job));
    }

    // Imports block: one line per distinct operator needed.
    let mut needs_bash = false;
    let mut needs_glue = false;
    for (task_id, ty, _) in &task_types {
        match ty {
            JobType::Bash => needs_bash = true,
            JobType::Glue => needs_glue = true,
            JobType::Emr | _ => {
                return Err(anyhow!(
                    "DAG '{}' task '{}': job type '{}' is not supported in Airflow codegen yet",
                    dag.name,
                    task_id,
                    ty
                ));
            }
        }
    }
    // Per-task `publishes:` still needs the Dataset import; trigger-derived
    // dataset imports flow in via `trender.extra_imports` (Phase 30 plan 30-01
    // moved DAG-level dataset-trigger import emission into render_trigger).
    let has_datasets_for_publishes = task_types
        .iter()
        .any(|(_, _, j)| j.airflow.as_ref().is_some_and(|a| !a.publishes.is_empty()));

    let mut import_lines = Vec::new();
    if needs_bash {
        import_lines.push("from airflow.operators.bash import BashOperator".to_string());
    }
    if needs_glue {
        import_lines.push(
            "from airflow.providers.amazon.aws.operators.glue import GlueJobOperator".to_string(),
        );
    }
    if has_datasets_for_publishes {
        import_lines.push("from airflow.datasets import Dataset".to_string());
    }

    // default_args dict. Only include fields we actually have.
    let default_args = render_default_args(&dag.config);

    // DAG-level default aws_conn_id passed into `render_trigger` as a
    // primitive `&str`. Precedence (highest first):
    //   1. Cascaded `airflow.aws.aws_conn_id` on this DAG (yard.yaml →
    //      account → region → dag.yaml chain via `merge_airflow_sections`).
    //   2. Project-root `aws.aws_conn_id` (yard.yaml top-level `aws:` block).
    //   3. `derive_aws_conn_id(project-root assume_role)` — pre-cascade
    //      behavior; same-account/no-role still yields `None` here and
    //      sensors fall through to `aws_default` at runtime.
    let default_aws_conn_id: Option<String> = if let Some(explicit) = dag
        .config
        .aws
        .as_ref()
        .and_then(|a| a.aws_conn_id.as_deref())
        .filter(|s| !s.is_empty())
    {
        Some(explicit.to_string())
    } else if let Some(explicit) = manifest
        .aws
        .as_ref()
        .and_then(|a| a.aws_conn_id.as_deref())
        .filter(|s| !s.is_empty())
    {
        Some(explicit.to_string())
    } else {
        manifest
            .aws
            .as_ref()
            .and_then(|a| a.assume_role.as_deref())
            .filter(|s| !s.is_empty())
            .map(derive_aws_conn_id)
            .transpose()?
    };

    // D-05 (Phase 30 plan 30-01): roots = user task IDs with no upstream
    // edges. Sensor branches (plans 30-02/03/04) connect
    // `_yard_wait_<source> >> root` for every root. Computed here in
    // generation.rs and threaded as primitive `&[String]` into
    // render_trigger so triggers.rs stays free of `BTreeMap` walking.
    let roots: Vec<String> = dag
        .tasks
        .iter()
        .filter(|t| {
            dag.depends_on
                .get(*t)
                .map(|v| v.is_empty())
                .unwrap_or(true)
        })
        .cloned()
        .collect();

    // D-03: ALL schedule-expression resolution lives inside render_trigger now.
    // Phase 29 mutual-exclusion validation guarantees only one of `trigger`
    // or `schedule` is non-None at this point.
    let trender: TriggerRender = triggers::render_trigger(
        dag.config.trigger.as_ref(),
        dag.config.schedule.as_deref(),
        default_aws_conn_id.as_deref(),
        &roots,
    );
    let schedule = trender.schedule_expr.clone();

    // Merge trigger-derived imports (e.g. `from airflow.datasets import Dataset`,
    // sensor providers in plans 30-02/03) with the per-task imports computed
    // above. BTreeSet de-dups (so a DAG that BOTH trigger-on-Dataset AND
    // per-task-publishes-Dataset emits the import line once) and gives
    // deterministic ordering across runs.
    //
    // Phase 32 (PUB-01): the PUB-01 branch below mutates this set to add the
    // EmptyOperator import only when `dag.config.publishes` is non-empty —
    // free de-dup against any heterogeneous-all `_yard_join` import that may
    // already be present from `trender.extra_imports`. The `imports_block`
    // flush is deferred to after that branch.
    let mut combined_imports: BTreeSet<String> = import_lines
        .into_iter()
        .chain(trender.extra_imports.iter().cloned())
        .collect();

    // D-15 / D-16: max_active_runs_block is empty for schedule-only DAGs
    // (PRES-02 byte-identical guarantee — no new kwarg leaks into existing
    // fixtures), and `    max_active_runs={N},\n` when set.
    //
    // CONC-01 + user-override-wins (plan 30-04): `AirflowSection.max_active_runs`
    // (user spec) always wins over `trender.max_active_runs` (CONC-01 auto-default
    // of 1 for any trigger DAG). When BOTH are None — schedule-only DAGs without
    // an explicit value — emit no kwarg line at all so Airflow's implicit
    // default of 16 applies by absence.
    let effective_max_active = dag.config.max_active_runs.or(trender.max_active_runs);
    let max_active_runs_block = match effective_max_active {
        Some(n) => format!("    max_active_runs={n},\n"),
        None => String::new(),
    };

    // Task definitions, one per line, indented one level for inside `with DAG:`.
    let mut task_lines = Vec::new();
    for (task_id, job_type, job) in &task_types {
        task_lines.push(render_task(
            task_id,
            *job_type,
            job,
            manifest,
            script_locations,
        )?);
    }
    // D-05 / D-07: sensor task lines prepend user task lines so the rendered
    // DAG reads top-down: imports -> sensors -> user tasks. Empty for plan
    // 30-01 (Datasets branch emits no sensors); plans 30-02/03/04 fill these.
    // PRES-02 byte-identical for schedule-only DAGs: trender.sensor_tasks is
    // an empty Vec, so the join below is a no-op vs. pre-Phase-30 behavior.
    //
    // Phase 32 (PUB-01): the publish branch below appends `_yard_publish` to
    // this Vec when `dag.config.publishes` is non-empty, so `tasks_block` is
    // computed AFTER the branch.
    let mut all_task_lines: Vec<String> = trender.sensor_tasks.clone();
    all_task_lines.extend(task_lines);

    // Cross-account connection docstring. Empty for single-account DAGs so we
    // don't clutter the header.
    let required = required_connections_for_dag(manifest, dag)?;
    let required_connections_block = if required.is_empty() {
        String::new()
    } else {
        let mut out =
            String::from("# Required Airflow connections (create in MWAA before running):\n");
        for rc in &required {
            // write! to String never fails; the Ok arm is the only reachable path.
            let _ = writeln!(&mut out, "#   - {}  ->  {}", rc.conn_id, rc.role_arn);
        }
        out
    };

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
    // D-05 / D-07: sensor edges (`_yard_wait_<source> >> root` / sensor edges
    // to `_yard_join` / `_yard_join >> root`) precede user-task edges so the
    // rendered deps section reads top-down. Empty for plan 30-01 (Datasets
    // branch emits no sensor edges); plans 30-02/03/04 populate these.
    //
    // Phase 32 (PUB-01): the publish branch below appends a fan-in deps line
    // (`[leaf_a, leaf_b] >> _yard_publish`) when `dag.config.publishes` is
    // non-empty, so `deps_block` is computed AFTER the branch.
    let mut all_dep_lines: Vec<String> = trender.sensor_deps.clone();
    all_dep_lines.extend(dep_lines);

    // PUB-01: synthesize `_yard_publish` EmptyOperator + leaf fan-in when
    // `AirflowSection.publishes` is non-empty. Schedule-only AND trigger-only
    // DAGs without publishes skip this entire branch (D-05, PRES-02 byte-id
    // guard). URIs alpha-sorted in outlets per D-04. Symmetric to the root
    // detection at lines 113-123, but a leaf is a task that does NOT appear
    // in any OTHER task's `depends_on` Vec.
    if !dag.config.publishes.is_empty() {
        // D-04: alpha-sort URIs, independent of declaration order.
        let mut sorted_uris: Vec<&str> =
            dag.config.publishes.iter().map(String::as_str).collect();
        sorted_uris.sort();
        let outlets = sorted_uris
            .iter()
            .map(|u| format!("Dataset({})", python_string_literal(u)))
            .collect::<Vec<_>>()
            .join(", ");

        // Synthetic task body — mirrors Phase 30 `_yard_join` shape
        // (triggers.rs:454-461). `trigger_rule` defaults to `all_success`,
        // so we omit the kwarg — Dataset producer fires only after every
        // upstream user task succeeds.
        let yard_publish_task = format!(
            "    _yard_publish = EmptyOperator(\n        task_id=\"_yard_publish\",\n        outlets=[{outlets}],\n    )"
        );
        all_task_lines.push(yard_publish_task);

        // Leaf detection (Pitfall 1: lives ONLY inside this branch — never
        // computed for DAGs without publishes). Symmetric to root detection.
        let leaves: Vec<String> = dag
            .tasks
            .iter()
            .filter(|t| {
                !dag.depends_on
                    .iter()
                    .any(|(other, ups)| other.as_str() != t.as_str() && ups.contains(t))
            })
            .cloned()
            .collect();
        let leaf_list = leaves
            .iter()
            .map(|l| python_var_name(l))
            .collect::<Vec<_>>()
            .join(", ");
        all_dep_lines.push(format!("[{leaf_list}] >> _yard_publish"));

        // Pitfall 2: BTreeSet de-dups against any heterogeneous-all
        // `_yard_join` import already present in `combined_imports`. Also
        // ensure `Dataset` is imported — it may be absent if no per-task
        // `airflow.publishes` (PUB-02) is set anywhere AND no Dataset trigger
        // is configured. Idempotent re-insert is safe.
        combined_imports.insert("from airflow.operators.empty import EmptyOperator".to_string());
        combined_imports.insert("from airflow.datasets import Dataset".to_string());
    }

    // Final flush — `tasks_block`, `deps_block`, and `imports_block` are all
    // computed AFTER the PUB-01 branch so its mutations are observed.
    let tasks_block = all_task_lines.join("\n");
    let deps_block = if all_dep_lines.is_empty() {
        "# No task dependencies".to_string()
    } else {
        all_dep_lines.join("\n")
    };
    let imports_block = combined_imports.into_iter().collect::<Vec<_>>().join("\n");

    let mut ctx = Context::new();
    ctx.insert("dag_name", &dag.name);
    ctx.insert("imports_block", &imports_block);
    ctx.insert("default_args", &default_args);
    ctx.insert("schedule", &schedule);
    ctx.insert("max_active_runs_block", &max_active_runs_block);
    ctx.insert("tasks_block", &tasks_block);
    ctx.insert("deps_block", &deps_block);
    ctx.insert("required_connections_block", &required_connections_block);
    // API-01 (plan 30-04): API trigger contributes a header docstring with
    // curl/CLI snippets. Empty string for non-API triggers — concatenates
    // cleanly with required_connections_block (also empty-or-newline-terminated).
    ctx.insert("trigger_header_block", &trender.header_docstring);

    tera.render("airflow_dag", &ctx)
        .context("Failed to render Airflow DAG template")
}

/// Render the `default_args` dict for the Airflow DAG constructor.
///
/// Only includes fields that are explicitly set (owner, retries). Returns
/// `"{}"` when no default args are configured.
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

/// Render a single task assignment inside the `with DAG(...)` block.
///
/// Dispatches to operator-specific rendering by job type (Bash, Glue).
///
/// # Errors
///
/// Returns an error if the job type is unsupported, required config fields
/// are missing, or the script location cannot be resolved.
fn render_task(
    task_id: &str,
    job_type: JobType,
    job: &JobDefinition,
    manifest: &ProjectManifest,
    script_locations: &HashMap<String, String>,
) -> Result<String> {
    let var = python_var_name(task_id);
    let tid = python_string_literal(task_id);
    let outlets = render_outlets(job);
    match job_type {
        JobType::Bash => {
            let cmd = job
                .config
                .get("command")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("bash task '{task_id}' is missing 'command'"))?;
            Ok(format!(
                "    {var} = BashOperator(\n        task_id={tid},\n        bash_command={cmd_lit},{outlets}\n    )",
                var = var,
                tid = tid,
                cmd_lit = python_string_literal(cmd),
                outlets = outlets,
            ))
        }
        JobType::Glue => {
            let role = job
                .config
                .get("role")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    anyhow!("Glue render requires 'config.role' on the resolved job config")
                })
                .with_context(|| format!("task '{task_id}'"))?;

            let script_uri = script_locations
                .get(task_id)
                .ok_or_else(|| {
                    anyhow!(
                        "Glue render requires a persisted script URI — \
                     run 'yard apply' to upload and persist state"
                    )
                })
                .with_context(|| format!("task '{task_id}'"))?;

            let conn_id = resolve_task_aws_conn_id(job, manifest)
                .with_context(|| format!("task '{task_id}'"))?;

            Ok(format!(
                "    {var} = GlueJobOperator(\n        task_id={tid},\n        job_name={jn},\n        script_location={sl},\n        iam_role_arn={ir},\n        aws_conn_id={cn},{outlets}\n    )",
                var = var,
                tid = tid,
                jn = python_string_literal(task_id),
                sl = python_string_literal(script_uri),
                ir = python_string_literal(role),
                cn = python_string_literal(&conn_id),
                outlets = outlets,
            ))
        }
        JobType::Emr | _ => Err(anyhow!(
            "job type '{job_type}' is not supported in Airflow codegen yet"
        )),
    }
}

/// Render the per-task `outlets=[Dataset(...)]` kwarg fragment for PUB-02.
///
/// Returns an empty string when the job has no `airflow.publishes` entries,
/// so callers can unconditionally interpolate the result.
#[inline]
fn render_outlets(job: &JobDefinition) -> String {
    job.airflow
        .as_ref()
        .map(|a| &a.publishes)
        .filter(|p| !p.is_empty())
        .map(|uris| {
            let items = uris
                .iter()
                .map(|u| format!("Dataset({})", python_string_literal(u)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("\n        outlets=[{items}],")
        })
        .unwrap_or_default()
}
