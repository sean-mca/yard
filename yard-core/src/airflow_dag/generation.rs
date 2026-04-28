use anyhow::{Context as AnyhowContext, Result, anyhow};
use std::collections::HashMap;
use tera::{Context, Tera};
use yard_structs::{AirflowSection, JobDefinition, JobType, ProjectManifest};

use super::AIRFLOW_DAG_TEMPLATE;
use super::ResolvedDag;
use super::connections::{required_connections_for_dag, resolve_task_aws_conn_id};
use super::helpers::{python_string_literal, python_var_name};

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
            JobType::Emr => {
                return Err(anyhow!(
                    "DAG '{}' task '{}': job type '{}' is not supported in Airflow codegen yet",
                    dag.name,
                    task_id,
                    ty
                ));
            }
        }
    }
    // Determine if any task produces datasets, or if the DAG is dataset-triggered.
    let has_datasets = dag
        .config
        .trigger
        .as_ref()
        .is_some_and(|t| !t.dataset_uris().is_empty())
        || task_types
            .iter()
            .any(|(_, _, j)| j.airflow.as_ref().is_some_and(|a| !a.publishes.is_empty()));

    // `trigger.dataset_uris()` takes precedence over an inherited `schedule` --
    // a dataset-triggered DAG doesn't use a cron schedule even if one was
    // inherited from the project or account level. Non-dataset triggers
    // (s3/sqs/api) currently render schedule=None here; Phase 30 will adapt
    // those paths to emit sensor tasks instead.

    let mut import_lines = Vec::new();
    if needs_bash {
        import_lines.push("from airflow.operators.bash import BashOperator".to_string());
    }
    if needs_glue {
        import_lines.push(
            "from airflow.providers.amazon.aws.operators.glue import GlueJobOperator".to_string(),
        );
    }
    if has_datasets {
        import_lines.push("from airflow.datasets import Dataset".to_string());
    }
    let imports_block = import_lines.join("\n");

    // default_args dict. Only include fields we actually have.
    let default_args = render_default_args(&dag.config);

    // schedule: dataset-triggered DAGs get `[Dataset(...), ...]`;
    // cron DAGs get a quoted string; unscheduled DAGs get None.
    // Non-dataset triggers (s3/sqs/api) currently render schedule=None here —
    // Phase 30 owns their codegen (sensor task emission).
    let dataset_uris: Vec<&str> = dag
        .config
        .trigger
        .as_ref()
        .map(|t| t.dataset_uris())
        .unwrap_or_default();
    let schedule = if !dataset_uris.is_empty() {
        let datasets = dataset_uris
            .iter()
            .map(|uri| format!("Dataset({})", python_string_literal(uri)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{datasets}]")
    } else {
        match &dag.config.schedule {
            Some(s) => python_string_literal(s),
            None => "None".to_string(),
        }
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
    let tasks_block = task_lines.join("\n");

    // Cross-account connection docstring. Empty for single-account DAGs so we
    // don't clutter the header.
    let required = required_connections_for_dag(manifest, dag)?;
    let required_connections_block = if required.is_empty() {
        String::new()
    } else {
        let mut out =
            String::from("# Required Airflow connections (create in MWAA before running):\n");
        for rc in &required {
            out.push_str(&format!("#   - {}  ->  {}\n", rc.conn_id, rc.role_arn));
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
    ctx.insert("required_connections_block", &required_connections_block);

    tera.render("airflow_dag", &ctx)
        .context("Failed to render Airflow DAG template")
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
        JobType::Emr => Err(anyhow!(
            "job type '{job_type}' is not supported in Airflow codegen yet"
        )),
    }
}

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
