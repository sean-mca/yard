//! Per-source codegen for `airflow.trigger:` DAG configurations.
//!
//! Single public function [`render_trigger`] owns ALL schedule-expression
//! resolution (D-03) — top-level `schedule:`, dataset-derived `schedule=`,
//! and `schedule=None` for sensor-driven DAGs all flow through here.
//! Phase 30.1 ships the Datasets branch + skeleton; 30.2 adds S3, 30.3 adds
//! SQS, 30.4 adds API + heterogeneous-all + max_active_runs default.
//!
//! Determinism (D-07, D-11): sensor render order is alphabetical by source
//! kind; Dataset URIs inside `&` / `|` chains are alphabetically sorted.
//!
//! No I/O, no AWS-config types — this module takes the typed `Trigger`
//! plus primitive `&str` plumbing (`default_aws_conn_id`, `roots`) and
//! returns owned strings via [`TriggerRender`]. Mirror of
//! `connections.rs`: small, pure, deterministic, in-tree tests (D-04).

use yard_structs::{SingleSource, Trigger};

use super::helpers::{python_string_literal, python_var_name};

/// Result of rendering a [`Trigger`] to Python codegen fragments.
///
/// Empty fields = "no contribution" (e.g., schedule-only DAGs return
/// `schedule_expr` in quoted form and empty `sensor_tasks`).
///
/// Phase 31 (PAY-02) will add a non-breaking `op_kwargs:
/// Vec<(String, String)>` field for sensor → user-task XCom plumbing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TriggerRender {
    /// Python expression for the `schedule=` kwarg on `with DAG(...)`:
    /// `"None"`, `"\"@daily\""`, `"[Dataset(\"x\")]"`,
    /// `"(Dataset(\"a\") & Dataset(\"b\"))"`.
    pub schedule_expr: String,
    /// Python task assignments, one per entry. Source-kind alphabetical
    /// (D-07). Empty for Datasets-only / schedule-only DAGs.
    pub sensor_tasks: Vec<String>,
    /// Edge lines: `_yard_wait_s3 >> t_root` etc. Empty when no sensors.
    pub sensor_deps: Vec<String>,
    /// Provider import lines, e.g. `from airflow.datasets import Dataset`.
    pub extra_imports: Vec<String>,
    /// CONC-01 default: Some(1) when `trigger.is_some()` and user did not
    /// override. None when no trigger or schedule-only. Plan 30-04 wires the
    /// auto-default; plan 30-01 stakes the field shape only.
    pub max_active_runs: Option<u32>,
    /// API-01: optional `# ...` comment block emitted near the top of the
    /// rendered DAG file, documenting how to invoke the DAG via Airflow's
    /// REST API or CLI. Empty string when the trigger is not API-driven.
    pub header_docstring: String,
}

/// Render a resolved trigger into Python sensor tasks, schedule expression,
/// and required imports. Single entry point — owns ALL schedule-expression
/// resolution including the top-level `schedule:` literal-string render
/// (D-03). One pass; everything computed together so a sensor and its
/// import never get out of sync (D-02).
///
/// Phase 29 mutual-exclusion validation guarantees only one of `trigger`
/// or `schedule` is non-None at runtime. `default_aws_conn_id` is the
/// DAG-level `derive_aws_conn_id(assume_role)` value, used by S3 / SQS
/// sensor branches (plan 30-02 / 30-03) and ignored by the Datasets branch.
/// `roots` are user task IDs with no upstream — sensor edges connect
/// `_yard_wait_<source> >> root` (plan 30-02 / 30-03 / 30-04).
///
/// In plan 30-01 the S3/SQS/API arms fall through to `schedule=None` —
/// they get real implementations in subsequent plans. The skeleton + the
/// Datasets branch ship here so `generation.rs` has something to call.
pub(super) fn render_trigger(
    trigger: Option<&Trigger>,
    schedule: Option<&str>,
    default_aws_conn_id: Option<&str>,
    roots: &[String],
) -> TriggerRender {
    // D-12: collapse single-element composites to bare-single before branching.
    // The Trigger::Serialize impl already collapses for hashing (HASH-01); we
    // mirror here for render-side determinism so that a programmatically-built
    // `Trigger::All(vec![x])` (e.g. via merge code paths) still produces the
    // same Python output as `Trigger::Single(x)`.
    let normalized: Option<&SingleSource> = match trigger {
        Some(Trigger::Single(s)) => Some(s),
        Some(Trigger::All(v)) | Some(Trigger::Any(v)) if v.len() == 1 => v.first(),
        _ => None,
    };

    // Branch 1: bare-single (or collapsed single-element composite).
    let mut result = if let Some(single) = normalized {
        render_single(single, default_aws_conn_id, roots)
    } else if let Some(t) = trigger {
        // Branch 2: composite (>= 2 elements).
        render_composite(t, default_aws_conn_id, roots)
    } else {
        // Branch 3: no trigger — render top-level schedule literal or None.
        TriggerRender {
            schedule_expr: match schedule {
                Some(s) => python_string_literal(s),
                None => "None".to_string(),
            },
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: Vec::new(),
            max_active_runs: None,
            header_docstring: String::new(),
        }
    };

    // CONC-01: any DAG with a `trigger:` block defaults to max_active_runs=1
    // when neither a per-arm value nor a user override has set it. Schedule-only
    // DAGs (`trigger.is_none()`) preserve Airflow's implicit default of 16 — no
    // `max_active_runs=` line emitted (PRES-02 byte-identical guarantee).
    // User overrides via `AirflowSection.max_active_runs` always win — applied
    // later in generation.rs by overlaying onto `result.max_active_runs`.
    if trigger.is_some() && result.max_active_runs.is_none() {
        result.max_active_runs = Some(1);
    }
    result
}

/// Bare-single render branch.
///
/// `default_aws_conn_id` and `roots` are consumed by sensor branches
/// (S3 in plan 30-02; SQS in plan 30-03; API in plan 30-04). The Schedule
/// and Dataset branches ignore them.
///
/// S3 (plan 30-02) — emits `_yard_wait_s3 = S3KeySensor(...)` with knob
/// defaults (poke_interval=60, timeout=86400, deferrable=True), the
/// per-trigger > DAG-level > None aws_conn_id precedence (S3-04, D-08),
/// and the legacy `deferrable=False` escape hatch (S3-03).
///
/// SQS (plan 30-03) — emits `_yard_wait_sqs = SqsSensor(...)` with knob
/// defaults (wait_time_seconds=20 long-poll, max_messages=5,
/// delete_message_on_reception=True, deferrable=True). SqsTrigger has no
/// per-trigger aws_conn_id field (Phase 28 omission); plumbing flows from
/// `default_aws_conn_id` directly with the same omit-when-None contract.
fn render_single(
    s: &SingleSource,
    default_aws_conn_id: Option<&str>,
    roots: &[String],
) -> TriggerRender {
    match s {
        SingleSource::Schedule(sched) => TriggerRender {
            schedule_expr: python_string_literal(&sched.value),
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: Vec::new(),
            max_active_runs: None,
            header_docstring: String::new(),
        },
        SingleSource::Dataset(d) => TriggerRender {
            schedule_expr: format!("[Dataset({})]", python_string_literal(&d.uri)),
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: vec!["from airflow.datasets import Dataset".to_string()],
            max_active_runs: None,
            header_docstring: String::new(),
        },
        SingleSource::S3(s3) => {
            // Knob defaults (S3-02): poke_interval=60, timeout=86400.
            // Deferrable default (S3-03): true. User override
            // `deferrable: false` emits the legacy non-deferrable form for
            // old `apache-airflow-providers-amazon < 8.0.0` deployments.
            let poke = s3.poke_interval.unwrap_or(60);
            let timeout = s3.timeout.unwrap_or(86400);
            let deferrable = s3.deferrable.unwrap_or(true);
            // S3-04 precedence (D-08): per-trigger override beats DAG-level
            // default; absence of both falls back to Airflow's `aws_default`
            // by omitting the kwarg entirely.
            let conn = s3.aws_conn_id.as_deref().or(default_aws_conn_id);

            // bucket_key = exact `key` if set, else `prefix`. The typed model
            // does not enforce exactly-one-of (S3Trigger has both fields as
            // Option<String>); a future plan can add that in validate_dag_full
            // alongside the other knob rules. Here we trust the parser and
            // pick `key` first when both are present (consistent with
            // documented precedence).
            let bucket_key_value = s3
                .key
                .as_deref()
                .or(s3.prefix.as_deref())
                .unwrap_or("");

            // Build the sensor task lines. Indented 4 spaces (inside the
            // `with DAG(...) as dag:` block).
            let mut sensor = String::new();
            sensor.push_str("    _yard_wait_s3 = S3KeySensor(\n");
            sensor.push_str("        task_id=\"_yard_wait_s3\",\n");
            sensor.push_str(&format!(
                "        bucket_name={},\n",
                python_string_literal(&s3.bucket)
            ));
            sensor.push_str(&format!(
                "        bucket_key={},\n",
                python_string_literal(bucket_key_value)
            ));
            sensor.push_str(&format!("        poke_interval={poke},\n"));
            sensor.push_str(&format!("        timeout={timeout},\n"));
            sensor.push_str(&format!(
                "        deferrable={},\n",
                if deferrable { "True" } else { "False" }
            ));
            if let Some(c) = conn {
                sensor.push_str(&format!(
                    "        aws_conn_id={},\n",
                    python_string_literal(c)
                ));
            }
            sensor.push_str("    )");

            // One edge per root in input order — generation.rs computes
            // `roots` deterministically (DAG task list filter + cloned).
            let deps: Vec<String> = roots
                .iter()
                .map(|r| format!("_yard_wait_s3 >> {}", python_var_name(r)))
                .collect();

            TriggerRender {
                schedule_expr: "None".to_string(),
                sensor_tasks: vec![sensor],
                sensor_deps: deps,
                extra_imports: vec![
                    "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor".to_string(),
                ],
                max_active_runs: None,
                header_docstring: String::new(),
            }
        }
        SingleSource::Sqs(sqs) => {
            // Knob defaults (SQS-02): wait_time_seconds=20 (long-poll, saves
            // SQS API costs vs. the SDK's 0-second default), max_messages=5,
            // delete_message_on_reception=True. deferrable=True is locked
            // unconditionally — Phase 28 didn't add a `deferrable` field to
            // SqsTrigger, and the locked CONTEXT decision is to render as
            // deferrable always. A future SqsTrigger.deferrable field would be
            // a non-breaking addition that mirrors the S3 escape hatch.
            let wait = sqs.wait_time_seconds.unwrap_or(20);
            let max_msgs = sqs.max_messages.unwrap_or(5);
            let del_on_recv = sqs.delete_message_on_reception.unwrap_or(true);

            // SqsTrigger has NO per-trigger aws_conn_id field (Phase 28
            // omitted it). Use DAG-level default directly. None means
            // Airflow's `aws_default` applies by absence — same PRES-02
            // pattern as the S3 arm when both override and default are unset.
            let conn = default_aws_conn_id;

            // Build the sensor task lines. 4-space indent = inside the
            // `with DAG(...) as dag:` block. Kwarg order matches the S3 arm
            // for diff-churn minimization: task_id, queue, knobs, conn.
            let mut sensor = String::new();
            sensor.push_str("    _yard_wait_sqs = SqsSensor(\n");
            sensor.push_str("        task_id=\"_yard_wait_sqs\",\n");
            sensor.push_str(&format!(
                "        sqs_queue={},\n",
                python_string_literal(&sqs.queue_url)
            ));
            sensor.push_str(&format!("        wait_time_seconds={wait},\n"));
            sensor.push_str(&format!("        max_messages={max_msgs},\n"));
            sensor.push_str(&format!(
                "        delete_message_on_reception={},\n",
                if del_on_recv { "True" } else { "False" }
            ));
            sensor.push_str("        deferrable=True,\n");
            if let Some(c) = conn {
                sensor.push_str(&format!(
                    "        aws_conn_id={},\n",
                    python_string_literal(c)
                ));
            }
            sensor.push_str("    )");

            // One edge per root in input order — generation.rs computes
            // `roots` deterministically (DAG task list filter + cloned).
            let deps: Vec<String> = roots
                .iter()
                .map(|r| format!("_yard_wait_sqs >> {}", python_var_name(r)))
                .collect();

            TriggerRender {
                schedule_expr: "None".to_string(),
                sensor_tasks: vec![sensor],
                sensor_deps: deps,
                extra_imports: vec![
                    "from airflow.providers.amazon.aws.sensors.sqs import SqsSensor".to_string(),
                ],
                max_active_runs: None,
                header_docstring: String::new(),
            }
        }
        SingleSource::Api(api) => {
            // API-01..API-03 (plan 30-04). API triggers have no Airflow sensor
            // — they fire on manual REST/CLI invocation. The render contribution
            // is a header docstring documenting curl/CLI snippets with placeholder
            // env vars (no hardcoded URLs — CLAUDE.md "Never hardcode personal
            // info" precedent applies to AIRFLOW URLs too) and an auth-management
            // callout (yard does NOT manage Airflow REST auth). payload_schema
            // is doc-only in v1.6 — Airflow's typed Params landed in 3.x.
            let mut header = String::new();
            header.push_str("# Trigger: API (manual / external invocation)\n");
            if let Some(desc) = &api.description {
                header.push_str(&format!("# {desc}\n"));
            }
            header.push_str("#\n");
            header.push_str("# This DAG is triggered manually via Airflow's REST API or CLI.\n");
            header.push_str("# yard does NOT manage Airflow REST auth — configure JWT, Basic,\n");
            header.push_str("# or IAM SigV4 credentials in your Airflow deployment.\n");
            header.push_str("#\n");
            header.push_str("# Invoke via REST:\n");
            header.push_str("#   curl -X POST \"$AIRFLOW_URL/api/v1/dags/<dag_id>/dagRuns\" \\\n");
            header.push_str("#        -u \"$AIRFLOW_USER:$AIRFLOW_PASS\" \\\n");
            header.push_str("#        -H \"Content-Type: application/json\" \\\n");
            header.push_str("#        -d '{\"conf\": {\"key\": \"value\"}}'\n");
            header.push_str("#\n");
            header.push_str("# Invoke via CLI:\n");
            header.push_str(
                "#   airflow dags trigger <dag_id> --conf '{\"key\": \"value\"}'\n",
            );
            if let Some(schema) = &api.payload_schema {
                header.push_str("#\n");
                header.push_str(
                    "# Expected payload fields (doc-only — no runtime enforcement in v1.6):\n",
                );
                // BTreeMap iteration is sorted by key already, locking deterministic
                // header ordering across runs.
                for (field, ty) in schema {
                    header.push_str(&format!("#   {field}: {ty}\n"));
                }
            }
            TriggerRender {
                schedule_expr: "None".to_string(),
                sensor_tasks: Vec::new(),
                sensor_deps: Vec::new(),
                extra_imports: Vec::new(),
                // CONC-01 auto-default applied centrally in render_trigger.
                max_active_runs: None,
                header_docstring: header,
            }
        }
    }
}

/// Composite render branch.
///
/// - DS-02 / DS-03: homogeneous Datasets all/any (alpha-sorted `&` / `|` chain
///   at `schedule=` level).
/// - DS-04: heterogeneous-all — non-Dataset items render as sensor tasks under
///   `_yard_join` `EmptyOperator(trigger_rule="all_success")`.
/// - D-11: mixed Dataset + non-Dataset all — Datasets at `schedule=` level
///   AND non-Datasets as sensor tasks; `_yard_join` only when there are
///   2+ sensor tasks (D-10).
/// - D-07: sensor render order alphabetical by source kind (api < dataset <
///   s3 < sqs). API has no sensor task — its `header_docstring` flows through
///   the `combined_header` aggregator.
///
/// Phase 29 TRIG-06 already rejects heterogeneous-`any:` at validation, so
/// the only composite shape that reaches the heterogeneous branch here is
/// `Trigger::All(_)` with at least one non-Dataset item.
fn render_composite(
    t: &Trigger,
    default_aws_conn_id: Option<&str>,
    roots: &[String],
) -> TriggerRender {
    let (items, separator) = match t {
        Trigger::All(v) => (v, " & "),
        Trigger::Any(v) => (v, " | "),
        Trigger::Single(_) => unreachable!("normalized away in render_trigger"),
    };

    // DS-02 / DS-03: homogeneous Datasets — alpha-sort URIs and join.
    let all_datasets = items.iter().all(|s| matches!(s, SingleSource::Dataset(_)));
    if all_datasets {
        let mut uris: Vec<&str> = items
            .iter()
            .filter_map(|s| match s {
                SingleSource::Dataset(d) => Some(d.uri.as_str()),
                _ => None,
            })
            .collect();
        uris.sort();
        let chain = uris
            .iter()
            .map(|u| format!("Dataset({})", python_string_literal(u)))
            .collect::<Vec<_>>()
            .join(separator);
        return TriggerRender {
            schedule_expr: format!("({chain})"),
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: vec!["from airflow.datasets import Dataset".to_string()],
            max_active_runs: None,
            header_docstring: String::new(),
        };
    }

    // DS-04 + D-11: heterogeneous-all (Phase 29 TRIG-06 rejects heterogeneous-any
    // at validation, so the only composite shape reaching here is Trigger::All
    // with at least one non-Dataset item).
    debug_assert!(
        matches!(t, Trigger::All(_)),
        "heterogeneous Trigger::Any is rejected by Phase 29 validation; render_composite \
         should only see Trigger::All for heterogeneous mixes"
    );

    // Split Datasets (-> schedule level) from non-Datasets (-> sensor tasks).
    let mut dataset_uris: Vec<&str> = Vec::new();
    let mut non_datasets: Vec<&SingleSource> = Vec::new();
    for s in items {
        match s {
            SingleSource::Dataset(d) => dataset_uris.push(d.uri.as_str()),
            _ => non_datasets.push(s),
        }
    }
    dataset_uris.sort();

    // schedule= level: alpha-sorted `&` chain when Datasets present, else None.
    let schedule_expr = if dataset_uris.is_empty() {
        "None".to_string()
    } else if dataset_uris.len() == 1 {
        format!("[Dataset({})]", python_string_literal(dataset_uris[0]))
    } else {
        let chain = dataset_uris
            .iter()
            .map(|u| format!("Dataset({})", python_string_literal(u)))
            .collect::<Vec<_>>()
            .join(" & ");
        format!("({chain})")
    };

    // D-07: sensor render order alphabetical by source kind.
    let mut non_dataset_sorted: Vec<&SingleSource> = non_datasets.clone();
    non_dataset_sorted.sort_by_key(|s| s.source_kind());

    // Recurse into render_single for each non-Dataset source. Pass empty roots
    // so the per-source arms don't emit edges to user roots — we'll emit
    // sensor->_yard_join + _yard_join->root edges explicitly below (or
    // sensor->root if there's only one sensor).
    let empty_roots: &[String] = &[];
    let mut all_sensor_tasks: Vec<String> = Vec::new();
    let mut combined_imports: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    if !dataset_uris.is_empty() {
        combined_imports.insert("from airflow.datasets import Dataset".to_string());
    }
    let mut combined_header = String::new();
    for s in &non_dataset_sorted {
        let r = render_single(s, default_aws_conn_id, empty_roots);
        all_sensor_tasks.extend(r.sensor_tasks);
        combined_imports.extend(r.extra_imports);
        // API contributes header docstring; collect verbatim.
        if !r.header_docstring.is_empty() {
            combined_header.push_str(&r.header_docstring);
        }
    }

    // D-10: only emit _yard_join when there are 2+ sensor tasks.
    let mut sensor_deps: Vec<String> = Vec::new();
    if all_sensor_tasks.len() >= 2 {
        // Append _yard_join AFTER all sensors. 4-space indent for the
        // assignment + closing paren (inside the `with DAG:` block); 8-space
        // indent for the kwargs to match the S3 / SQS sensor formatting.
        let mut join_task = String::new();
        join_task.push_str("    _yard_join = EmptyOperator(\n");
        join_task.push_str("        task_id=\"_yard_join\",\n");
        join_task.push_str("        trigger_rule=\"all_success\",\n");
        join_task.push_str("    )");
        all_sensor_tasks.push(join_task);
        combined_imports.insert("from airflow.operators.empty import EmptyOperator".to_string());

        // Sensor -> _yard_join edges (alpha-sorted by emission order).
        for s in &non_dataset_sorted {
            let sensor_id = match s {
                SingleSource::S3(_) => Some("_yard_wait_s3"),
                SingleSource::Sqs(_) => Some("_yard_wait_sqs"),
                // API contributes no sensor task; skip for edge emission.
                SingleSource::Api(_) => None,
                // Dataset / Schedule cannot appear in non_datasets — Dataset
                // was filtered out into dataset_uris above; Schedule is
                // rejected at validation in heterogeneous composites.
                SingleSource::Dataset(_) | SingleSource::Schedule(_) => None,
            };
            if let Some(id) = sensor_id {
                sensor_deps.push(format!("{id} >> _yard_join"));
            }
        }
        // _yard_join -> root edges, in user-declared root order.
        for r in roots {
            sensor_deps.push(format!("_yard_join >> {}", python_var_name(r)));
        }
    } else if all_sensor_tasks.len() == 1 {
        // D-10: single-sensor mixed Dataset+sensor — connect the lone sensor
        // directly to roots, no _yard_join. Find which sensor it is.
        let sensor_id = non_dataset_sorted
            .iter()
            .find_map(|s| match s {
                SingleSource::S3(_) => Some("_yard_wait_s3"),
                SingleSource::Sqs(_) => Some("_yard_wait_sqs"),
                _ => None,
            })
            .unwrap_or("_yard_wait_unknown");
        for r in roots {
            sensor_deps.push(format!("{sensor_id} >> {}", python_var_name(r)));
        }
    }
    // If all_sensor_tasks.is_empty() (e.g. all-Datasets+API): no sensor_deps;
    // Datasets fire at schedule level, API has no sensor.

    TriggerRender {
        schedule_expr,
        sensor_tasks: all_sensor_tasks,
        sensor_deps,
        extra_imports: combined_imports.into_iter().collect(),
        // CONC-01 auto-default applied centrally in render_trigger.
        max_active_runs: None,
        header_docstring: combined_header,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use yard_structs::{ApiTrigger, DatasetTrigger, S3Trigger, ScheduleTrigger, SqsTrigger};

    fn ds(uri: &str) -> SingleSource {
        SingleSource::Dataset(DatasetTrigger {
            uri: uri.to_string(),
        })
    }

    /// Default-knob bare S3 trigger fixture for the render_trigger_s3_* tests.
    fn s3(bucket: &str, prefix: Option<&str>) -> SingleSource {
        SingleSource::S3(S3Trigger {
            bucket: bucket.to_string(),
            prefix: prefix.map(|s| s.to_string()),
            ..Default::default()
        })
    }

    /// Default-knob bare SQS trigger fixture for the render_trigger_sqs_* tests.
    /// SqsTrigger does not derive Default (no `..Default::default()`); enumerate
    /// every Option field explicitly.
    fn sqs(queue_url: &str) -> SingleSource {
        SingleSource::Sqs(SqsTrigger {
            queue_url: queue_url.to_string(),
            wait_time_seconds: None,
            max_messages: None,
            delete_message_on_reception: None,
        })
    }

    /// Bare API trigger fixture for the render_trigger_api_* tests.
    /// ApiTrigger derives Default, so callers can override only the fields
    /// they care about.
    fn api(description: Option<&str>) -> SingleSource {
        SingleSource::Api(ApiTrigger {
            description: description.map(|s| s.to_string()),
            payload_schema: None,
        })
    }

    #[test]
    fn render_trigger_none_with_no_schedule_emits_none() {
        let out = render_trigger(None, None, None, &[]);
        assert_eq!(out.schedule_expr, "None");
        assert!(out.sensor_tasks.is_empty());
        assert!(out.sensor_deps.is_empty());
        assert!(out.extra_imports.is_empty());
        assert_eq!(out.max_active_runs, None);
    }

    #[test]
    fn render_trigger_none_with_schedule_emits_quoted_string() {
        let out = render_trigger(None, Some("@daily"), None, &[]);
        assert_eq!(out.schedule_expr, "\"@daily\"");
        assert!(out.sensor_tasks.is_empty());
        assert!(out.extra_imports.is_empty());
        assert_eq!(out.max_active_runs, None);
    }

    #[test]
    fn render_trigger_schedule_via_trigger_block_renders_quoted_string() {
        let t = Trigger::Single(SingleSource::Schedule(ScheduleTrigger {
            value: "@hourly".into(),
        }));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.schedule_expr, "\"@hourly\"");
    }

    #[test]
    fn render_trigger_single_dataset_emits_schedule_list() {
        let t = Trigger::Single(ds("s3://x"));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.schedule_expr, "[Dataset(\"s3://x\")]");
        assert_eq!(
            out.extra_imports,
            vec!["from airflow.datasets import Dataset".to_string()]
        );
        assert!(out.sensor_tasks.is_empty());
    }

    #[test]
    fn render_trigger_homogeneous_all_datasets_emits_amp_chain_alpha_sorted() {
        // Pass URIs out-of-order; D-11 alpha-sort must produce the same chain.
        let t = Trigger::All(vec![ds("s3://z"), ds("s3://a")]);
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(
            out.schedule_expr,
            "(Dataset(\"s3://a\") & Dataset(\"s3://z\"))"
        );
        assert_eq!(
            out.extra_imports,
            vec!["from airflow.datasets import Dataset".to_string()]
        );
    }

    #[test]
    fn render_trigger_homogeneous_any_datasets_emits_pipe_chain_alpha_sorted() {
        let t = Trigger::Any(vec![ds("s3://z"), ds("s3://a")]);
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(
            out.schedule_expr,
            "(Dataset(\"s3://a\") | Dataset(\"s3://z\"))"
        );
    }

    #[test]
    fn render_trigger_single_element_all_collapses_to_bare_single() {
        // D-12: render-side normalization mirrors Trigger::Serialize collapse.
        let t_collapsed = Trigger::All(vec![ds("s3://x")]);
        let t_bare = Trigger::Single(ds("s3://x"));
        let a = render_trigger(Some(&t_collapsed), None, None, &[]);
        let b = render_trigger(Some(&t_bare), None, None, &[]);
        assert_eq!(a, b, "single-element all collapses to bare-single (D-12)");
    }

    #[test]
    fn render_trigger_single_element_any_collapses_to_bare_single() {
        let t_collapsed = Trigger::Any(vec![ds("s3://x")]);
        let t_bare = Trigger::Single(ds("s3://x"));
        let a = render_trigger(Some(&t_collapsed), None, None, &[]);
        let b = render_trigger(Some(&t_bare), None, None, &[]);
        assert_eq!(a, b, "single-element any collapses to bare-single (D-12)");
    }

    #[test]
    fn render_trigger_dataset_import_only_emitted_once() {
        let t = Trigger::Single(ds("s3://x"));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.extra_imports.len(), 1);
    }

    #[test]
    fn render_trigger_max_active_runs_field_is_some_one_for_dataset_trigger() {
        // CONC-01 (plan 30-04 flipped this): any DAG with a `trigger:` block
        // defaults to max_active_runs=Some(1). Plan 30-01 staked the field
        // shape with None; plan 30-04 wires the auto-default centrally in
        // render_trigger. Schedule-only DAGs still preserve None (PRES-02).
        let t = Trigger::Single(ds("s3://x"));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.max_active_runs, Some(1));
    }

    #[test]
    fn render_trigger_homogeneous_dataset_two_elements_imports_dataset_once() {
        let t = Trigger::All(vec![ds("s3://a"), ds("s3://b")]);
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.extra_imports.len(), 1);
        assert_eq!(out.extra_imports[0], "from airflow.datasets import Dataset");
    }

    // --- Phase 30 plan 30-02: S3 single-source render branch (S3-01..S3-04) ---

    #[test]
    fn render_trigger_s3_single_account_emits_deferrable_sensor() {
        // S3-01: single-account default — knob defaults (poke_interval=60,
        // timeout=86400, deferrable=True), no aws_conn_id (Airflow's
        // aws_default applies by absence), one >> edge per root.
        let t = Trigger::Single(s3("mybucket", Some("input/")));
        let out = render_trigger(Some(&t), None, None, &["root_task".to_string()]);
        assert_eq!(out.schedule_expr, "None");
        assert_eq!(out.sensor_tasks.len(), 1);
        let st = &out.sensor_tasks[0];
        assert!(st.contains("_yard_wait_s3 = S3KeySensor("), "got: {st}");
        assert!(st.contains("task_id=\"_yard_wait_s3\""), "got: {st}");
        assert!(st.contains("bucket_name=\"mybucket\""), "got: {st}");
        assert!(st.contains("bucket_key=\"input/\""), "got: {st}");
        assert!(st.contains("poke_interval=60"), "got: {st}");
        assert!(st.contains("timeout=86400"), "got: {st}");
        assert!(st.contains("deferrable=True"), "got: {st}");
        assert!(
            !st.contains("aws_conn_id="),
            "no override + no default = no conn line: {st}"
        );
        assert_eq!(
            out.sensor_deps,
            vec!["_yard_wait_s3 >> t_root_task".to_string()]
        );
        assert_eq!(
            out.extra_imports,
            vec![
                "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor".to_string()
            ]
        );
        // CONC-01 (plan 30-04): any trigger DAG defaults to max_active_runs=1.
        assert_eq!(out.max_active_runs, Some(1));
    }

    #[test]
    fn render_trigger_s3_with_exact_key_uses_bucket_key() {
        // bucket_key takes the exact `key` path when set (vs. prefix glob).
        let t = Trigger::Single(SingleSource::S3(S3Trigger {
            bucket: "b".into(),
            key: Some("path/to/file.csv".into()),
            prefix: None,
            ..Default::default()
        }));
        let out = render_trigger(Some(&t), None, None, &["r".to_string()]);
        let st = &out.sensor_tasks[0];
        assert!(st.contains("bucket_key=\"path/to/file.csv\""), "got: {st}");
    }

    #[test]
    fn render_trigger_s3_user_override_knobs_propagate() {
        // S3-02: user values override the render defaults verbatim.
        let t = Trigger::Single(SingleSource::S3(S3Trigger {
            bucket: "b".into(),
            prefix: Some("p/".into()),
            poke_interval: Some(120),
            timeout: Some(3600),
            ..Default::default()
        }));
        let out = render_trigger(Some(&t), None, None, &["r".to_string()]);
        let st = &out.sensor_tasks[0];
        assert!(st.contains("poke_interval=120"), "got: {st}");
        assert!(st.contains("timeout=3600"), "got: {st}");
    }

    #[test]
    fn render_trigger_s3_deferrable_false_renders_legacy_form() {
        // S3-03: legacy escape hatch for old apache-airflow-providers-amazon.
        // Python's `False` (capital F).
        let t = Trigger::Single(SingleSource::S3(S3Trigger {
            bucket: "b".into(),
            prefix: Some("p/".into()),
            deferrable: Some(false),
            ..Default::default()
        }));
        let out = render_trigger(Some(&t), None, None, &["r".to_string()]);
        let st = &out.sensor_tasks[0];
        assert!(st.contains("deferrable=False"), "got: {st}");
        assert!(!st.contains("deferrable=True"), "got: {st}");
    }

    #[test]
    fn render_trigger_s3_aws_conn_id_user_override_wins() {
        // S3-04 / D-08: per-trigger aws_conn_id beats DAG-level default.
        let t = Trigger::Single(SingleSource::S3(S3Trigger {
            bucket: "b".into(),
            prefix: Some("p/".into()),
            aws_conn_id: Some("custom_conn".into()),
            ..Default::default()
        }));
        let out = render_trigger(
            Some(&t),
            None,
            Some("dag_default_conn"),
            &["r".to_string()],
        );
        let st = &out.sensor_tasks[0];
        assert!(st.contains("aws_conn_id=\"custom_conn\""), "got: {st}");
        assert!(!st.contains("dag_default_conn"), "got: {st}");
    }

    #[test]
    fn render_trigger_s3_aws_conn_id_dag_default_used_when_no_override() {
        // S3-04: absence of per-trigger override falls through to DAG-level
        // derive_aws_conn_id value.
        let t = Trigger::Single(s3("b", Some("p/")));
        let out = render_trigger(
            Some(&t),
            None,
            Some("dag_default_conn"),
            &["r".to_string()],
        );
        let st = &out.sensor_tasks[0];
        assert!(st.contains("aws_conn_id=\"dag_default_conn\""), "got: {st}");
    }

    #[test]
    fn render_trigger_s3_no_aws_conn_id_when_both_none() {
        // S3-04: neither override nor DAG default = omit kwarg entirely.
        // Airflow's `aws_default` applies by absence.
        let t = Trigger::Single(s3("b", Some("p/")));
        let out = render_trigger(Some(&t), None, None, &["r".to_string()]);
        let st = &out.sensor_tasks[0];
        assert!(!st.contains("aws_conn_id="), "got: {st}");
    }

    #[test]
    fn render_trigger_s3_multiple_roots_emit_multiple_dep_edges() {
        // S3-01: one >> edge per root, in input order. generation.rs computes
        // roots from the DAG task list deterministically; we mirror that
        // order here without re-sorting.
        let t = Trigger::Single(s3("b", Some("p/")));
        let out = render_trigger(
            Some(&t),
            None,
            None,
            &["a".to_string(), "b".to_string()],
        );
        assert_eq!(
            out.sensor_deps,
            vec![
                "_yard_wait_s3 >> t_a".to_string(),
                "_yard_wait_s3 >> t_b".to_string(),
            ]
        );
    }

    // --- Phase 30 plan 30-03: SQS single-source render branch (SQS-01, SQS-02) ---

    #[test]
    fn render_trigger_sqs_default_knobs_emits_long_poll_sensor() {
        // SQS-01 + SQS-02 single-account default — knob defaults
        // (wait_time_seconds=20 long-poll, max_messages=5,
        // delete_message_on_reception=True, deferrable=True), no aws_conn_id
        // (Airflow's aws_default applies by absence), one >> edge per root.
        let t = Trigger::Single(sqs(
            "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
        ));
        let out = render_trigger(Some(&t), None, None, &["root_task".to_string()]);
        assert_eq!(out.schedule_expr, "None");
        assert_eq!(out.sensor_tasks.len(), 1);
        let st = &out.sensor_tasks[0];
        assert!(st.contains("_yard_wait_sqs = SqsSensor("), "got: {st}");
        assert!(st.contains("task_id=\"_yard_wait_sqs\""), "got: {st}");
        assert!(
            st.contains(
                "sqs_queue=\"https://sqs.us-east-1.amazonaws.com/123456789012/myqueue\""
            ),
            "got: {st}"
        );
        assert!(st.contains("wait_time_seconds=20"), "got: {st}");
        assert!(st.contains("max_messages=5"), "got: {st}");
        assert!(st.contains("delete_message_on_reception=True"), "got: {st}");
        assert!(st.contains("deferrable=True"), "got: {st}");
        assert!(
            !st.contains("aws_conn_id="),
            "no override + no default = no conn line: {st}"
        );
        assert_eq!(
            out.sensor_deps,
            vec!["_yard_wait_sqs >> t_root_task".to_string()]
        );
        assert_eq!(
            out.extra_imports,
            vec!["from airflow.providers.amazon.aws.sensors.sqs import SqsSensor".to_string()]
        );
        // CONC-01 (plan 30-04): any trigger DAG defaults to max_active_runs=1.
        assert_eq!(out.max_active_runs, Some(1));
    }

    #[test]
    fn render_trigger_sqs_user_override_knobs_propagate() {
        // SQS-02: user values override the render defaults verbatim.
        let t = Trigger::Single(SingleSource::Sqs(SqsTrigger {
            queue_url: "q".into(),
            wait_time_seconds: Some(10),
            max_messages: Some(1),
            delete_message_on_reception: Some(false),
        }));
        let out = render_trigger(Some(&t), None, None, &["r".to_string()]);
        let st = &out.sensor_tasks[0];
        assert!(st.contains("wait_time_seconds=10"), "got: {st}");
        assert!(st.contains("max_messages=1"), "got: {st}");
        assert!(
            st.contains("delete_message_on_reception=False"),
            "got: {st}"
        );
        assert!(
            !st.contains("delete_message_on_reception=True"),
            "got: {st}"
        );
    }

    #[test]
    fn render_trigger_sqs_with_dag_default_aws_conn_id_threads_into_sensor() {
        // SqsTrigger has no per-trigger aws_conn_id field (Phase 28 omitted it).
        // The DAG-level default flows in directly; absence on both sides means
        // no kwarg line at all (Airflow's aws_default applies).
        let t = Trigger::Single(sqs("q"));
        let out = render_trigger(
            Some(&t),
            None,
            Some("yard_123456789012_MyRole"),
            &["r".to_string()],
        );
        let st = &out.sensor_tasks[0];
        assert!(
            st.contains("aws_conn_id=\"yard_123456789012_MyRole\""),
            "got: {st}"
        );
    }

    #[test]
    fn render_trigger_sqs_multiple_roots_emit_multiple_dep_edges() {
        // SQS-01: one >> edge per root, in input order — mirrors S3 fan-out.
        let t = Trigger::Single(sqs("q"));
        let out = render_trigger(
            Some(&t),
            None,
            None,
            &["a".to_string(), "b".to_string()],
        );
        assert_eq!(
            out.sensor_deps,
            vec![
                "_yard_wait_sqs >> t_a".to_string(),
                "_yard_wait_sqs >> t_b".to_string(),
            ]
        );
    }

    // --- Phase 30 plan 30-04: API single-source render branch (API-01..API-03) ---

    #[test]
    fn render_trigger_api_default_emits_schedule_none_and_header() {
        // API-01: bare API trigger emits schedule=None, no sensor task, but a
        // header docstring with curl/CLI snippets and placeholders. CONC-01:
        // any trigger DAG defaults to max_active_runs=Some(1).
        let t = Trigger::Single(api(None));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.schedule_expr, "None");
        assert!(out.sensor_tasks.is_empty(), "API has no sensor task");
        assert!(out.sensor_deps.is_empty(), "API has no sensor deps");
        assert!(out.extra_imports.is_empty(), "API needs no provider imports");
        assert_eq!(
            out.max_active_runs,
            Some(1),
            "CONC-01 default fires for any trigger DAG: {out:?}"
        );
        let h = &out.header_docstring;
        assert!(h.contains("$AIRFLOW_URL"), "header missing $AIRFLOW_URL: {h}");
        assert!(h.contains("$AIRFLOW_USER"), "header missing $AIRFLOW_USER: {h}");
        assert!(h.contains("$AIRFLOW_PASS"), "header missing $AIRFLOW_PASS: {h}");
        assert!(h.contains("curl -X POST"), "header missing curl snippet: {h}");
        assert!(
            h.contains("airflow dags trigger"),
            "header missing CLI snippet: {h}"
        );
    }

    #[test]
    fn render_trigger_api_with_description_includes_in_header() {
        // API-02 doc-only: description threads into the header verbatim.
        let t = Trigger::Single(api(Some("Replay failed S3 ingests")));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert!(
            out.header_docstring.contains("Replay failed S3 ingests"),
            "header missing description: {}",
            out.header_docstring
        );
    }

    #[test]
    fn render_trigger_api_with_payload_schema_documents_fields() {
        // API-02 doc-only: payload_schema fields render into the header
        // (sorted alphabetically — BTreeMap iteration is sorted).
        let mut schema = BTreeMap::new();
        schema.insert("customer_id".to_string(), "string".to_string());
        schema.insert("event_id".to_string(), "string".to_string());
        let t = Trigger::Single(SingleSource::Api(ApiTrigger {
            description: None,
            payload_schema: Some(schema),
        }));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        assert!(h.contains("customer_id"), "header missing customer_id: {h}");
        assert!(h.contains("event_id"), "header missing event_id: {h}");
        // BTreeMap is sorted: customer_id appears BEFORE event_id.
        let i_customer = h.find("customer_id").expect("customer_id present");
        let i_event = h.find("event_id").expect("event_id present");
        assert!(
            i_customer < i_event,
            "BTreeMap iteration must render alphabetically: customer_id before event_id: {h}"
        );
    }

    #[test]
    fn render_trigger_api_header_uses_placeholders_not_hardcoded_urls() {
        // API-01: never hardcode URLs. Use $AIRFLOW_URL placeholder.
        let t = Trigger::Single(api(None));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        assert!(
            !h.contains("https://airflow.example.com"),
            "header must not hardcode airflow.example.com: {h}"
        );
        assert!(
            !h.contains("localhost:8080"),
            "header must not hardcode localhost:8080: {h}"
        );
        assert!(h.contains("$AIRFLOW_URL"), "header must use $AIRFLOW_URL: {h}");
    }

    #[test]
    fn render_trigger_api_header_includes_no_auth_management_callout() {
        // API-03: yard does NOT manage Airflow REST auth. Header must say so.
        let t = Trigger::Single(api(None));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        let mentions_no_auth = h.contains("does NOT manage")
            || h.contains("does not manage")
            || h.contains("wire JWT")
            || h.contains("JWT")
            || h.contains("Basic")
            || h.contains("IAM SigV4");
        assert!(
            mentions_no_auth,
            "header must call out that yard does not manage Airflow REST auth: {h}"
        );
    }

    // --- Phase 30 plan 30-04 Task 2: heterogeneous-all (DS-04 + D-11 + D-10) ---

    /// Default-knob bare S3 trigger fixture for the heterogeneous-all tests.
    fn s3_basic(bucket: &str, prefix: &str) -> SingleSource {
        SingleSource::S3(S3Trigger {
            bucket: bucket.to_string(),
            prefix: Some(prefix.to_string()),
            ..Default::default()
        })
    }

    #[test]
    fn render_trigger_heterogeneous_all_s3_sqs_emits_join() {
        // DS-04: heterogeneous Trigger::All(S3, SQS) emits both sensors plus
        // _yard_join EmptyOperator with trigger_rule="all_success", and
        // sensor->join->root edges. CONC-01: max_active_runs=Some(1).
        let t = Trigger::All(vec![s3_basic("b", "p/"), sqs("q")]);
        let out = render_trigger(
            Some(&t),
            None,
            None,
            &["root_a".to_string(), "root_b".to_string()],
        );
        assert_eq!(out.schedule_expr, "None");
        // 2 sensors + _yard_join task = 3 entries.
        assert_eq!(out.sensor_tasks.len(), 3, "tasks: {:?}", out.sensor_tasks);
        // D-07: alpha-sort by source kind. "s3" < "sqs", _yard_join LAST.
        assert!(
            out.sensor_tasks[0].contains("_yard_wait_s3 = S3KeySensor("),
            "first sensor must be S3: {}",
            out.sensor_tasks[0]
        );
        assert!(
            out.sensor_tasks[1].contains("_yard_wait_sqs = SqsSensor("),
            "second sensor must be SQS: {}",
            out.sensor_tasks[1]
        );
        assert!(
            out.sensor_tasks[2].contains("_yard_join = EmptyOperator("),
            "_yard_join task must be last: {}",
            out.sensor_tasks[2]
        );
        assert!(
            out.sensor_tasks[2].contains("task_id=\"_yard_join\""),
            "_yard_join task_id literal: {}",
            out.sensor_tasks[2]
        );
        assert!(
            out.sensor_tasks[2].contains("trigger_rule=\"all_success\""),
            "_yard_join trigger_rule: {}",
            out.sensor_tasks[2]
        );
        // Edges: sensor->join (alpha), then join->root.
        assert_eq!(
            out.sensor_deps,
            vec![
                "_yard_wait_s3 >> _yard_join".to_string(),
                "_yard_wait_sqs >> _yard_join".to_string(),
                "_yard_join >> t_root_a".to_string(),
                "_yard_join >> t_root_b".to_string(),
            ]
        );
        // Imports: S3KeySensor + SqsSensor + EmptyOperator. Vec is sorted by
        // BTreeSet inside render_composite for determinism.
        assert!(
            out.extra_imports.iter().any(|i| i
                .contains("from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor")),
            "S3KeySensor import: {:?}",
            out.extra_imports
        );
        assert!(
            out.extra_imports
                .iter()
                .any(|i| i.contains("from airflow.providers.amazon.aws.sensors.sqs import SqsSensor")),
            "SqsSensor import: {:?}",
            out.extra_imports
        );
        assert!(
            out.extra_imports
                .iter()
                .any(|i| i.contains("from airflow.operators.empty import EmptyOperator")),
            "EmptyOperator import: {:?}",
            out.extra_imports
        );
        assert_eq!(out.max_active_runs, Some(1), "CONC-01 default");
    }

    #[test]
    fn render_trigger_heterogeneous_all_dataset_plus_s3_splits_correctly() {
        // D-11: Trigger::All([Dataset, S3]) — Dataset goes to schedule= level,
        // S3 becomes a sensor. With only ONE sensor, no _yard_join (D-10).
        let t = Trigger::All(vec![ds("ds_uri"), s3_basic("b", "p/")]);
        let out = render_trigger(Some(&t), None, None, &["root".to_string()]);
        assert_eq!(out.schedule_expr, "[Dataset(\"ds_uri\")]");
        // 1 sensor (S3 only — Dataset is NOT a sensor in this split).
        assert_eq!(out.sensor_tasks.len(), 1, "tasks: {:?}", out.sensor_tasks);
        assert!(
            out.sensor_tasks[0].contains("_yard_wait_s3 = S3KeySensor("),
            "S3 sensor only: {}",
            out.sensor_tasks[0]
        );
        // No Dataset sensor edge — Dataset fires at schedule level.
        assert_eq!(
            out.sensor_deps,
            vec!["_yard_wait_s3 >> t_root".to_string()]
        );
        // Imports include both Dataset (schedule level) and S3KeySensor.
        assert!(
            out.extra_imports
                .iter()
                .any(|i| i.contains("from airflow.datasets import Dataset")),
            "Dataset import: {:?}",
            out.extra_imports
        );
        assert!(
            out.extra_imports.iter().any(|i| i
                .contains("from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor")),
            "S3KeySensor import: {:?}",
            out.extra_imports
        );
        // No EmptyOperator import — only one sensor, no join needed.
        assert!(
            !out.extra_imports
                .iter()
                .any(|i| i.contains("EmptyOperator")),
            "single-sensor mixed Dataset+sensor must NOT pull EmptyOperator: {:?}",
            out.extra_imports
        );
    }

    #[test]
    fn render_trigger_heterogeneous_all_dataset_plus_two_sensors_emits_join() {
        // D-11 + DS-04: Dataset at schedule level, S3 + SQS sensors under
        // _yard_join (multiple sensors require the join).
        let t = Trigger::All(vec![
            ds("ds_uri"),
            s3_basic("b", "p/"),
            sqs("q"),
        ]);
        let out = render_trigger(Some(&t), None, None, &["root".to_string()]);
        assert_eq!(out.schedule_expr, "[Dataset(\"ds_uri\")]");
        assert_eq!(out.sensor_tasks.len(), 3, "tasks: {:?}", out.sensor_tasks);
        assert!(
            out.sensor_tasks[2].contains("_yard_join = EmptyOperator("),
            "_yard_join must appear: {}",
            out.sensor_tasks[2]
        );
        assert_eq!(
            out.sensor_deps,
            vec![
                "_yard_wait_s3 >> _yard_join".to_string(),
                "_yard_wait_sqs >> _yard_join".to_string(),
                "_yard_join >> t_root".to_string(),
            ]
        );
    }

    #[test]
    fn render_trigger_heterogeneous_all_three_sensors_alpha_sorted() {
        // D-07: source-kind alphabetical (api < s3 < sqs). API has no sensor
        // task (it contributes header docstring only) — sensor_tasks must
        // contain only S3 + SQS + _yard_join.
        let t = Trigger::All(vec![sqs("q"), s3_basic("b", "p/"), api(None)]);
        let out = render_trigger(Some(&t), None, None, &["root".to_string()]);
        assert_eq!(
            out.sensor_tasks.len(),
            3,
            "API contributes no sensor task — should be S3 + SQS + _yard_join: {:?}",
            out.sensor_tasks
        );
        assert!(
            out.sensor_tasks[0].contains("_yard_wait_s3"),
            "alpha-sorted: S3 first: {}",
            out.sensor_tasks[0]
        );
        assert!(
            out.sensor_tasks[1].contains("_yard_wait_sqs"),
            "alpha-sorted: SQS second: {}",
            out.sensor_tasks[1]
        );
        assert!(
            out.sensor_tasks[2].contains("_yard_join"),
            "_yard_join last: {}",
            out.sensor_tasks[2]
        );
        // API still contributes header docstring.
        assert!(
            !out.header_docstring.is_empty(),
            "API arm contributes header docstring even in heterogeneous-all"
        );
    }

    #[test]
    fn render_trigger_single_sensor_does_not_emit_join() {
        // D-10: bare-single S3 trigger (or Trigger::All([S3]) collapsed) must
        // NOT emit _yard_join. Already covered by 30-02 fixtures, re-asserted
        // here to lock the contract.
        let t = Trigger::Single(s3_basic("b", "p/"));
        let out = render_trigger(Some(&t), None, None, &["root".to_string()]);
        assert_eq!(out.sensor_tasks.len(), 1, "single sensor only");
        assert!(
            !out.sensor_tasks[0].contains("_yard_join"),
            "single-sensor must NOT emit _yard_join: {}",
            out.sensor_tasks[0]
        );
        assert!(
            !out.extra_imports
                .iter()
                .any(|i| i.contains("EmptyOperator")),
            "single-sensor must NOT import EmptyOperator: {:?}",
            out.extra_imports
        );
    }

    #[test]
    fn render_trigger_homogeneous_all_datasets_does_not_emit_join() {
        // D-10: homogeneous all-Datasets is pure schedule-level (no sensors).
        // Re-assert no _yard_join leaks in.
        let t = Trigger::All(vec![ds("a"), ds("b")]);
        let out = render_trigger(Some(&t), None, None, &["root".to_string()]);
        assert!(out.sensor_tasks.is_empty(), "no sensor tasks for Datasets");
        assert!(
            !out.extra_imports
                .iter()
                .any(|i| i.contains("EmptyOperator")),
            "homogeneous Datasets must NOT pull EmptyOperator: {:?}",
            out.extra_imports
        );
    }

    #[test]
    fn render_trigger_max_active_runs_auto_default_for_all_trigger_kinds() {
        // CONC-01: every kind of trigger flips max_active_runs to Some(1).
        let cases: Vec<Trigger> = vec![
            Trigger::Single(s3_basic("b", "p/")),
            Trigger::Single(sqs("q")),
            Trigger::Single(api(None)),
            Trigger::Single(ds("a")),
            Trigger::All(vec![ds("a"), ds("b")]),
            Trigger::Any(vec![ds("a"), ds("b")]),
            Trigger::All(vec![s3_basic("b", "p/"), sqs("q")]),
        ];
        for t in &cases {
            let out = render_trigger(Some(t), None, None, &["r".to_string()]);
            assert_eq!(
                out.max_active_runs,
                Some(1),
                "CONC-01 must default to Some(1) for every trigger kind: {t:?}"
            );
        }
    }

    #[test]
    fn render_trigger_max_active_runs_none_for_no_trigger() {
        // PRES-02: schedule-only DAGs (trigger.is_none()) preserve Airflow's
        // implicit default of 16 — no max_active_runs= line emitted.
        let out = render_trigger(None, Some("@daily"), None, &[]);
        assert_eq!(out.max_active_runs, None, "schedule-only must not auto-default");
        let out2 = render_trigger(None, None, None, &[]);
        assert_eq!(out2.max_active_runs, None);
    }

    // --- Phase 32 plan 32-03: VERSION_BANNER + per-source backfill caveats (DOC-05) ---
    //
    // Six checks across five tests + two integration fixtures (in mod.rs):
    // - Dataset / S3 / SQS arms now populate header_docstring with a per-source
    //   backfill caveat block (D-13).
    // - render_trigger prepends a fixed Airflow-version-contract banner whenever
    //   trigger.is_some() (D-11 / D-14b — inline-prepend, no template change).
    // - Schedule-only path (trigger.is_none()) keeps header_docstring empty so
    //   the 20+ pre-Phase-32 schedule-only fixtures stay byte-identical (PRES-02
    //   / D-12).
    // - API arm regression: Phase 30's "# Trigger: API" header still rendered,
    //   PLUS the new banner is prepended.

    #[test]
    fn render_trigger_dataset_emits_per_source_backfill_header() {
        // D-13: Dataset arm now fills header_docstring with the
        // "Datasets have no logical_date" backfill caveat. D-11/D-14b: the
        // VERSION_BANNER is prepended above it because trigger.is_some().
        let t = Trigger::Single(SingleSource::Dataset(DatasetTrigger {
            uri: "s3://x/y".into(),
        }));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        // Per-source backfill caveat block (Dataset).
        assert!(
            h.contains("# Trigger: Dataset (s3://x/y)"),
            "Dataset header missing per-source line: {h}"
        );
        assert!(
            h.contains("Backfill caveat: Datasets have no logical_date"),
            "Dataset header missing backfill caveat: {h}"
        );
        // Version banner — prepended on every event-driven trigger.
        assert!(
            h.contains("# Airflow version contract:"),
            "Dataset header missing VERSION_BANNER: {h}"
        );
        assert!(
            h.contains("#   - apache-airflow-providers-amazon >= 8.13.0"),
            "Dataset header missing providers-amazon banner line: {h}"
        );
    }

    #[test]
    fn render_trigger_s3_emits_per_source_backfill_header() {
        // D-13: S3 arm now fills header_docstring with the deferrable-sensor
        // re-poke backfill caveat. Banner prepended (D-11).
        let t = Trigger::Single(SingleSource::S3(S3Trigger {
            bucket: "b".into(),
            key: None,
            prefix: Some("p".into()),
            ..Default::default()
        }));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        assert!(
            h.contains("# Airflow version contract:"),
            "S3 header missing VERSION_BANNER: {h}"
        );
        assert!(
            h.contains("deferrable sensor re-pokes"),
            "S3 header missing backfill caveat: {h}"
        );
    }

    #[test]
    fn render_trigger_sqs_emits_per_source_backfill_header() {
        // D-13: SQS arm now fills header_docstring with the destructive-drain
        // backfill caveat. Banner prepended (D-11).
        let t = Trigger::Single(sqs("https://sqs/q"));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        assert!(
            h.contains("# Airflow version contract:"),
            "SQS header missing VERSION_BANNER: {h}"
        );
        assert!(
            h.contains("drains the real queue"),
            "SQS header missing backfill caveat: {h}"
        );
    }

    #[test]
    fn render_trigger_schedule_emits_no_banner() {
        // D-12 / PRES-02: schedule-only DAGs (trigger.is_none()) MUST render
        // an EMPTY header_docstring — banner absent, no per-source block.
        // This guarantees the 20+ existing schedule-only fixtures stay
        // byte-identical to pre-Phase-32 output.
        let out = render_trigger(None, Some("@daily"), None, &[]);
        assert!(
            out.header_docstring.is_empty(),
            "schedule-only must not emit any header (PRES-02): {:?}",
            out.header_docstring
        );
        let out2 = render_trigger(None, None, None, &[]);
        assert!(
            out2.header_docstring.is_empty(),
            "schedule-only (no schedule literal) must not emit any header: {:?}",
            out2.header_docstring
        );
    }

    #[test]
    fn render_trigger_api_arm_still_passes_phase_30_regression() {
        // The Phase 30 API header content stays — just gets the banner
        // prepended. Asserts both: the existing "# Trigger: API" marker AND
        // the new VERSION_BANNER prefix.
        let t = Trigger::Single(SingleSource::Api(ApiTrigger::default()));
        let out = render_trigger(Some(&t), None, None, &[]);
        let h = &out.header_docstring;
        assert!(
            h.contains("# Trigger: API"),
            "Phase 30 API header marker must still appear: {h}"
        );
        assert!(
            h.contains("# Airflow version contract:"),
            "Phase 32 banner must be prepended on API arm too: {h}"
        );
        // Banner must precede the per-source block.
        let i_banner = h.find("# Airflow version contract:").expect("banner present");
        let i_api = h.find("# Trigger: API").expect("api marker present");
        assert!(
            i_banner < i_api,
            "VERSION_BANNER must be prepended ABOVE the API per-source header: {h}"
        );
    }
}
