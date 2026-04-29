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

use super::helpers::python_string_literal;

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
    _default_aws_conn_id: Option<&str>,
    _roots: &[String],
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
    if let Some(single) = normalized {
        return render_single(single);
    }

    // Branch 2: composite (>= 2 elements).
    if let Some(t) = trigger {
        return render_composite(t);
    }

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
    }
}

/// Bare-single render branch (DS-01 + 30.2/30.3/30.4 stubs).
fn render_single(s: &SingleSource) -> TriggerRender {
    match s {
        SingleSource::Schedule(sched) => TriggerRender {
            schedule_expr: python_string_literal(&sched.value),
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: Vec::new(),
            max_active_runs: None,
        },
        SingleSource::Dataset(d) => TriggerRender {
            schedule_expr: format!("[Dataset({})]", python_string_literal(&d.uri)),
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: vec!["from airflow.datasets import Dataset".to_string()],
            max_active_runs: None,
        },
        // S3 / SQS / API render branches land in plans 30-02 / 30-03 / 30-04.
        // Until then, fall back to schedule=None — this code path is never
        // actually hit by tests until those plans ship, because no fixture
        // declares a non-Dataset single-source trigger in plan 30-01.
        SingleSource::S3(_) | SingleSource::Sqs(_) | SingleSource::Api(_) => TriggerRender {
            schedule_expr: "None".to_string(),
            sensor_tasks: Vec::new(),
            sensor_deps: Vec::new(),
            extra_imports: Vec::new(),
            max_active_runs: None,
        },
    }
}

/// Composite render branch — Datasets-only homogeneous all/any (DS-02, DS-03).
/// Heterogeneous all + max_active_runs default land in plan 30-04.
fn render_composite(t: &Trigger) -> TriggerRender {
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
        };
    }

    // Heterogeneous-all (DS-04) + non-Dataset composites land in plans
    // 30-02 / 30-03 / 30-04. For now (plan 30-01), these return a
    // placeholder that won't be exercised by 30-01 fixtures; Phase 29's
    // TRIG-06 already rejects heterogeneous-`any:`, so the only composite
    // shape that reaches this fall-through is heterogeneous-`all:` which
    // 30-04 will own.
    TriggerRender {
        schedule_expr: "None".to_string(),
        sensor_tasks: Vec::new(),
        sensor_deps: Vec::new(),
        extra_imports: Vec::new(),
        max_active_runs: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use yard_structs::{DatasetTrigger, ScheduleTrigger};

    fn ds(uri: &str) -> SingleSource {
        SingleSource::Dataset(DatasetTrigger {
            uri: uri.to_string(),
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
    fn render_trigger_max_active_runs_field_is_none_for_datasets_only_in_this_plan() {
        // Plan 30-01 stakes the field shape but does not yet wire CONC-01
        // auto-default-to-Some(1). That lands in plan 30-04. Lock the
        // current behavior so plan 30-04 can flip it intentionally.
        let t = Trigger::Single(ds("s3://x"));
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.max_active_runs, None);
    }

    #[test]
    fn render_trigger_homogeneous_dataset_two_elements_imports_dataset_once() {
        let t = Trigger::All(vec![ds("s3://a"), ds("s3://b")]);
        let out = render_trigger(Some(&t), None, None, &[]);
        assert_eq!(out.extra_imports.len(), 1);
        assert_eq!(out.extra_imports[0], "from airflow.datasets import Dataset");
    }
}
