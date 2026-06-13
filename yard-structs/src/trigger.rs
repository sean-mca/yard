//! Typed trigger model for Airflow DAGs (TRIG-01..TRIG-03, Phase 28).
//!
//! Replaces the legacy untyped `triggered_by: Vec<String>` field on
//! `AirflowSection` with a typed `Trigger` enum that supports five trigger
//! sources (schedule, S3, Dataset, SQS, manual API) plus composite `all`/
//! `any` shapes (D-01, D-02). Leaf structs each carry
//! `#[serde(deny_unknown_fields)]` (T-28-01-05 mitigation) so attacker-
//! controlled dag.yaml files cannot smuggle extra fields past the parse
//! boundary.
//!
//! `Trigger` and `SingleSource` are NOT `#[derive(Serialize, Deserialize)]`
//! — they get hand-rolled impls because:
//! - The deserialize path needs to emit the actionable `unknown trigger
//!   source 'X' — valid: schedule, s3, dataset, sqs, api, all, any` error
//!   for typos (D-03, D-21).
//! - The serialize path needs to sort composite (`all`/`any`) lists by
//!   canonical-JSON-string of each element so `Trigger::All([a, b])` and
//!   `Trigger::All([b, a])` produce byte-identical wire forms (HASH-02
//!   invariant locked at the type boundary, D-08, D-10).
//! - The schedule single-source flattens its wrapper to a bare-string wire
//!   form (`{"schedule": "@daily"}`, not `{"schedule": {"value": "@daily"}}`)
//!   per D-Discretion specifics.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

/// A cron-style schedule trigger (e.g. `"@daily"`, `"0 8 * * *"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleTrigger {
    /// Cron expression or Airflow preset string.
    pub value: String,
}

/// S3 file-drop trigger via `S3KeySensor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct S3Trigger {
    /// S3 bucket to watch.
    pub bucket: String,
    /// Exact S3 key to wait for (mutually exclusive with `prefix`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// S3 key prefix to watch (mutually exclusive with `key`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Sensor polling interval in seconds (minimum 10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poke_interval: Option<u64>,
    /// Sensor timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    /// Airflow connection ID override for S3 access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws_conn_id: Option<String>,
    /// Whether to use deferrable mode (defaults to true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deferrable: Option<bool>,
}

/// Airflow Dataset trigger for cross-DAG dependency scheduling.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetTrigger {
    /// Dataset URI (e.g. `"s3://bucket/path"`).
    pub uri: String,
}

/// SQS queue trigger via `SqsSensor`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqsTrigger {
    /// SQS queue URL to poll.
    pub queue_url: String,
    /// Long-poll wait time in seconds (defaults to 20).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_time_seconds: Option<u64>,
    /// Maximum messages per poll (defaults to 5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<u32>,
    /// Whether to delete messages after receipt (defaults to true).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_message_on_reception: Option<bool>,
}

/// Manual API trigger (DAG runs via Airflow REST API).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ApiTrigger {
    /// Human-readable description of when/why to trigger this DAG.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Expected payload schema (field name to type description).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_schema: Option<BTreeMap<String, String>>,
}

/// Top-level trigger configuration for an Airflow DAG.
///
/// A trigger is either a single source, or a composite (`all` / `any`) of
/// multiple sources. Hand-rolled `Serialize` and `Deserialize` impls enforce
/// canonical wire forms and actionable error messages.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum Trigger {
    /// A single trigger source (schedule, S3, Dataset, SQS, or API).
    Single(SingleSource),
    /// All listed sources must fire before the DAG runs.
    All(Vec<SingleSource>),
    /// Any one of the listed sources firing triggers the DAG.
    Any(Vec<SingleSource>),
}

/// A single trigger source variant.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum SingleSource {
    /// Cron-style schedule trigger.
    Schedule(ScheduleTrigger),
    /// S3 file-drop sensor trigger.
    S3(S3Trigger),
    /// Airflow Dataset dependency trigger.
    Dataset(DatasetTrigger),
    /// SQS queue sensor trigger.
    Sqs(SqsTrigger),
    /// Manual API invocation trigger.
    Api(ApiTrigger),
}

// --- SingleSource hand-rolled (de)serialize (D-03, D-10) ---

impl<'de> Deserialize<'de> for SingleSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SingleSourceVisitor;

        impl<'de> serde::de::Visitor<'de> for SingleSourceVisitor {
            type Value = SingleSource;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a single-key trigger source map (schedule|s3|dataset|sqs|api)")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let key: String = match map.next_key::<String>()? {
                    Some(k) => k,
                    None => {
                        return Err(serde::de::Error::custom(
                            "trigger source map cannot be empty",
                        ));
                    }
                };
                let val: serde_json::Value = map.next_value()?;
                if map.next_key::<String>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "trigger source map must contain exactly one key",
                    ));
                }
                let result = match key.as_str() {
                    "schedule" => {
                        // Bare-string wire form: { schedule: "@daily" }, not { schedule: { value: "@daily" } }.
                        let value: String = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        SingleSource::Schedule(ScheduleTrigger { value })
                    }
                    "s3" => {
                        let t: S3Trigger = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        SingleSource::S3(t)
                    }
                    "dataset" | "asset" => {
                        let t: DatasetTrigger = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        SingleSource::Dataset(t)
                    }
                    "sqs" => {
                        let t: SqsTrigger = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        SingleSource::Sqs(t)
                    }
                    "api" => {
                        let t: ApiTrigger = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        SingleSource::Api(t)
                    }
                    other => {
                        return Err(serde::de::Error::custom(format!(
                            "unknown trigger source '{other}' — valid: schedule, s3, dataset (or asset), sqs, api, all, any"
                        )));
                    }
                };
                Ok(result)
            }
        }

        deserializer.deserialize_map(SingleSourceVisitor)
    }
}

impl Serialize for SingleSource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut m = serializer.serialize_map(Some(1))?;
        match self {
            // Bare-string wire form: { schedule: "@daily" }, not { schedule: { value: "@daily" } }.
            SingleSource::Schedule(t) => m.serialize_entry("schedule", &t.value)?,
            SingleSource::S3(t) => m.serialize_entry("s3", t)?,
            SingleSource::Dataset(t) => m.serialize_entry("dataset", t)?,
            SingleSource::Sqs(t) => m.serialize_entry("sqs", t)?,
            SingleSource::Api(t) => m.serialize_entry("api", t)?,
        }
        m.end()
    }
}

// --- Trigger hand-rolled (de)serialize (D-03, D-07, D-08, D-10) ---

impl<'de> Deserialize<'de> for Trigger {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TriggerVisitor;

        impl<'de> serde::de::Visitor<'de> for TriggerVisitor {
            type Value = Trigger;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a single-key trigger map (schedule|s3|dataset|sqs|api|all|any)",
                )
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let key: String = match map.next_key::<String>()? {
                    Some(k) => k,
                    None => {
                        return Err(serde::de::Error::custom("trigger map cannot be empty"));
                    }
                };
                let val: serde_json::Value = map.next_value()?;
                if map.next_key::<String>()?.is_some() {
                    return Err(serde::de::Error::custom(
                        "trigger map must contain exactly one key",
                    ));
                }
                match key.as_str() {
                    "all" => {
                        let v: Vec<SingleSource> = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Trigger::All(v))
                    }
                    "any" => {
                        let v: Vec<SingleSource> = serde_json::from_value(val)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Trigger::Any(v))
                    }
                    _ => {
                        // Delegate single-source case to SingleSource::Deserialize, which
                        // emits the actionable `unknown trigger source 'X' — valid: ...`
                        // error message on typos.
                        let single_map = serde_json::json!({ key: val });
                        let s: SingleSource = serde_json::from_value(single_map)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Trigger::Single(s))
                    }
                }
            }
        }

        deserializer.deserialize_map(TriggerVisitor)
    }
}

impl Serialize for Trigger {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            Trigger::Single(s) => s.serialize(serializer),
            // D-12 (Phase 30, plan 30-01): collapse single-element composites
            // to bare-single so `all: [x]` / `any: [x]` and `<x>` produce
            // byte-identical wire form AND blake3 hash (HASH-01). This sits
            // BEFORE the multi-element sort branch so len==1 short-circuits
            // through `s.serialize` directly without entering the map writer.
            Trigger::All(items) | Trigger::Any(items) if items.len() == 1 => {
                items[0].serialize(serializer)
            }
            Trigger::All(items) | Trigger::Any(items) => {
                // Sort by canonical-JSON-string of each element so
                // Trigger::All([a, b]) and Trigger::All([b, a]) produce
                // byte-identical wire forms (HASH-02 invariant, D-08).
                let mut sortable: Vec<(String, &SingleSource)> = items
                    .iter()
                    .map(|elem| {
                        let s = serde_json::to_string(elem)
                            .map_err(serde::ser::Error::custom)?;
                        Ok::<_, S::Error>((s, elem))
                    })
                    .collect::<Result<_, _>>()?;
                sortable.sort_by(|a, b| a.0.cmp(&b.0));
                let sorted: Vec<&SingleSource> =
                    sortable.into_iter().map(|(_, v)| v).collect();
                let key = if matches!(self, Trigger::All(_)) {
                    "all"
                } else {
                    "any"
                };
                let mut m = serializer.serialize_map(Some(1))?;
                m.serialize_entry(key, &sorted)?;
                m.end()
            }
        }
    }
}

impl Trigger {
    /// Extract dataset URIs from a Trigger for v1.5-compat dataset-trigger
    /// codegen. Returns empty Vec if no Dataset variants are present
    /// (e.g., schedule-only, s3, sqs, api triggers — those route through
    /// Phase 30 codegen which emits sensor tasks instead of `schedule=[Dataset(...)]`).
    ///
    /// Used by `yard-core/src/airflow_dag/generation.rs` to preserve the
    /// existing v1.5 dataset-trigger schedule-rendering behavior after
    /// the typed Trigger model lands.
    pub fn dataset_uris(&self) -> Vec<&str> {
        match self {
            Trigger::Single(SingleSource::Dataset(d)) => vec![d.uri.as_str()],
            Trigger::All(sources) | Trigger::Any(sources) => sources
                .iter()
                .filter_map(|s| match s {
                    SingleSource::Dataset(d) => Some(d.uri.as_str()),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}

impl SingleSource {
    /// Wire-form key for this source variant — must agree exactly with
    /// the Serialize impl keys at lines 177-183 above. Used by
    /// `validate_dag_full` (TRIG-06) to render the `{kinds}` list of
    /// non-Dataset sources in heterogeneous-`any:` errors.
    pub fn source_kind(&self) -> &'static str {
        match self {
            SingleSource::Schedule(_) => "schedule",
            SingleSource::S3(_) => "s3",
            SingleSource::Dataset(_) => "dataset",
            SingleSource::Sqs(_) => "sqs",
            SingleSource::Api(_) => "api",
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trigger_single_source_round_trip_schedule() {
        let input = json!({"schedule": "@daily"});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(
            parsed,
            Trigger::Single(SingleSource::Schedule(ScheduleTrigger {
                value: "@daily".into(),
            }))
        );
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input, "schedule wire form must be bare-string");
    }

    #[test]
    fn trigger_single_source_round_trip_s3() {
        let input = json!({"s3": {"bucket": "b", "prefix": "p"}});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(
            parsed,
            Trigger::Single(SingleSource::S3(S3Trigger {
                bucket: "b".into(),
                prefix: Some("p".into()),
                ..Default::default()
            }))
        );
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input, "None fields must be skipped on serialize");
    }

    #[test]
    fn trigger_single_source_round_trip_dataset() {
        let input = json!({"dataset": {"uri": "s3://x"}});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(
            parsed,
            Trigger::Single(SingleSource::Dataset(DatasetTrigger {
                uri: "s3://x".into(),
            }))
        );
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn trigger_single_source_round_trip_sqs() {
        let input = json!({"sqs": {"queue_url": "https://sqs.us-east-1.amazonaws.com/111111111111/q"}});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(
            parsed,
            Trigger::Single(SingleSource::Sqs(SqsTrigger {
                queue_url: "https://sqs.us-east-1.amazonaws.com/111111111111/q".into(),
                wait_time_seconds: None,
                max_messages: None,
                delete_message_on_reception: None,
            }))
        );
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn trigger_single_source_round_trip_api() {
        let input = json!({"api": {}});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(
            parsed,
            Trigger::Single(SingleSource::Api(ApiTrigger {
                description: None,
                payload_schema: None,
            }))
        );
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn trigger_composite_all_round_trip() {
        let input = json!({"all": [{"dataset": {"uri": "a"}}, {"dataset": {"uri": "b"}}]});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert!(matches!(parsed, Trigger::All(_)));
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn trigger_composite_any_round_trip() {
        let input = json!({"any": [{"dataset": {"uri": "a"}}, {"dataset": {"uri": "b"}}]});
        let parsed: Trigger = serde_json::from_value(input.clone()).unwrap();
        assert!(matches!(parsed, Trigger::Any(_)));
        let reser = serde_json::to_value(&parsed).unwrap();
        assert_eq!(reser, input);
    }

    #[test]
    fn trigger_unknown_source_rejects() {
        let err = serde_json::from_value::<Trigger>(json!({"s4": {"bucket": "x"}}))
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown trigger source 's4'"),
            "got: {msg}"
        );
        assert!(
            msg.contains("valid: schedule, s3, dataset (or asset), sqs, api, all, any"),
            "got: {msg}"
        );
    }

    #[test]
    fn trigger_empty_map_rejects() {
        let err = serde_json::from_value::<Trigger>(json!({})).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("trigger map cannot be empty"), "got: {msg}");
    }

    #[test]
    fn trigger_multi_key_rejects() {
        let err = serde_json::from_value::<Trigger>(json!({
            "s3": {"bucket": "x"},
            "dataset": {"uri": "y"}
        }))
        .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("trigger map must contain exactly one key"),
            "got: {msg}"
        );
    }

    #[test]
    fn trigger_serialize_sort_homogeneous() {
        let t1 = Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
        ]);
        let t2 = Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
        ]);
        assert_eq!(
            serde_json::to_string(&t1).unwrap(),
            serde_json::to_string(&t2).unwrap(),
            "homogeneous Trigger::All must serialize order-independently (HASH-02)"
        );
    }

    #[test]
    fn trigger_serialize_sort_heterogeneous() {
        let t1 = Trigger::All(vec![
            SingleSource::S3(S3Trigger {
                bucket: "z".into(),
                ..Default::default()
            }),
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
        ]);
        let t2 = Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
            SingleSource::S3(S3Trigger {
                bucket: "z".into(),
                ..Default::default()
            }),
        ]);
        assert_eq!(
            serde_json::to_string(&t1).unwrap(),
            serde_json::to_string(&t2).unwrap(),
            "heterogeneous Trigger::All must serialize order-independently"
        );
    }

    #[test]
    fn trigger_serialize_sort_any_homogeneous() {
        let t1 = Trigger::Any(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
        ]);
        let t2 = Trigger::Any(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
        ]);
        assert_eq!(
            serde_json::to_string(&t1).unwrap(),
            serde_json::to_string(&t2).unwrap(),
            "homogeneous Trigger::Any must serialize order-independently"
        );
    }

    #[test]
    fn trigger_serialize_single_no_sort() {
        let t = Trigger::Single(SingleSource::Schedule(ScheduleTrigger {
            value: "@daily".into(),
        }));
        let s = serde_json::to_string(&t).unwrap();
        assert_eq!(s, r#"{"schedule":"@daily"}"#);
    }

    #[test]
    fn s3_trigger_deny_unknown_fields() {
        let err = serde_json::from_value::<Trigger>(json!({
            "s3": {"bucket": "x", "buckt": "y"}
        }))
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown field"), "got: {msg}");
        assert!(msg.contains("buckt"), "got: {msg}");
    }

    #[test]
    fn dataset_trigger_deny_unknown_fields() {
        let err = serde_json::from_value::<Trigger>(json!({
            "dataset": {"uri": "x", "extra": {}}
        }))
        .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("unknown field"), "got: {msg}");
        assert!(msg.contains("extra"), "got: {msg}");
    }

    // --- Trigger::dataset_uris (Phase 28 v1.5-compat helper) ---

    #[test]
    fn dataset_uris_extracts_from_single_dataset() {
        let t = Trigger::Single(SingleSource::Dataset(DatasetTrigger {
            uri: "s3://x".into(),
        }));
        assert_eq!(t.dataset_uris(), vec!["s3://x"]);
    }

    #[test]
    fn dataset_uris_extracts_from_composite_all_dataset() {
        let t = Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger {
                uri: "s3://a".into(),
            }),
            SingleSource::Dataset(DatasetTrigger {
                uri: "s3://b".into(),
            }),
        ]);
        let mut uris = t.dataset_uris();
        uris.sort();
        assert_eq!(uris, vec!["s3://a", "s3://b"]);
    }

    #[test]
    fn dataset_uris_empty_for_non_dataset_trigger() {
        let t = Trigger::Single(SingleSource::Schedule(ScheduleTrigger {
            value: "@daily".into(),
        }));
        assert_eq!(t.dataset_uris(), Vec::<&str>::new());
    }

    #[test]
    fn dataset_uris_filters_heterogeneous_composite() {
        let t = Trigger::Any(vec![
            SingleSource::Dataset(DatasetTrigger {
                uri: "s3://a".into(),
            }),
            SingleSource::S3(S3Trigger {
                bucket: "b".into(),
                ..Default::default()
            }),
        ]);
        assert_eq!(t.dataset_uris(), vec!["s3://a"]);
    }

    // --- SingleSource::source_kind (Phase 29 wire-key helper, D-19) ---

    // --- Single-element composite collapse (Phase 30, plan 30-01, D-12) ---

    #[test]
    fn trigger_serialize_collapses_single_element_all_to_bare_single() {
        // D-12: Trigger::All([x]) must serialize byte-identical to
        // Trigger::Single(x) so `all: [x]` and `<x>` produce same wire form
        // (and therefore same blake3 hash via diff.rs).
        let collapsed = Trigger::All(vec![SingleSource::Dataset(DatasetTrigger {
            uri: "s3://x".into(),
        })]);
        let bare = Trigger::Single(SingleSource::Dataset(DatasetTrigger {
            uri: "s3://x".into(),
        }));
        assert_eq!(
            serde_json::to_string(&collapsed).unwrap(),
            r#"{"dataset":{"uri":"s3://x"}}"#,
            "Trigger::All([x]) must collapse to bare single on serialize"
        );
        assert_eq!(
            serde_json::to_string(&collapsed).unwrap(),
            serde_json::to_string(&bare).unwrap(),
            "single-element all and bare-single must serialize byte-identical"
        );
    }

    #[test]
    fn trigger_serialize_collapses_single_element_any_to_bare_single() {
        let collapsed = Trigger::Any(vec![SingleSource::Dataset(DatasetTrigger {
            uri: "s3://x".into(),
        })]);
        let bare = Trigger::Single(SingleSource::Dataset(DatasetTrigger {
            uri: "s3://x".into(),
        }));
        assert_eq!(
            serde_json::to_string(&collapsed).unwrap(),
            r#"{"dataset":{"uri":"s3://x"}}"#
        );
        assert_eq!(
            serde_json::to_string(&collapsed).unwrap(),
            serde_json::to_string(&bare).unwrap(),
            "single-element any and bare-single must serialize byte-identical"
        );
    }

    #[test]
    fn trigger_single_element_composite_blake3_matches_bare_single() {
        // HASH-01 regression: byte-identical serialization is the necessary
        // and sufficient condition for identical blake3 hashes (diff.rs hashes
        // serde_json::to_string(trigger).as_bytes()). Testing string equality
        // here proves the hash equality without pulling blake3 into yard-structs.
        let ds_x = SingleSource::Dataset(DatasetTrigger { uri: "x".into() });
        let single = serde_json::to_string(&Trigger::Single(ds_x.clone())).unwrap();
        let all = serde_json::to_string(&Trigger::All(vec![ds_x.clone()])).unwrap();
        let any = serde_json::to_string(&Trigger::Any(vec![ds_x])).unwrap();
        assert_eq!(single, all, "Trigger::All([x]) bytes must match Single(x)");
        assert_eq!(single, any, "Trigger::Any([x]) bytes must match Single(x)");
        // Sanity check: same bytes -> same hash for any hasher.
        assert_eq!(
            single.as_bytes(),
            all.as_bytes(),
            "byte equality is the HASH-01 invariant precondition"
        );
    }

    #[test]
    fn trigger_serialize_two_element_composite_keeps_all_key() {
        // Collapse must only fire at len==1. Two-element composites keep the
        // `all:` / `any:` wrapper.
        let t = Trigger::All(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
        ]);
        let s = serde_json::to_string(&t).unwrap();
        assert!(
            s.starts_with(r#"{"all":"#),
            "two-element all must keep 'all' key, got: {s}"
        );
    }

    #[test]
    fn trigger_serialize_two_element_any_keeps_any_key() {
        let t = Trigger::Any(vec![
            SingleSource::Dataset(DatasetTrigger { uri: "a".into() }),
            SingleSource::Dataset(DatasetTrigger { uri: "b".into() }),
        ]);
        let s = serde_json::to_string(&t).unwrap();
        assert!(
            s.starts_with(r#"{"any":"#),
            "two-element any must keep 'any' key, got: {s}"
        );
    }

    // --- SingleSource::source_kind (Phase 29 wire-key helper, D-19) ---

    // --- Asset alias (Phase 55, ALIAS-01/02/03) ---

    #[test]
    fn trigger_asset_alias_single_source_round_trip() {
        // D-10/D-12/D-17: "asset" parses as Dataset, re-serializes as "dataset"
        for key in ["dataset", "asset"] {
            let input = serde_json::json!({key: {"uri": "s3://b/k"}});
            let parsed: SingleSource = serde_json::from_value(input).unwrap();
            assert_eq!(
                parsed,
                SingleSource::Dataset(DatasetTrigger {
                    uri: "s3://b/k".into(),
                }),
                "'{key}' must parse as SingleSource::Dataset"
            );
            // ALIAS-02: serialization always emits "dataset"
            let reser = serde_json::to_value(&parsed).unwrap();
            assert_eq!(
                reser,
                serde_json::json!({"dataset": {"uri": "s3://b/k"}}),
                "'{key}' input must re-serialize with 'dataset' key"
            );
        }
    }

    #[test]
    fn trigger_asset_alias_all_composite() {
        // D-11: "asset" inside all: composite flows through SingleSource
        let input = serde_json::json!({"all": [
            {"asset": {"uri": "s3://a"}},
            {"dataset": {"uri": "s3://b"}}
        ]});
        let parsed: Trigger = serde_json::from_value(input).unwrap();
        match &parsed {
            Trigger::All(sources) => {
                assert_eq!(sources.len(), 2);
                assert_eq!(
                    sources[0],
                    SingleSource::Dataset(DatasetTrigger { uri: "s3://a".into() })
                );
                assert_eq!(
                    sources[1],
                    SingleSource::Dataset(DatasetTrigger { uri: "s3://b".into() })
                );
            }
            other => panic!("expected Trigger::All, got {other:?}"),
        }
    }

    #[test]
    fn trigger_asset_alias_any_composite() {
        // D-11: "asset" inside any: composite
        let input = serde_json::json!({"any": [{"asset": {"uri": "s3://a"}}]});
        let parsed: Trigger = serde_json::from_value(input).unwrap();
        match &parsed {
            Trigger::Any(sources) => {
                assert_eq!(sources.len(), 1);
                assert_eq!(
                    sources[0],
                    SingleSource::Dataset(DatasetTrigger { uri: "s3://a".into() })
                );
            }
            other => panic!("expected Trigger::Any, got {other:?}"),
        }
    }

    #[test]
    fn trigger_unknown_source_mentions_asset() {
        // D-13/D-18: error message must include "dataset (or asset)"
        let err = serde_json::from_value::<SingleSource>(serde_json::json!({"typo": {}}))
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("dataset (or asset)"),
            "unknown-source error must mention asset alias, got: {msg}"
        );
    }

    #[test]
    fn trigger_unknown_source_via_trigger_level_mentions_asset() {
        // D-14: Trigger-level delegation propagates the updated message
        let err = serde_json::from_value::<Trigger>(serde_json::json!({"typo": {}}))
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("dataset (or asset)"),
            "Trigger-level unknown-source error must mention asset alias, got: {msg}"
        );
    }

    #[test]
    fn single_source_kind_returns_wire_keys() {
        assert_eq!(
            SingleSource::Schedule(ScheduleTrigger {
                value: "@daily".into(),
            })
            .source_kind(),
            "schedule"
        );
        assert_eq!(
            SingleSource::S3(S3Trigger {
                bucket: "b".into(),
                ..Default::default()
            })
            .source_kind(),
            "s3"
        );
        assert_eq!(
            SingleSource::Dataset(DatasetTrigger { uri: "x".into() }).source_kind(),
            "dataset"
        );
        assert_eq!(
            SingleSource::Sqs(SqsTrigger {
                queue_url: "q".into(),
                wait_time_seconds: None,
                max_messages: None,
                delete_message_on_reception: None,
            })
            .source_kind(),
            "sqs"
        );
        assert_eq!(
            SingleSource::Api(ApiTrigger::default()).source_kind(),
            "api"
        );
    }
}
