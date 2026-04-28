use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DiffType {
    Create,
    Modify {
        changes: BTreeMap<String, (String, String)>,
    },
    Delete,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Diff {
    pub name: String,
    pub diff_type: DiffType,
    pub old_hash: Option<String>,
    pub new_hash: Option<String>,
}

/// Alias preserved for callers; structurally identical to `Diff`.
pub type JobDiff = Diff;
/// Alias preserved for callers; structurally identical to `Diff`.
pub type DagDiff = Diff;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn job_diff_modify_round_trip() {
        // D-10 case 1: JobDiff (alias for Diff) with DiffType::Modify { changes }
        // populated + old_hash + new_hash. Round-trip JSON; assert exactly four
        // top-level fields and no spurious `_phantom`-style noise.
        let mut changes: BTreeMap<String, (String, String)> = BTreeMap::new();
        changes.insert(
            "config".to_string(),
            ("old-value".to_string(), "new-value".to_string()),
        );
        let diff: JobDiff = JobDiff {
            name: "my-job".to_string(),
            diff_type: DiffType::Modify { changes },
            old_hash: Some("hash-old".to_string()),
            new_hash: Some("hash-new".to_string()),
        };

        let serialized = serde_json::to_value(&diff).unwrap();
        let parsed: JobDiff = serde_json::from_value(serialized.clone()).unwrap();
        let reserialized = serde_json::to_value(&parsed).unwrap();

        // Round-trip equality (proves no data loss on serialize → deserialize → serialize).
        assert_eq!(
            reserialized, serialized,
            "JobDiff JSON must round-trip byte-identically"
        );

        // Assert exactly four top-level fields, no extras (defensive against
        // future regressions like an accidental PhantomData marker leaking out).
        let obj = serialized
            .as_object()
            .expect("Diff must serialize as a JSON object");
        assert_eq!(obj.len(), 4, "Diff must emit exactly 4 top-level fields");
        assert!(obj.contains_key("name"));
        assert!(obj.contains_key("diff_type"));
        assert!(obj.contains_key("old_hash"));
        assert!(obj.contains_key("new_hash"));
    }

    #[test]
    fn dag_diff_create_and_delete_round_trip() {
        // D-10 case 2: DagDiff (alias for Diff) covering DiffType::Create
        // (no `changes` map) and DiffType::Delete. Same four-field assertion.
        let create: DagDiff = DagDiff {
            name: "my-dag".to_string(),
            diff_type: DiffType::Create,
            old_hash: None,
            new_hash: Some("hash-new".to_string()),
        };
        let create_json = serde_json::to_value(&create).unwrap();
        let create_back: DagDiff = serde_json::from_value(create_json.clone()).unwrap();
        let create_again = serde_json::to_value(&create_back).unwrap();
        assert_eq!(
            create_again, create_json,
            "DagDiff (Create) JSON must round-trip byte-identically"
        );
        assert_eq!(create_json.as_object().unwrap().len(), 4);

        let delete: DagDiff = DagDiff {
            name: "my-dag".to_string(),
            diff_type: DiffType::Delete,
            old_hash: Some("hash-old".to_string()),
            new_hash: None,
        };
        let delete_json = serde_json::to_value(&delete).unwrap();
        let delete_back: DagDiff = serde_json::from_value(delete_json.clone()).unwrap();
        let delete_again = serde_json::to_value(&delete_back).unwrap();
        assert_eq!(
            delete_again, delete_json,
            "DagDiff (Delete) JSON must round-trip byte-identically"
        );
        assert_eq!(delete_json.as_object().unwrap().len(), 4);
    }

    #[test]
    fn job_diff_and_dag_diff_json_shapes_are_byte_identical() {
        // D-10 optional but encouraged: JobDiff and DagDiff produce byte-identical
        // JSON for the same field values. Locks the alias-equivalence contract.
        let job: JobDiff = JobDiff {
            name: "shared".to_string(),
            diff_type: DiffType::Create,
            old_hash: None,
            new_hash: Some("h".to_string()),
        };
        let dag: DagDiff = DagDiff {
            name: "shared".to_string(),
            diff_type: DiffType::Create,
            old_hash: None,
            new_hash: Some("h".to_string()),
        };
        let job_json = serde_json::to_value(&job).unwrap();
        let dag_json = serde_json::to_value(&dag).unwrap();
        assert_eq!(
            job_json, dag_json,
            "JobDiff and DagDiff must serialize identically (they are aliases for Diff)"
        );
    }

    #[test]
    fn job_diff_modify_changes_emits_sorted_keys() {
        // Phase 28 / D-16: `DiffType::Modify::changes` is a `BTreeMap`, so
        // iteration is sorted by key at the type level. This test locks the
        // invariant by deliberately inserting B, A, C out-of-order and
        // asserting the serialized JSON has them in A, B, C order.
        let mut changes: BTreeMap<String, (String, String)> = BTreeMap::new();
        // Deliberately-out-of-order inserts; BTreeMap normalizes.
        changes.insert("B".to_string(), ("old_b".into(), "new_b".into()));
        changes.insert("A".to_string(), ("old_a".into(), "new_a".into()));
        changes.insert("C".to_string(), ("old_c".into(), "new_c".into()));
        let diff = Diff {
            name: "demo".to_string(),
            old_hash: None,
            new_hash: Some("h".to_string()),
            diff_type: DiffType::Modify { changes },
        };
        let s = serde_json::to_string(&diff).unwrap();
        let pos_a = s.find("\"A\"").expect("key A in serialized output");
        let pos_b = s.find("\"B\"").expect("key B in serialized output");
        let pos_c = s.find("\"C\"").expect("key C in serialized output");
        assert!(pos_a < pos_b, "expected A before B in {s}");
        assert!(pos_b < pos_c, "expected B before C in {s}");
    }
}
