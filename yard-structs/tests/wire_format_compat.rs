//! Wire-format regression test (PRES-05, Phase 21 D-22).
//!
//! Locks the user-facing yard.yaml + state-file JSON shape across the typed-
//! configs migration. Loads the multi-job fixture under
//! `tests/fixtures/wire_format/`, deserializes through `ProjectManifest`, and
//! asserts that re-serializing produces a byte-equal JSON Value. Any
//! accidental Rust-side rename or shape-change that would invalidate
//! existing user state files breaks this test.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use yard_structs::ProjectManifest;

const FIXTURE: &str = include_str!("fixtures/wire_format/multi_job_manifest.json");

#[test]
fn round_trip_locks_wire_format() {
    let input: serde_json::Value =
        serde_json::from_str(FIXTURE).expect("fixture is valid JSON");

    let parsed: ProjectManifest =
        serde_json::from_value(input.clone()).expect("fixture must parse as ProjectManifest");

    let reserialized: serde_json::Value =
        serde_json::to_value(&parsed).expect("serialization must succeed");

    assert_eq!(
        reserialized, input,
        "wire format drift: deserialize→serialize did not round-trip"
    );
}
