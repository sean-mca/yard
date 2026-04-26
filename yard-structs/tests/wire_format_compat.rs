//! Wire-format regression test (PRES-05, Phase 21 D-22).
//!
//! Locks the user-facing yard.yaml + state-file JSON shape across the typed-
//! configs migration. Loads the multi-job fixture under
//! `tests/fixtures/wire_format/`, deserializes through `ProjectManifest`, and
//! asserts that re-serializing produces a byte-equal JSON Value. Any
//! accidental Rust-side rename or shape-change that would invalidate
//! existing user state files breaks this test.
//!
//! Plan 21-02 extended this test to specifically exercise the typed
//! `AwsCredentialConfig` round-trip across the four call sites
//! (StateBackend::S3.aws, ProjectManifest.aws, AirflowSection.aws inside
//! `providers.airflow`, and the per-job `_aws` blob inside
//! `JobDefinition.config` which intentionally stays Value-typed per D-14).
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

#[test]
fn typed_aws_credential_config_round_trips_at_all_four_sites() {
    // Plan 21-02 (TYPE-02): the fixture covers every wire shape that the
    // typed `Option<AwsCredentialConfig>` migration touches. This test
    // pulls the parsed manifest apart and confirms the typed accessors
    // agree with the fixture's JSON values across all four call sites,
    // PLUS the per-job `_aws` Value blob which deliberately stays untyped
    // per D-14 (forward-compat for provider-specific extension fields).
    let input: serde_json::Value =
        serde_json::from_str(FIXTURE).expect("fixture is valid JSON");
    let parsed: ProjectManifest =
        serde_json::from_value(input).expect("fixture must parse as ProjectManifest");

    // Site 1: ProjectManifest.aws (root) — populated case.
    let root = parsed.aws.as_ref().expect("root aws block must parse");
    assert_eq!(
        root.assume_role.as_deref(),
        Some("arn:aws:iam::444444444444:role/RootAws")
    );
    assert_eq!(root.region.as_deref(), Some("us-east-1"));
    assert!(root.external_id.is_none());

    // Site 2: StateBackend::S3.aws — populated case.
    if let yard_structs::StateBackend::S3 { aws, .. } = &parsed.state {
        let creds = aws.as_ref().expect("state.aws must parse");
        assert_eq!(
            creds.assume_role.as_deref(),
            Some("arn:aws:iam::111111111111:role/StateAccess")
        );
        assert_eq!(creds.external_id.as_deref(), Some("xid-1"));
    } else {
        panic!("expected StateBackend::S3 in fixture");
    }

    // Site 3: AirflowSection.aws — populated case (inside `providers.airflow`,
    // which is parsed by yard-core::parsing::parse_airflow_section at runtime;
    // here we just confirm the JSON shape is what AwsCredentialConfig parses).
    let airflow_aws = parsed
        .providers
        .get("airflow")
        .and_then(|v| v.get("aws"))
        .cloned()
        .expect("fixture providers.airflow.aws must be present");
    let airflow_creds: yard_structs::AwsCredentialConfig =
        serde_json::from_value(airflow_aws).expect("airflow aws must parse as AwsCredentialConfig");
    assert_eq!(
        airflow_creds.assume_role.as_deref(),
        Some("arn:aws:iam::222222222222:role/DagUpload")
    );

    // Site 4 (D-14): per-job `_aws` stays inside JobDefinition.config: Value.
    // Confirm that JobDefinition.config is still untyped Value after Plan 21-02
    // and that the `_aws` blob is round-trippable as AwsCredentialConfig at
    // the consumer boundary (where dag_lifecycle / providers actually parse it).
    let ingest = parsed
        .jobs
        .get("ingest-glue")
        .expect("fixture must define ingest-glue job");
    let per_job_aws = ingest
        .config
        .get("_aws")
        .cloned()
        .expect("ingest-glue must have a per-job _aws override");
    let per_job_creds: yard_structs::AwsCredentialConfig =
        serde_json::from_value(per_job_aws).expect("per-job _aws must parse as AwsCredentialConfig");
    assert_eq!(
        per_job_creds.assume_role.as_deref(),
        Some("arn:aws:iam::333333333333:role/PerJobOverride")
    );

    // Negative case: jobs without an `_aws` block must surface as None
    // when consumers query the typed view.
    let compute = parsed.jobs.get("compute-emr").expect("compute-emr present");
    assert!(
        compute.config.get("_aws").is_none(),
        "compute-emr fixture has no _aws override"
    );
}
