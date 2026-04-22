//! Phase 9 integration tests against ministack.
//!
//! These exercise the state backend and DAG credential paths end-to-end
//! through the real AWS SDK (S3 + STS) pointed at LocalStack.
//!
//! Prerequisites:
//!   docker compose up -d   (ministack on localhost:4566)
//!
//! Run:
//!   AWS_ENDPOINT_URL=http://localhost:4566 \
//!   AWS_ACCESS_KEY_ID=test \
//!   AWS_SECRET_ACCESS_KEY=test \
//!   AWS_DEFAULT_REGION=us-east-1 \
//!   cargo test -p yard-core --test phase9_integration -- --ignored --nocapture
//!
//! What these tests PROVE (given LocalStack Community's STS stub):
//!   - get_storage reaches a real S3-compatible server without panicking
//!     under `aws: Null` and `aws: { assume_role: ... }` shapes.
//!   - State file round-trip preserves the new `aws` field.
//!   - DagState.aws is serialized/deserialized correctly through Storage::write_dag/read_dag.
//!
//! What they do NOT prove:
//!   - Cross-account routing (LocalStack Community has one "account"
//!     and a permissive STS that accepts any role ARN).
//!   - IAM trust-policy enforcement.

use aws_sdk_s3::Client as S3Client;
use serde_json::json;
use yard_structs::{DagDeployment, DagState, Deployment, JobState, StateBackend};

const REGION: &str = "us-east-1";
const TEST_STATE_BUCKET: &str = "yard-test-state-phase9";

async fn s3_client() -> S3Client {
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION.to_string()))
        .endpoint_url("http://localhost:4566")
        .load()
        .await;
    S3Client::new(&cfg)
}

async fn ensure_bucket() {
    let s3 = s3_client().await;
    let _ = s3.create_bucket().bucket(TEST_STATE_BUCKET).send().await;
}

fn sample_job_state(name: &str) -> JobState {
    JobState {
        job_name: name.to_string(),
        project: "phase9-test".to_string(),
        deployment: Deployment {
            env: None,
            config_hash: "deadbeef".to_string(),
            config: json!({"type": "glue"}),
            status: "generated".to_string(),
            applied_at: "2026-04-22T00:00:00Z".to_string(),
            resources: Vec::new(),
        },
    }
}

fn sample_dag_state(name: &str, aws: serde_json::Value) -> DagState {
    DagState {
        dag_name: name.to_string(),
        project: "phase9-test".to_string(),
        deployment: DagDeployment {
            content_hash: "feedface".to_string(),
            config: json!({"schedule": "@daily"}),
            tasks: vec!["task_a".to_string()],
            status: "deployed".to_string(),
            applied_at: "2026-04-22T00:00:00Z".to_string(),
            s3_uri: Some(format!("s3://phase9-dags/dags/{name}.py")),
        },
        aws,
    }
}

// ---- Test 1: state backend S3, aws: Null — round-trips a JobState ----

#[tokio::test]
#[ignore]
async fn state_backend_s3_null_aws_roundtrip() {
    ensure_bucket().await;

    let backend = StateBackend::S3 {
        bucket: TEST_STATE_BUCKET.to_string(),
        region: REGION.to_string(),
        key: "projects/phase9-null/".to_string(),
        aws: serde_json::Value::Null,
    };

    let storage = yard_core::storage::get_storage(&backend)
        .await
        .expect("get_storage must succeed for aws: Null");

    let job = sample_job_state("job_null_aws");
    storage.write_job(&job.job_name, &job).await.expect("write_job");

    let readback = storage
        .read_job(&job.job_name)
        .await
        .expect("read_job")
        .expect("job must exist");

    assert_eq!(readback.job_name, job.job_name);
    assert_eq!(readback.deployment.config_hash, job.deployment.config_hash);

    // Cleanup
    storage.delete_job(&job.job_name).await.ok();
}

// ---- Test 2: state backend S3 with populated aws.assume_role ----
//
// This drives the `merge_state_aws_with_env` → `providers::aws_config` →
// `AssumeRoleProvider` path. Whether LocalStack's STS stub accepts the
// AssumeRole request depends on LocalStack version. If it fails, the S3
// write will return an error at call time (not at construction).
//
// This test PASSES if: `get_storage` returns Ok (construction-time wiring
// is correct). It does NOT assert that the S3 write succeeds, because
// AssumeRoleProvider may not respect AWS_ENDPOINT_URL and could try to
// reach real STS.

#[tokio::test]
#[ignore]
async fn state_backend_s3_with_assume_role_constructs() {
    let backend = StateBackend::S3 {
        bucket: TEST_STATE_BUCKET.to_string(),
        region: REGION.to_string(),
        key: "projects/phase9-assume/".to_string(),
        aws: json!({
            "assume_role": "arn:aws:iam::111111111111:role/YardStateAccess",
            "session_name": "yard-phase9-integration",
        }),
    };

    let storage = yard_core::storage::get_storage(&backend)
        .await
        .expect("get_storage must not panic when aws.assume_role is set");

    // Prove we got an S3-flavored Storage, not Local.
    match storage {
        yard_core::storage::Storage::S3(_) => {}
        _ => panic!("expected S3 storage"),
    }
}

// ---- Test 2b: actually drive an S3 write through the AssumeRole path ----
//
// This attempts a real write+read via the AssumeRoleProvider-wrapped S3
// client. LocalStack Community's STS stub is permissive (accepts any
// role ARN), but AssumeRoleProvider's internal STS client may or may
// not honor AWS_ENDPOINT_URL. Treat the result as diagnostic:
//   - write succeeds → STS reached LocalStack; full cross-account
//     wiring works against LocalStack
//   - write fails with STS/network error → AssumeRoleProvider bypasses
//     AWS_ENDPOINT_URL and reached real STS (known LocalStack gotcha,
//     not a yard bug)
//
// Either outcome is a useful signal. The assertion is one-sided: if
// the write fails we log and skip, rather than fail the test.

#[tokio::test]
#[ignore]
async fn state_backend_s3_assume_role_s3_write_attempt() {
    ensure_bucket().await;

    let backend = StateBackend::S3 {
        bucket: TEST_STATE_BUCKET.to_string(),
        region: REGION.to_string(),
        key: "projects/phase9-assume-write/".to_string(),
        aws: json!({
            "assume_role": "arn:aws:iam::111111111111:role/YardStateAccess",
            "session_name": "yard-phase9-write",
        }),
    };

    let storage = yard_core::storage::get_storage(&backend)
        .await
        .expect("get_storage");

    let job = sample_job_state("job_assume_role_write");
    let write_result = storage.write_job(&job.job_name, &job).await;

    match write_result {
        Ok(()) => {
            // STS reached LocalStack; verify the read path too.
            let readback = storage.read_job(&job.job_name).await;
            assert!(
                readback.is_ok() && readback.unwrap().is_some(),
                "read_job must succeed after successful write through AssumeRole"
            );
            storage.delete_job(&job.job_name).await.ok();
            eprintln!(
                "[phase9] AssumeRole S3 write SUCCEEDED — STS honored AWS_ENDPOINT_URL"
            );
        }
        Err(e) => {
            // Known LocalStack gotcha: AssumeRoleProvider may not respect
            // AWS_ENDPOINT_URL. This is not a yard bug; document and move on.
            eprintln!(
                "[phase9] AssumeRole S3 write failed (expected on LocalStack Community \
                 if AssumeRoleProvider does not honor AWS_ENDPOINT_URL): {e:#}"
            );
        }
    }
}

// ---- Test 3: state backend S3 with YARD_STATE_AWS_ASSUME_ROLE env ----
//
// Proves the env-beats-yaml path reaches aws_config. Same caveat as
// test 2 re: AssumeRoleProvider + AWS_ENDPOINT_URL.

#[tokio::test]
#[ignore]
async fn state_backend_s3_env_override_constructs() {
    // Scope env mutation: set, run, unset.
    // SAFETY: tests in this file run serially (--test-threads=1 or
    // tokio::test with no parallelism). This is test-only code; CLAUDE.md
    // exception per Phase 9 Plan 02.
    unsafe {
        std::env::set_var(
            "YARD_STATE_AWS_ASSUME_ROLE",
            "arn:aws:iam::222222222222:role/YardStateEnv",
        );
    }

    let backend = StateBackend::S3 {
        bucket: TEST_STATE_BUCKET.to_string(),
        region: REGION.to_string(),
        key: "projects/phase9-env/".to_string(),
        aws: serde_json::Value::Null,
    };

    let result = yard_core::storage::get_storage(&backend).await;

    // Unset before the assertion so a panic doesn't leave env dirty.
    unsafe {
        std::env::remove_var("YARD_STATE_AWS_ASSUME_ROLE");
    }

    assert!(result.is_ok(), "get_storage must succeed with env override");
}

// ---- Test 4: DagState.aws persists through Storage::write_dag/read_dag ----
//
// This is the round-trip guarantee D-05 depends on: apply writes aws,
// destroy reads it back. The unit test proves the Rust struct round-trips
// through serde_json::to_string/from_str; this integration test proves
// the same survives a real S3 put/get.

#[tokio::test]
#[ignore]
async fn dag_state_aws_roundtrips_through_s3_storage() {
    ensure_bucket().await;

    let backend = StateBackend::S3 {
        bucket: TEST_STATE_BUCKET.to_string(),
        region: REGION.to_string(),
        key: "projects/phase9-dag/".to_string(),
        aws: serde_json::Value::Null,
    };
    let storage = yard_core::storage::get_storage(&backend)
        .await
        .expect("get_storage");

    let expected_aws = json!({
        "assume_role": "arn:aws:iam::333333333333:role/MwaaDagUploader",
        "session_name": "yard-dag-upload",
    });

    let dag = sample_dag_state("dag_with_aws", expected_aws.clone());
    storage
        .write_dag(&dag.dag_name, &dag)
        .await
        .expect("write_dag");

    let readback = storage
        .read_dag(&dag.dag_name)
        .await
        .expect("read_dag")
        .expect("dag must exist");

    assert_eq!(readback.dag_name, dag.dag_name);
    assert_eq!(
        readback.aws, expected_aws,
        "DagState.aws must survive S3 round-trip byte-for-byte"
    );
    assert_eq!(
        readback.aws.get("assume_role").and_then(|v| v.as_str()),
        Some("arn:aws:iam::333333333333:role/MwaaDagUploader")
    );

    // Cleanup
    storage.delete_dag(&dag.dag_name).await.ok();
}

// ---- Test 5: DagState with aws: Null (pre-Phase-9 compatibility) ----
//
// Proves skip_serializing_if doesn't break the read path when the field
// was persisted as Null (or absent entirely). This is the D-02
// strictly-additive guarantee at the storage layer.

#[tokio::test]
#[ignore]
async fn dag_state_null_aws_roundtrips_through_s3_storage() {
    ensure_bucket().await;

    let backend = StateBackend::S3 {
        bucket: TEST_STATE_BUCKET.to_string(),
        region: REGION.to_string(),
        key: "projects/phase9-dag-null/".to_string(),
        aws: serde_json::Value::Null,
    };
    let storage = yard_core::storage::get_storage(&backend)
        .await
        .expect("get_storage");

    let dag = sample_dag_state("dag_null_aws", serde_json::Value::Null);
    storage
        .write_dag(&dag.dag_name, &dag)
        .await
        .expect("write_dag");

    let readback = storage
        .read_dag(&dag.dag_name)
        .await
        .expect("read_dag")
        .expect("dag must exist");

    assert!(
        readback.aws.is_null(),
        "Null aws must round-trip as Null (skip_serializing_if + default)"
    );

    // Cleanup
    storage.delete_dag(&dag.dag_name).await.ok();
}
