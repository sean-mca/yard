//! Integration tests for the EMR provider against ministack.
//!
//! Prerequisites:
//!   docker compose up -d   (ministack on localhost:4566)
//!
//! Run:
//!   AWS_ENDPOINT_URL=http://localhost:4566 \
//!   AWS_ACCESS_KEY_ID=test \
//!   AWS_SECRET_ACCESS_KEY=test \
//!   AWS_DEFAULT_REGION=us-east-1 \
//!   cargo test -p yard-core --test emr_integration -- --ignored --nocapture

use aws_config::BehaviorVersion;
use aws_sdk_emr::Client as EmrClient;
use aws_sdk_s3::Client as S3Client;
use serde_json::json;
use yard_core::providers::get_provider;

const ENDPOINT: &str = "http://localhost:4566";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "yard-emr-test-scripts";

async fn aws_config() -> aws_config::SdkConfig {
    aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION.to_string()))
        .endpoint_url(ENDPOINT)
        .load()
        .await
}

async fn s3_client() -> S3Client {
    S3Client::new(&aws_config().await)
}

async fn emr_client() -> EmrClient {
    EmrClient::new(&aws_config().await)
}

async fn ensure_bucket(s3: &S3Client) {
    let _ = s3.create_bucket().bucket(TEST_BUCKET).send().await;
}

/// Create a ministack EMR cluster and return its ID.
async fn create_test_cluster(emr: &EmrClient) -> String {
    let resp = emr
        .run_job_flow()
        .name("yard-test-cluster")
        .release_label("emr-7.0.0")
        .instances(
            aws_sdk_emr::types::JobFlowInstancesConfig::builder()
                .master_instance_type("m5.xlarge")
                .slave_instance_type("m5.xlarge")
                .instance_count(2)
                .keep_job_flow_alive_when_no_steps(true)
                .build(),
        )
        .service_role("arn:aws:iam::123456789012:role/EMR_DefaultRole")
        .job_flow_role("EMR_EC2_DefaultRole")
        .applications(
            aws_sdk_emr::types::Application::builder()
                .name("Spark")
                .build(),
        )
        .send()
        .await
        .expect("Failed to create test EMR cluster");

    resp.job_flow_id().unwrap().to_string()
}

async fn terminate_cluster(emr: &EmrClient, cluster_id: &str) {
    let _ = emr
        .terminate_job_flows()
        .job_flow_ids(cluster_id)
        .send()
        .await;
}

fn test_provider_config(cluster_id: &str) -> serde_json::Value {
    json!({
        "region": REGION,
        "script_bucket": TEST_BUCKET,
        "script_prefix": "yard-scripts/",
        "cluster_id": cluster_id,
    })
}

fn test_job_config() -> serde_json::Value {
    json!({
        "type": "emr",
    })
}

const TEST_SCRIPT: &str = r#"
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
df.show()
spark.stop()
"#;

// ---- S3 Script Upload ----

#[tokio::test]
#[ignore]
async fn emr_s3_upload_script() {
    let s3 = s3_client().await;
    let emr = emr_client().await;
    ensure_bucket(&s3).await;

    let cluster_id = create_test_cluster(&emr).await;

    let provider = get_provider("emr", &test_provider_config(&cluster_id))
        .await
        .expect("Failed to create provider");

    let resources = provider
        .deploy("test-emr-upload", TEST_SCRIPT, &test_job_config())
        .await
        .expect("Deploy failed");

    // Verify S3 object
    let s3_resource = resources
        .iter()
        .find(|r| r.r#type == "s3_object")
        .expect("No s3_object resource");

    assert!(
        s3_resource.id.contains("yard-scripts/test-emr-upload.py"),
        "S3 URI mismatch: {}",
        s3_resource.id
    );

    // Read back
    let resp = s3
        .get_object()
        .bucket(TEST_BUCKET)
        .key("yard-scripts/test-emr-upload.py")
        .send()
        .await
        .expect("Failed to get S3 object");

    let body = resp
        .body
        .collect()
        .await
        .expect("Failed to read body")
        .into_bytes();

    let content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");
    assert!(content.contains("SparkSession"), "Script content mismatch");

    // Cleanup
    provider
        .destroy("test-emr-upload", &resources)
        .await
        .expect("Destroy failed");
    terminate_cluster(&emr, &cluster_id).await;
}

// ---- EMR Step Submission ----

#[tokio::test]
#[ignore]
async fn emr_submit_step() {
    let s3 = s3_client().await;
    let emr = emr_client().await;
    ensure_bucket(&s3).await;

    let cluster_id = create_test_cluster(&emr).await;

    let provider = get_provider("emr", &test_provider_config(&cluster_id))
        .await
        .expect("Failed to create provider");

    let resources = provider
        .deploy("test-emr-step", TEST_SCRIPT, &test_job_config())
        .await
        .expect("Deploy failed");

    // Verify step resource
    let step_resource = resources
        .iter()
        .find(|r| r.r#type == "emr_step")
        .expect("No emr_step resource");

    assert!(
        !step_resource.id.is_empty(),
        "Step ID should not be empty"
    );

    // Note: ministack returns timestamps as strings instead of epoch floats,
    // which causes the AWS SDK's ListSteps deserialization to fail.
    // The step was successfully submitted (we got a step ID back from AddJobFlowSteps),
    // so we verify the ID is well-formed instead.
    assert!(
        step_resource.id.starts_with("s-"),
        "Step ID should start with 's-', got: {}",
        step_resource.id
    );

    // Cleanup
    provider
        .destroy("test-emr-step", &resources)
        .await
        .expect("Destroy failed");
    terminate_cluster(&emr, &cluster_id).await;
}

// ---- Destroy Cleans Up ----

#[tokio::test]
#[ignore]
async fn emr_destroy_cleans_up() {
    let s3 = s3_client().await;
    let emr = emr_client().await;
    ensure_bucket(&s3).await;

    let cluster_id = create_test_cluster(&emr).await;

    let provider = get_provider("emr", &test_provider_config(&cluster_id))
        .await
        .expect("Failed to create provider");

    let resources = provider
        .deploy("test-emr-destroy", TEST_SCRIPT, &test_job_config())
        .await
        .expect("Deploy failed");

    provider
        .destroy("test-emr-destroy", &resources)
        .await
        .expect("Destroy failed");

    // Verify S3 script is gone
    let result = s3
        .get_object()
        .bucket(TEST_BUCKET)
        .key("yard-scripts/test-emr-destroy.py")
        .send()
        .await;

    assert!(result.is_err(), "S3 script should not exist after destroy");

    terminate_cluster(&emr, &cluster_id).await;
}

// ---- Config Validation ----

#[tokio::test]
#[ignore]
async fn emr_missing_cluster_id_errors() {
    let config = json!({
        "region": REGION,
        "script_bucket": TEST_BUCKET,
        // no cluster_id
    });

    let result = get_provider("emr", &config).await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Should fail without cluster_id"),
    };
    assert!(
        err.contains("cluster_id"),
        "Error should mention cluster_id, got: {err}"
    );
}

#[tokio::test]
#[ignore]
async fn emr_missing_script_bucket_errors() {
    let config = json!({
        "region": REGION,
        "cluster_id": "j-FAKE",
        // no script_bucket
    });

    let result = get_provider("emr", &config).await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Should fail without script_bucket"),
    };
    assert!(
        err.contains("script_bucket"),
        "Error should mention script_bucket, got: {err}"
    );
}

// ---- Idempotent Deploy (update script, new step) ----

#[tokio::test]
#[ignore]
async fn emr_deploy_twice_creates_new_step() {
    let s3 = s3_client().await;
    let emr = emr_client().await;
    ensure_bucket(&s3).await;

    let cluster_id = create_test_cluster(&emr).await;

    let provider = get_provider("emr", &test_provider_config(&cluster_id))
        .await
        .expect("Failed to create provider");

    // First deploy
    let resources1 = provider
        .deploy("test-emr-idempotent", TEST_SCRIPT, &test_job_config())
        .await
        .expect("First deploy failed");

    let step1 = resources1
        .iter()
        .find(|r| r.r#type == "emr_step")
        .expect("No step in first deploy")
        .id
        .clone();

    // Second deploy
    let updated = TEST_SCRIPT.to_string() + "\n# updated\n";
    let resources2 = provider
        .deploy("test-emr-idempotent", &updated, &test_job_config())
        .await
        .expect("Second deploy failed");

    let step2 = resources2
        .iter()
        .find(|r| r.r#type == "emr_step")
        .expect("No step in second deploy")
        .id
        .clone();

    // Each deploy creates a new step
    assert_ne!(step1, step2, "Second deploy should create a new step ID");

    // Cleanup
    provider
        .destroy("test-emr-idempotent", &resources2)
        .await
        .expect("Destroy failed");
    terminate_cluster(&emr, &cluster_id).await;
}
