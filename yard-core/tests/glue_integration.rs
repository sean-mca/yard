//! Integration tests for the Glue provider against ministack.
//!
//! Prerequisites:
//!   docker compose up -d   (ministack on localhost:4566)
//!
//! Run:
//!   AWS_ENDPOINT_URL=http://localhost:4566 \
//!   AWS_ACCESS_KEY_ID=test \
//!   AWS_SECRET_ACCESS_KEY=test \
//!   AWS_DEFAULT_REGION=us-east-1 \
//!   cargo test -p yard-core --test glue_integration -- --ignored --nocapture

use aws_config::BehaviorVersion;
use aws_sdk_glue::Client as GlueClient;
use aws_sdk_s3::Client as S3Client;
use serde_json::json;
use yard_core::providers::get_provider;
use yard_structs::JobType;

const ENDPOINT: &str = "http://localhost:4566";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "yard-test-scripts";

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

async fn glue_client() -> GlueClient {
    GlueClient::new(&aws_config().await)
}

async fn ensure_bucket(s3: &S3Client) {
    let _ = s3
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await;
}

fn test_provider_config() -> serde_json::Value {
    json!({
        "region": REGION,
        "script_bucket": TEST_BUCKET,
        "script_prefix": "yard-scripts/",
        "glue_version": "4.0",
        "worker_type": "G.1X",
        "number_of_workers": 2,
    })
}

fn test_job_config() -> serde_json::Value {
    json!({
        "type": "glue",
        "role": "arn:aws:iam::123456789012:role/TestGlueRole",
    })
}

const TEST_SCRIPT: &str = r#"
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
"#;

// ---- S3 Script Upload ----

#[tokio::test]
#[ignore]
async fn s3_upload_script() {

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let provider = get_provider(JobType::Glue, &test_provider_config())
        .await
        .expect("Failed to create provider");

    let resources = provider
        .deploy("test-s3-upload", TEST_SCRIPT, &test_job_config())
        .await
        .expect("Deploy failed");

    // Verify S3 object was created
    let s3_resource = resources
        .iter()
        .find(|r| r.r#type == "s3_object")
        .expect("No s3_object resource returned");

    assert!(
        s3_resource.id.contains("yard-scripts/test-s3-upload.py"),
        "S3 URI should contain the script path, got: {}",
        s3_resource.id
    );

    // Verify we can read the script back from S3
    let resp = s3
        .get_object()
        .bucket(TEST_BUCKET)
        .key("yard-scripts/test-s3-upload.py")
        .send()
        .await
        .expect("Failed to get object from S3");

    let body = resp
        .body
        .collect()
        .await
        .expect("Failed to read body")
        .into_bytes();

    let content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");
    assert!(content.contains("getResolvedOptions"), "Script content mismatch");

    // Cleanup
    provider
        .destroy("test-s3-upload", &resources)
        .await
        .expect("Destroy failed");
}

// ---- Glue Job Create ----

#[tokio::test]
#[ignore]
async fn glue_create_job() {

    let s3 = s3_client().await;
    let glue = glue_client().await;
    ensure_bucket(&s3).await;

    let provider = get_provider(JobType::Glue, &test_provider_config())
        .await
        .expect("Failed to create provider");

    let resources = provider
        .deploy("test-create-job", TEST_SCRIPT, &test_job_config())
        .await
        .expect("Deploy failed");

    // Verify Glue job resource was returned
    let glue_resource = resources
        .iter()
        .find(|r| r.r#type == "glue_job")
        .expect("No glue_job resource returned");

    assert_eq!(glue_resource.id, "test-create-job");

    // Verify job exists in Glue
    let job = glue
        .get_job()
        .job_name("test-create-job")
        .send()
        .await
        .expect("Failed to get Glue job");

    let job = job.job().expect("No job in response");
    assert_eq!(job.name(), Some("test-create-job"));
    assert_eq!(job.role(), Some("arn:aws:iam::123456789012:role/TestGlueRole"));

    let command = job.command().expect("No command on job");
    assert_eq!(command.name(), Some("glueetl"));
    assert!(
        command
            .script_location()
            .unwrap_or("")
            .contains("yard-scripts/test-create-job.py"),
        "Script location mismatch"
    );

    // Cleanup
    provider
        .destroy("test-create-job", &resources)
        .await
        .expect("Destroy failed");
}

// ---- Glue Job Update (idempotent deploy) ----

#[tokio::test]
#[ignore]
async fn glue_update_job_idempotent() {

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let provider = get_provider(JobType::Glue, &test_provider_config())
        .await
        .expect("Failed to create provider");

    // First deploy — creates the job
    let resources = provider
        .deploy("test-update-job", TEST_SCRIPT, &test_job_config())
        .await
        .expect("First deploy failed");

    // Second deploy — should update, not error
    let updated_script = TEST_SCRIPT.to_string() + "\n# updated\n";
    let resources2 = provider
        .deploy("test-update-job", &updated_script, &test_job_config())
        .await
        .expect("Second deploy (update) failed");

    assert_eq!(resources.len(), resources2.len());

    // Verify the script was actually updated
    let resp = s3
        .get_object()
        .bucket(TEST_BUCKET)
        .key("yard-scripts/test-update-job.py")
        .send()
        .await
        .expect("Failed to get object");

    let body = resp
        .body
        .collect()
        .await
        .expect("Failed to read body")
        .into_bytes();

    let content = String::from_utf8(body.to_vec()).expect("Invalid UTF-8");
    assert!(content.contains("# updated"), "Script should contain update marker");

    // Cleanup
    provider
        .destroy("test-update-job", &resources2)
        .await
        .expect("Destroy failed");
}

// ---- Destroy ----

#[tokio::test]
#[ignore]
async fn glue_destroy_cleans_up() {

    let s3 = s3_client().await;
    let glue = glue_client().await;
    ensure_bucket(&s3).await;

    let provider = get_provider(JobType::Glue, &test_provider_config())
        .await
        .expect("Failed to create provider");

    let resources = provider
        .deploy("test-destroy-job", TEST_SCRIPT, &test_job_config())
        .await
        .expect("Deploy failed");

    // Destroy
    provider
        .destroy("test-destroy-job", &resources)
        .await
        .expect("Destroy failed");

    // Verify Glue job is gone
    let result = glue
        .get_job()
        .job_name("test-destroy-job")
        .send()
        .await;

    assert!(
        result.is_err(),
        "Glue job should not exist after destroy"
    );

    // Verify S3 script is gone
    let result = s3
        .get_object()
        .bucket(TEST_BUCKET)
        .key("yard-scripts/test-destroy-job.py")
        .send()
        .await;

    assert!(
        result.is_err(),
        "S3 script should not exist after destroy"
    );
}

// ---- Provider Config Validation ----

#[tokio::test]
#[ignore]
async fn glue_missing_role_errors() {

    let s3 = s3_client().await;
    ensure_bucket(&s3).await;

    let provider = get_provider(JobType::Glue, &test_provider_config())
        .await
        .expect("Failed to create provider");

    // Job config without a role
    let bad_config = json!({ "type": "glue" });

    let result = provider
        .deploy("test-no-role", TEST_SCRIPT, &bad_config)
        .await;

    assert!(result.is_err(), "Deploy should fail without a role");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("role"),
        "Error should mention role, got: {err}"
    );
}

#[tokio::test]
#[ignore]
async fn glue_missing_script_bucket_errors() {

    let config = json!({
        "region": REGION,
        // no script_bucket
    });

    let result = get_provider(JobType::Glue, &config).await;
    let err = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("Provider creation should fail without script_bucket"),
    };
    assert!(
        err.contains("script_bucket"),
        "Error should mention script_bucket, got: {err}"
    );
}

// ---- Provider Config Options ----

#[tokio::test]
#[ignore]
async fn glue_deploy_with_full_config() {

    let s3 = s3_client().await;
    let glue = glue_client().await;
    ensure_bucket(&s3).await;

    let config = json!({
        "region": REGION,
        "script_bucket": TEST_BUCKET,
        "script_prefix": "yard-scripts/",
        "glue_version": "4.0",
        "worker_type": "G.2X",
        "number_of_workers": 4,
        "timeout": 120,
        "max_retries": 3,
        "bookmark": "enabled",
    });

    let provider = get_provider(JobType::Glue, &config)
        .await
        .expect("Failed to create provider");

    let job_config = json!({
        "type": "glue",
        "role": "arn:aws:iam::123456789012:role/TestGlueRole",
    });

    let resources = provider
        .deploy("test-full-config", TEST_SCRIPT, &job_config)
        .await
        .expect("Deploy failed");

    // Verify job config was applied
    let job = glue
        .get_job()
        .job_name("test-full-config")
        .send()
        .await
        .expect("Failed to get Glue job");

    let job = job.job().expect("No job in response");
    assert_eq!(job.max_retries(), 3);
    assert_eq!(job.timeout(), Some(120));
    assert_eq!(job.number_of_workers(), Some(4));

    // Check bookmark in default arguments
    if let Some(args) = job.default_arguments() {
        assert_eq!(
            args.get("--job-bookmark-option"),
            Some(&"job-bookmark-enable".to_string()),
            "Bookmark should be enabled in default arguments"
        );
    }

    // Cleanup
    provider
        .destroy("test-full-config", &resources)
        .await
        .expect("Destroy failed");
}
