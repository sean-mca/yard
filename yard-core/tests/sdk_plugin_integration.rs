#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Integration tests for the SDK-based test plugin.
//!
//! Exercises all 6 operations through the Phase 66 PluginSpawner host,
//! proving SDK-01 (all operations work) and SDK-02 (stdout protection
//! works under real process spawning).

use std::path::PathBuf;
use std::sync::OnceLock;

use serde_json::json;
use yard_core::plugin_host::{PluginHostConfig, PluginSpawner};

static SDK_PLUGIN_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Build the SDK test plugin binary once per test run and return its path.
fn build_sdk_plugin() -> PathBuf {
    SDK_PLUGIN_PATH
        .get_or_init(|| {
            let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("tests/test_plugin_sdk/Cargo.toml");

            let output = std::process::Command::new("cargo")
                .arg("build")
                .arg("--manifest-path")
                .arg(&manifest_path)
                .output()
                .expect("failed to run cargo build for SDK test plugin");

            assert!(
                output.status.success(),
                "SDK test plugin build failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );

            let binary = manifest_path
                .parent()
                .unwrap()
                .join("target/debug/test-plugin-sdk");
            assert!(
                binary.exists(),
                "SDK test plugin binary not found at {}",
                binary.display()
            );

            binary
        })
        .clone()
}

/// Create a PluginSpawner pointed at the SDK test binary.
fn make_sdk_spawner() -> PluginSpawner {
    let binary = build_sdk_plugin();
    let config = PluginHostConfig {
        plugins_dir: PathBuf::from("/tmp/yard-test-plugins-sdk"),
        timeout_secs: 5,
        lock_file_path: None,
    };
    PluginSpawner::new(binary, "test-plugin-sdk".to_string(), config)
}

// ---- SDK plugin operation tests ----

#[tokio::test]
async fn test_sdk_validate_operation() {
    let spawner = make_sdk_spawner();
    let result = spawner
        .call_validate("test-job", &json!({"type": "glue"}))
        .await;

    let errors = result.unwrap();
    assert!(errors.is_empty(), "expected empty errors, got: {errors:?}");
}

#[tokio::test]
async fn test_sdk_codegen_operation() {
    let spawner = make_sdk_spawner();
    let result = spawner
        .call_codegen("test-job", &json!({"type": "glue"}))
        .await;

    let script = result.unwrap();
    assert!(script.is_some(), "expected Some(script)");
    let content = script.unwrap();
    assert!(
        content.contains("generated-by-sdk-test"),
        "expected SDK test marker, got: {content}"
    );
}

#[tokio::test]
async fn test_sdk_deploy_operation() {
    let spawner = make_sdk_spawner();
    let result = spawner
        .call_deploy("test-job", "print('hello')", &json!({"type": "glue"}))
        .await;

    let resources = result.unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].r#type, "SdkTestResource");
    assert_eq!(resources[0].id, "sdk-test-123");
    assert_eq!(resources[0].provider, "test-plugin-sdk");
}

#[tokio::test]
async fn test_sdk_destroy_operation() {
    let spawner = make_sdk_spawner();
    let resources = vec![yard_structs::Resource {
        r#type: "SdkTestResource".to_string(),
        id: "sdk-test-123".to_string(),
        provider: "test-plugin-sdk".to_string(),
    }];

    let result = spawner.call_destroy("test-job", &resources).await;
    assert!(result.is_ok(), "destroy should succeed: {:?}", result.err());
}

#[tokio::test]
async fn test_sdk_verify_operation() {
    let spawner = make_sdk_spawner();
    let resources = vec![yard_structs::Resource {
        r#type: "SdkTestResource".to_string(),
        id: "sdk-test-123".to_string(),
        provider: "test-plugin-sdk".to_string(),
    }];

    let result = spawner.call_verify("test-job", &resources).await;
    let statuses = result.unwrap();
    assert_eq!(statuses.len(), 1);
    assert!(statuses[0].exists, "resource should exist");
    assert_eq!(statuses[0].resource.id, "sdk-test-123");
}

#[tokio::test]
async fn test_sdk_schema_operation() {
    let spawner = make_sdk_spawner();
    let result = spawner.call_schema().await;

    let fields = result.unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name, "region");
    assert_eq!(fields[0].field_type, "string");
    assert!(fields[0].required);
    assert_eq!(fields[0].description, "AWS region");
}

#[tokio::test]
async fn test_sdk_stdout_protection() {
    // The SDK test plugin calls println!() inside its deploy handler.
    // SDK-02 requires that stdout pollution does not corrupt the protocol.
    // If stdout protection is broken, the println output would appear on
    // the protocol channel and deserialization would fail.
    let spawner = make_sdk_spawner();
    let result = spawner
        .call_deploy("test-job", "print('hello')", &json!({"type": "glue"}))
        .await;

    // Success proves the println did not corrupt protocol JSON.
    let resources = result.unwrap();
    assert_eq!(resources.len(), 1, "deploy should return one resource");
    assert_eq!(resources[0].id, "sdk-test-123");
}
