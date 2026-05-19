//! Shared test helpers for yard-server E2E and integration tests.
//!
//! This module is declared as `#[cfg(test)] mod test_support;` in main.rs,
//! so it is only compiled during test runs. All items are `pub(crate)` to be
//! importable from any test module within the crate (e.g., router.rs tests,
//! github/e2e_tests.rs).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::api::dashboard::ApiState;
use crate::api::events::new_event_channel;
use crate::db::Database;
use crate::github::client::test_support::InMemoryGitHubApi;
use crate::github::client::GitHubApi;
use crate::github::router::AppState;
use crate::secrets::test_support::InMemorySecretStore;
use crate::secrets::SecretStore;
use axum::body::Body;
use axum::http::{header, Method, Request};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

/// Process-unique counter for temp directory names, preventing collisions
/// across concurrent tests.
static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Hand-rolled scoped temp dir -- `tempfile` is NOT a yard-server dep.
/// Mirrors `yard-core/tests/common/mod.rs:24-51` line-for-line.
pub(crate) struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub(crate) fn new() -> Self {
        let n = FIXTURE_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir()
            .join(format!("yard_e2e_fixture_{}_{}", std::process::id(), n));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Build a fixture git repo with per-environment structure:
/// `<root>/production/us-east-1/jobs/example/config.yaml`
/// Returns the tempdir and the HEAD SHA.
pub(crate) fn build_fixture_repo() -> (TempDir, String) {
    let tmp = TempDir::new();
    let dir = tmp.path();

    // Include a unique nonce in yard.yaml so that concurrent calls produce
    // distinct git SHAs -- clone_at_sha uses `yard-{sha}` as the workdir
    // name, so identical SHAs from parallel tests cause directory collisions.
    let nonce = FIXTURE_COUNTER.fetch_add(1, Ordering::SeqCst);
    fs::write(
        dir.join("yard.yaml"),
        format!(
            "project: yard-fixture\nstate:\n  type: local\n  path: .yard/state/\n# nonce: {nonce}\n"
        ),
    )
    .unwrap();

    // Per-env structure for discover_environments
    let env_dir = dir.join("production");
    fs::create_dir_all(&env_dir).unwrap();
    fs::write(env_dir.join("account.yaml"), "account_id: \"000000000000\"\n").unwrap();

    let region_dir = env_dir.join("us-east-1");
    fs::create_dir_all(&region_dir).unwrap();
    fs::write(region_dir.join("region.yaml"), "region: us-east-1\n").unwrap();

    let example_dir = region_dir.join("jobs").join("example");
    fs::create_dir_all(&example_dir).unwrap();
    fs::write(
        example_dir.join("config.yaml"),
        "type: glue\nrole: arn:aws:iam::000000000000:role/yard-fixture\n",
    )
    .unwrap();

    let init_out = std::process::Command::new("git")
        .arg("init")
        .current_dir(dir)
        .output()
        .unwrap();
    if !init_out.status.success() {
        panic!(
            "git init failed in fixture builder:\n{}",
            String::from_utf8_lossy(&init_out.stderr)
        );
    }

    let add_out = std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .unwrap();
    if !add_out.status.success() {
        panic!(
            "git add failed in fixture builder:\n{}",
            String::from_utf8_lossy(&add_out.stderr)
        );
    }

    let commit_out = std::process::Command::new("git")
        .args([
            "-c", "user.email=test@test",
            "-c", "user.name=test",
            "commit", "-m", "fixture",
        ])
        .current_dir(dir)
        .output()
        .unwrap();
    if !commit_out.status.success() {
        panic!(
            "git commit failed in fixture builder:\n{}",
            String::from_utf8_lossy(&commit_out.stderr)
        );
    }

    let head_out = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(dir)
        .output()
        .unwrap();
    if !head_out.status.success() {
        panic!(
            "git rev-parse HEAD failed in fixture builder:\n{}",
            String::from_utf8_lossy(&head_out.stderr)
        );
    }
    let head_sha = String::from_utf8(head_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    (tmp, head_sha)
}

/// Build a multi-environment fixture git repo with 3 environments:
/// - `production/us-east-1/jobs/etl-job/config.yaml`
/// - `production/eu-west-1/jobs/etl-job/config.yaml`
/// - `staging/us-east-1/jobs/etl-job/config.yaml`
///
/// Returns the tempdir and the HEAD SHA.
pub(crate) fn build_multi_env_fixture_repo() -> (TempDir, String) {
    let tmp = TempDir::new();
    let dir = tmp.path();

    let nonce = FIXTURE_COUNTER.fetch_add(1, Ordering::SeqCst);
    fs::write(
        dir.join("yard.yaml"),
        format!(
            "project: yard-multi-env\nstate:\n  type: local\n  path: .yard/state/\n# nonce: {nonce}\n"
        ),
    )
    .unwrap();

    // -- production environment (2 regions) --
    let prod_dir = dir.join("production");
    fs::create_dir_all(&prod_dir).unwrap();
    fs::write(
        prod_dir.join("account.yaml"),
        "account_id: \"111111111111\"\n",
    )
    .unwrap();

    // production/us-east-1
    let prod_use1 = prod_dir.join("us-east-1");
    fs::create_dir_all(&prod_use1).unwrap();
    fs::write(prod_use1.join("region.yaml"), "region: us-east-1\n").unwrap();
    let prod_use1_job = prod_use1.join("jobs").join("etl-job");
    fs::create_dir_all(&prod_use1_job).unwrap();
    fs::write(
        prod_use1_job.join("config.yaml"),
        "type: glue\nrole: arn:aws:iam::111111111111:role/yard-etl\n",
    )
    .unwrap();

    // production/eu-west-1
    let prod_euw1 = prod_dir.join("eu-west-1");
    fs::create_dir_all(&prod_euw1).unwrap();
    fs::write(prod_euw1.join("region.yaml"), "region: eu-west-1\n").unwrap();
    let prod_euw1_job = prod_euw1.join("jobs").join("etl-job");
    fs::create_dir_all(&prod_euw1_job).unwrap();
    fs::write(
        prod_euw1_job.join("config.yaml"),
        "type: glue\nrole: arn:aws:iam::111111111111:role/yard-etl\n",
    )
    .unwrap();

    // -- staging environment --
    let staging_dir = dir.join("staging");
    fs::create_dir_all(&staging_dir).unwrap();
    fs::write(
        staging_dir.join("account.yaml"),
        "account_id: \"222222222222\"\n",
    )
    .unwrap();

    let staging_use1 = staging_dir.join("us-east-1");
    fs::create_dir_all(&staging_use1).unwrap();
    fs::write(staging_use1.join("region.yaml"), "region: us-east-1\n").unwrap();
    let staging_use1_job = staging_use1.join("jobs").join("etl-job");
    fs::create_dir_all(&staging_use1_job).unwrap();
    fs::write(
        staging_use1_job.join("config.yaml"),
        "type: glue\nrole: arn:aws:iam::222222222222:role/yard-etl\n",
    )
    .unwrap();

    let init_out = std::process::Command::new("git")
        .arg("init")
        .current_dir(dir)
        .output()
        .unwrap();
    if !init_out.status.success() {
        panic!(
            "git init failed in multi-env fixture builder:\n{}",
            String::from_utf8_lossy(&init_out.stderr)
        );
    }

    let add_out = std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(dir)
        .output()
        .unwrap();
    if !add_out.status.success() {
        panic!(
            "git add failed in multi-env fixture builder:\n{}",
            String::from_utf8_lossy(&add_out.stderr)
        );
    }

    let commit_out = std::process::Command::new("git")
        .args([
            "-c", "user.email=test@test",
            "-c", "user.name=test",
            "commit", "-m", "multi-env fixture",
        ])
        .current_dir(dir)
        .output()
        .unwrap();
    if !commit_out.status.success() {
        panic!(
            "git commit failed in multi-env fixture builder:\n{}",
            String::from_utf8_lossy(&commit_out.stderr)
        );
    }

    let head_out = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(dir)
        .output()
        .unwrap();
    if !head_out.status.success() {
        panic!(
            "git rev-parse HEAD failed in multi-env fixture builder:\n{}",
            String::from_utf8_lossy(&head_out.stderr)
        );
    }
    let head_sha = String::from_utf8(head_out.stdout)
        .unwrap()
        .trim()
        .to_string();

    (tmp, head_sha)
}

/// HMAC-SHA256-sign a webhook body, producing a `sha256=<hex>` string
/// suitable for the `X-Hub-Signature-256` header.
pub(crate) fn sign_webhook(secret: &str, payload: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(payload);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// Build a complete `AppState` wired with in-memory test doubles.
///
/// Uses hardcoded test values: `github_token="test-token"`,
/// `repo_owner="yard-test-owner"`, `repo_name="yard-test-repo"`,
/// empty `InMemorySecretStore`, `dashboard_url: None`.
pub(crate) fn build_test_state(
    mock_gh: Arc<InMemoryGitHubApi>,
    db: Arc<dyn Database>,
    webhook_secret: &str,
) -> Arc<AppState> {
    let (event_tx, _event_rx) = new_event_channel();
    let secret_store: Arc<dyn SecretStore> =
        Arc::new(InMemorySecretStore::new(HashMap::new()));
    let api_state = Arc::new(ApiState {
        github_token: "test-token".to_string(),
        repo_owner: "yard-test-owner".to_string(),
        repo_name: "yard-test-repo".to_string(),
        db: db.clone(),
        event_tx,
        secret_store,
    });

    Arc::new(AppState {
        github_client: mock_gh as Arc<dyn GitHubApi>,
        webhook_secret: webhook_secret.to_string(),
        db,
        api_state,
        dashboard_url: None,
    })
}

/// Build a fully-signed HTTP POST request for `/api/webhook/github`.
///
/// Computes the HMAC-SHA256 signature and sets the required GitHub webhook
/// headers (`X-GitHub-Event`, `X-Hub-Signature-256`, `Content-Type`).
pub(crate) fn build_webhook_request(
    event_type: &str,
    payload: &serde_json::Value,
    webhook_secret: &str,
) -> Request<Body> {
    let payload_bytes = serde_json::to_vec(payload).unwrap();
    let sig = sign_webhook(webhook_secret, &payload_bytes);

    Request::builder()
        .method(Method::POST)
        .uri("/api/webhook/github")
        .header("X-GitHub-Event", event_type)
        .header("X-Hub-Signature-256", &sig)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload_bytes))
        .unwrap()
}
