//! E2E integration tests for the yard-server chatOps lifecycle.
//!
//! Tests in this module exercise the full webhook -> plan -> comment flow
//! using `tower::ServiceExt::oneshot` with in-memory test doubles.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::db::test_support::InMemoryDb;
use crate::db::Database;
use crate::github::client::test_support::InMemoryGitHubApi;
use crate::github::client::PrComment;
use crate::github::router::github_router;
use crate::test_support::{
    build_fixture_repo, build_multi_env_fixture_repo, build_test_state, build_webhook_request,
};
use axum::http::StatusCode;
use std::sync::Arc;
use tower::ServiceExt;

/// E2E-01: Full apply happy-path.
///
/// Plan webhook posts a plan comment, apply webhook dispatches apply,
/// and the result comment is posted (with expected failure since there is
/// no real AWS environment).
#[tokio::test]
async fn test_apply_happy_path() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (fixture, head_sha) = build_fixture_repo();
    let clone_url = fixture.path().to_string_lossy().to_string();

    let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
    let mock_gh = Arc::new(InMemoryGitHubApi::new());

    // Seed changed_files so filter_affected_environments finds "production"
    {
        let mut files = mock_gh.changed_files.lock().await;
        files.push("production/us-east-1/jobs/example/config.yaml".to_string());
    }
    // Set head_sha to match the fixture repo's actual HEAD (prevents stale-plan rejection)
    {
        let mut sha = mock_gh.head_sha.lock().await;
        *sha = head_sha.clone();
    }

    let webhook_state = build_test_state(mock_gh.clone(), db.clone(), "test-webhook-secret");

    // Step 1: Plan webhook (pull_request opened)
    let plan_payload = serde_json::json!({
        "action": "opened",
        "number": 42,
        "pull_request": {
            "head": { "ref": "feature", "sha": head_sha },
            "base": { "ref": "main", "sha": "0000000" }
        },
        "repository": {
            "full_name": "yard-test-owner/yard-test-repo",
            "clone_url": clone_url
        }
    });

    let response = github_router(webhook_state.clone())
        .oneshot(build_webhook_request(
            "pull_request",
            &plan_payload,
            "test-webhook-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify plan comment was posted via raw_posts
    {
        let raw_posts = mock_gh.raw_posts.lock().await;
        assert_eq!(
            raw_posts.len(),
            1,
            "expected exactly one plan comment; got {}",
            raw_posts.len()
        );
        assert!(
            raw_posts[0].body.starts_with("<!-- yard-plan-comment -->"),
            "missing plan comment marker"
        );
    }

    // Step 2: Apply webhook (issue_comment "yard apply")
    let apply_payload = serde_json::json!({
        "action": "created",
        "comment": { "body": "yard apply" },
        "issue": {
            "number": 42,
            "pull_request": { "url": "https://api.github.com/pulls/42" }
        },
        "repository": {
            "full_name": "yard-test-owner/yard-test-repo",
            "clone_url": clone_url
        }
    });

    let response = github_router(webhook_state.clone())
        .oneshot(build_webhook_request(
            "issue_comment",
            &apply_payload,
            "test-webhook-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Step 3: Verify apply result comment was posted
    // The apply handler posts via post_comment (CommentMode::Apply) which goes to mock_gh.posts
    let posts = mock_gh.posts.lock().await;
    assert!(
        !posts.is_empty(),
        "expected at least one apply result comment in posts"
    );
    assert!(
        posts.iter().any(|p| p.body.contains("yard apply")),
        "expected apply result body to contain 'yard apply'; got: {:?}",
        posts.iter().map(|p| &p.body).collect::<Vec<_>>()
    );
}

/// E2E-02: Comment upsert via two-webhook sequence.
///
/// First plan webhook creates a new comment, second plan webhook (re-trigger
/// via "yard plan" comment) finds and updates the existing comment.
#[tokio::test]
async fn test_comment_upsert_two_webhook_sequence() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (fixture, head_sha) = build_fixture_repo();
    let clone_url = fixture.path().to_string_lossy().to_string();

    let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
    let mock_gh = Arc::new(InMemoryGitHubApi::new());

    // Seed changed_files
    {
        let mut files = mock_gh.changed_files.lock().await;
        files.push("production/us-east-1/jobs/example/config.yaml".to_string());
    }
    // Set head_sha to match fixture
    {
        let mut sha = mock_gh.head_sha.lock().await;
        *sha = head_sha.clone();
    }

    let webhook_state = build_test_state(mock_gh.clone(), db.clone(), "test-webhook-secret");

    // Step 1: First plan webhook (pull_request opened)
    let plan_payload = serde_json::json!({
        "action": "opened",
        "number": 42,
        "pull_request": {
            "head": { "ref": "feature", "sha": head_sha },
            "base": { "ref": "main", "sha": "0000000" }
        },
        "repository": {
            "full_name": "yard-test-owner/yard-test-repo",
            "clone_url": clone_url
        }
    });

    let response = github_router(webhook_state.clone())
        .oneshot(build_webhook_request(
            "pull_request",
            &plan_payload,
            "test-webhook-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Capture first comment body
    let first_comment_body = {
        let raw_posts = mock_gh.raw_posts.lock().await;
        assert_eq!(
            raw_posts.len(),
            1,
            "expected exactly one plan comment after first webhook"
        );
        raw_posts[0].body.clone()
    };

    // Step 2: Seed comments for upsert -- simulates GitHub state after first comment
    {
        let mut comments = mock_gh.comments.lock().await;
        comments.push(PrComment {
            id: 100,
            body: first_comment_body,
        });
    }

    // Step 3: Second plan webhook (issue_comment "yard plan" re-trigger)
    let replan_payload = serde_json::json!({
        "action": "created",
        "comment": { "body": "yard plan" },
        "issue": {
            "number": 42,
            "pull_request": { "url": "https://api.github.com/pulls/42" }
        },
        "repository": {
            "full_name": "yard-test-owner/yard-test-repo",
            "clone_url": clone_url
        }
    });

    let response = github_router(webhook_state.clone())
        .oneshot(build_webhook_request(
            "issue_comment",
            &replan_payload,
            "test-webhook-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Step 4: Assert upsert -- the second plan should have updated the existing comment
    let updated = mock_gh.updated_comments.lock().await;
    assert_eq!(
        updated.len(),
        1,
        "expected exactly one updated comment; got {}",
        updated.len()
    );
    assert_eq!(
        updated[0].0, 100,
        "expected updated comment ID 100; got {}",
        updated[0].0
    );
    assert!(
        updated[0].1.contains("<!-- yard-plan-comment -->"),
        "updated comment body should contain plan marker"
    );
}

/// E2E-03: Multi-environment plan with collapsible sections.
///
/// A 3-environment fixture produces per-environment collapsible `<details>`
/// sections in the plan comment.
#[tokio::test]
async fn test_multi_env_plan_collapsible_sections() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (fixture, head_sha) = build_multi_env_fixture_repo();
    let clone_url = fixture.path().to_string_lossy().to_string();

    let db: Arc<dyn Database> = Arc::new(InMemoryDb::new());
    let mock_gh = Arc::new(InMemoryGitHubApi::new());

    // Seed changed_files covering all 3 environments
    {
        let mut files = mock_gh.changed_files.lock().await;
        files.push("production/us-east-1/jobs/etl-job/config.yaml".to_string());
        files.push("production/eu-west-1/jobs/etl-job/config.yaml".to_string());
        files.push("staging/us-east-1/jobs/etl-job/config.yaml".to_string());
    }
    // Set head_sha to match the multi-env fixture repo
    {
        let mut sha = mock_gh.head_sha.lock().await;
        *sha = head_sha.clone();
    }

    let webhook_state = build_test_state(mock_gh.clone(), db.clone(), "test-webhook-secret");

    // Step 1: Plan webhook
    let payload = serde_json::json!({
        "action": "opened",
        "number": 99,
        "pull_request": {
            "head": { "ref": "multi-env-feature", "sha": head_sha },
            "base": { "ref": "main", "sha": "0000000" }
        },
        "repository": {
            "full_name": "yard-test-owner/yard-test-repo",
            "clone_url": clone_url
        }
    });

    let response = github_router(webhook_state)
        .oneshot(build_webhook_request(
            "pull_request",
            &payload,
            "test-webhook-secret",
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Step 2: Assert per-env collapsible sections
    let raw_posts = mock_gh.raw_posts.lock().await;
    assert_eq!(
        raw_posts.len(),
        1,
        "expected exactly one plan comment; got {}",
        raw_posts.len()
    );

    let body = &raw_posts[0].body;

    assert!(
        body.contains("<!-- yard-plan-comment -->"),
        "missing plan comment marker; body was:\n{body}"
    );
    assert!(
        body.contains("<details>"),
        "missing collapsible <details> wrapper; body was:\n{body}"
    );
    assert!(
        body.contains("production"),
        "missing 'production' environment name; body was:\n{body}"
    );
    assert!(
        body.contains("staging"),
        "missing 'staging' environment name; body was:\n{body}"
    );
    // Both environments should have their own collapsible sections
    let details_count = body.matches("<details>").count();
    assert!(
        details_count >= 2,
        "expected at least 2 <details> sections (one per environment); got {details_count}; body was:\n{body}"
    );
    // Verify the plan header with SHA is present
    assert!(
        body.contains("### yard plan (SHA:"),
        "missing plan header; body was:\n{body}"
    );
}
