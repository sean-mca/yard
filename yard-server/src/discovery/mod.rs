//! Environment discovery orchestrator.
//!
//! Wires together git clone, `discover_environments()`, database persistence,
//! and per-account credential health checks.
//!
//! Key invariants:
//! - Discovery (directory walk + persistence) MUST NOT be blocked by credential
//!   failures (T-40-10, Pitfall 1). The credential health check loop runs AFTER
//!   all environments are persisted.
//! - An unreachable account logs a warning but does not prevent other environments
//!   from being discovered.
//! - Fresh installation token generated per `clone_and_discover` call (T-40-09).

pub mod credentials;
pub mod repo;

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;

use crate::db::{
    AccountHealth, Database, Environment, JobSummaryEntity, RegionEntity,
};
use credentials::CredentialProvider;

/// Run environment discovery on a cloned repo directory.
///
/// 1. Calls `discover_environments()` from yard-core.
/// 2. Persists all environments, regions, and job summaries to the database.
/// 3. Checks credential health per unique account (AFTER persistence).
///
/// # Arguments
/// * `db` - Database handle for persistence
/// * `credential_provider` - Provider for checking per-account credential health
/// * `repo_path` - Path to the cloned repo (must contain yard.yaml)
///
/// # Error Handling
/// - Discovery errors (missing yard.yaml, malformed config) are propagated.
/// - Credential failures are logged as warnings but do NOT block discovery.
#[allow(dead_code)]
pub async fn run_discovery(
    db: &dyn Database,
    credential_provider: &dyn CredentialProvider,
    repo_path: &Path,
) -> Result<()> {
    // Phase 1: Discover environments from the repo directory tree.
    let environments = yard_core::resolve::discover_environments(repo_path)
        .context("failed to discover environments from repo")?;

    let env_count = environments.len();

    // Phase 2: Persist all environments, regions, and job summaries.
    // This completes BEFORE credential checks (T-40-10).
    for discovered_env in &environments {
        // Convert to db::Environment
        let region_names: Vec<String> = discovered_env
            .regions
            .iter()
            .map(|r| r.name.clone())
            .collect();

        let total_jobs: u64 = discovered_env
            .regions
            .iter()
            .map(|r| r.job_count)
            .sum();

        let env = Environment {
            name: discovered_env.name.clone(),
            regions: region_names,
            job_count: total_jobs,
            last_scanned: Utc::now(),
        };

        db.upsert_environment(&env)
            .await
            .with_context(|| format!("failed to upsert environment: {}", discovered_env.name))?;

        // Persist regions and jobs within each environment
        for region in &discovered_env.regions {
            let region_entity = RegionEntity {
                env_name: discovered_env.name.clone(),
                name: region.name.clone(),
                job_count: region.job_count,
                dag_count: region.dag_count,
            };

            db.upsert_region(&discovered_env.name, &region_entity)
                .await
                .with_context(|| {
                    format!(
                        "failed to upsert region {}/{}", discovered_env.name, region.name
                    )
                })?;

            for job in &region.jobs {
                let job_entity = JobSummaryEntity {
                    env_name: discovered_env.name.clone(),
                    region_name: region.name.clone(),
                    name: job.name.clone(),
                    job_type: job.job_type.to_string(),
                };

                db.upsert_job_summary(&discovered_env.name, &job_entity)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to upsert job summary {}/{}/{}",
                            discovered_env.name, region.name, job.name
                        )
                    })?;
            }
        }
    }

    // Phase 3: Check credential health per unique account.
    // Deduplicate by role_arn since multiple environments may share the same role.
    // This runs AFTER all environments are persisted (T-40-10).
    let mut checked_roles: HashMap<String, bool> = HashMap::new();
    let mut accounts_checked: usize = 0;

    for discovered_env in &environments {
        let account_id = match &discovered_env.account_id {
            Some(id) => id.clone(),
            None => continue,
        };

        match &discovered_env.role_arn {
            Some(role_arn) => {
                // Skip if we already checked this role_arn
                if checked_roles.contains_key(role_arn) {
                    continue;
                }
                accounts_checked += 1;

                match credential_provider
                    .get_credentials(&account_id, role_arn)
                    .await
                {
                    Ok(_) => {
                        checked_roles.insert(role_arn.clone(), true);
                        let health = AccountHealth {
                            account_id: account_id.clone(),
                            status: "healthy".to_string(),
                            last_checked: Utc::now(),
                            error_message: None,
                        };
                        if let Err(e) = db.set_account_health(&health).await {
                            tracing::warn!(
                                account_id = %account_id,
                                error = %e,
                                "failed to persist healthy account health status"
                            );
                        }
                    }
                    Err(e) => {
                        checked_roles.insert(role_arn.clone(), false);
                        // T-40-10: log warning but do NOT block other environments
                        tracing::warn!(
                            account_id = %account_id,
                            error = %e,
                            "credential resolution failed for account; marking as unreachable"
                        );
                        let health = AccountHealth {
                            account_id: account_id.clone(),
                            status: "unreachable".to_string(),
                            last_checked: Utc::now(),
                            error_message: Some(e.to_string()),
                        };
                        if let Err(e) = db.set_account_health(&health).await {
                            tracing::warn!(
                                account_id = %account_id,
                                error = %e,
                                "failed to persist unreachable account health status"
                            );
                        }
                    }
                }
            }
            None => {
                // No role configured — mark as degraded
                accounts_checked += 1;
                let health = AccountHealth {
                    account_id: account_id.clone(),
                    status: "degraded".to_string(),
                    last_checked: Utc::now(),
                    error_message: Some("no role_arn configured".to_string()),
                };
                if let Err(e) = db.set_account_health(&health).await {
                    tracing::warn!(
                        account_id = %account_id,
                        error = %e,
                        "failed to persist degraded account health status"
                    );
                }
            }
        }
    }

    tracing::info!(
        environments = env_count,
        accounts_checked = accounts_checked,
        "{env_count} environments discovered, {accounts_checked} accounts checked"
    );

    Ok(())
}

/// RAII guard that removes a directory on drop.
struct TempDirGuard(std::path::PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Clone a repository and run discovery.
///
/// Generates a fresh installation token (T-40-09), clones the repo into a
/// unique temp directory (D-03), then runs the full discovery + credential
/// health flow. The temp directory is cleaned up on return (success or error).
///
/// # Arguments
/// * `app_id` - GitHub App ID
/// * `private_key_pem` - PEM-encoded RSA private key for the App
/// * `installation_id` - GitHub App installation ID
/// * `repo_url` - HTTPS GitHub repo URL
/// * `db` - Database handle for persistence
/// * `credential_provider` - Provider for checking per-account credential health
#[allow(dead_code)]
pub async fn clone_and_discover(
    app_id: u64,
    private_key_pem: &str,
    installation_id: u64,
    repo_url: &str,
    db: &dyn Database,
    credential_provider: &dyn CredentialProvider,
) -> Result<()> {
    let temp_dir = std::env::temp_dir().join(format!(
        "yard-server-repo-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let _guard = TempDirGuard(temp_dir.clone());

    // T-40-09: fresh token per cycle
    let token = repo::generate_installation_token(app_id, private_key_pem, installation_id)
        .await
        .context("failed to generate installation token")?;

    // D-03: unique dir per cycle, cleaned up by _guard on drop
    repo::clone_repo(repo_url, &temp_dir, &token)
        .await
        .context("failed to clone repository")?;

    run_discovery(db, credential_provider, &temp_dir).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::db::test_support::InMemoryDb;
    use credentials::test_support::MockCredentialProvider;
    use std::fs;
    use std::path::PathBuf;

    /// Simple RAII temp directory for tests.
    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join("yard-server-test")
                .join(name)
                .join(format!("{}", std::process::id()));
            if path.exists() {
                fs::remove_dir_all(&path).ok();
            }
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.path).ok();
        }
    }

    /// Create a minimal yard project structure in a temp directory.
    /// Returns the temp dir handle (auto-cleaned on drop).
    #[allow(clippy::type_complexity)]
    fn create_test_project(
        test_name: &str,
        envs: &[(&str, Option<&str>, Option<&str>, &[(&str, &[&str])])],
    ) -> TestDir {
        let tmp = TestDir::new(test_name);
        let root = tmp.path();

        // Create yard.yaml at root
        fs::write(
            root.join("yard.yaml"),
            "project:\n  name: test-project\nstate:\n  type: local\n  path: ./state\n",
        )
        .unwrap();

        for (env_name, account_id, role_arn, regions) in envs {
            let env_dir = root.join(env_name);
            fs::create_dir_all(&env_dir).unwrap();

            // Create account.yaml
            let mut account_yaml = String::new();
            if let Some(acct_id) = account_id {
                account_yaml.push_str(&format!("account_id: \"{acct_id}\"\n"));
            }
            if let Some(role) = role_arn {
                account_yaml.push_str(&format!("aws:\n  assume_role: \"{role}\"\n"));
            }
            fs::write(env_dir.join("account.yaml"), &account_yaml).unwrap();

            for (region_name, jobs) in *regions {
                let region_dir = env_dir.join(region_name);
                fs::create_dir_all(&region_dir).unwrap();

                // Create region.yaml marker
                fs::write(
                    region_dir.join("region.yaml"),
                    format!("name: {region_name}\n"),
                )
                .unwrap();

                // Create job YAML files
                for job_name in *jobs {
                    fs::write(
                        region_dir.join(format!("{job_name}.yaml")),
                        "type: glue\nglue:\n  script_location: s3://bucket/script.py\n",
                    )
                    .unwrap();
                }
            }
        }

        tmp
    }

    #[tokio::test]
    async fn test_run_discovery_persists_environments() {
        let tmp = create_test_project("persist_envs", &[(
            "production",
            Some("123456789012"),
            Some("arn:aws:iam::123456789012:role/YardRole"),
            &[("us-east-1", &["etl-pipeline"])],
        )]);

        let db = InMemoryDb::new();
        let mock_creds = MockCredentialProvider::new();

        run_discovery(&db, &mock_creds, tmp.path()).await.unwrap();

        // Verify environment was persisted
        let envs = db.list_environments().await.unwrap();
        assert_eq!(envs.len(), 1, "expected 1 environment");
        assert_eq!(envs[0].name, "production");
        assert_eq!(envs[0].regions, vec!["us-east-1"]);
        assert_eq!(envs[0].job_count, 1);

        // Verify region was persisted
        let regions = db.list_regions("production").await.unwrap();
        assert_eq!(regions.len(), 1, "expected 1 region");
        assert_eq!(regions[0].name, "us-east-1");
        assert_eq!(regions[0].job_count, 1);

        // Verify account health is healthy
        let health = db.get_account_health("123456789012").await.unwrap();
        assert!(health.is_some(), "account health should be set");
        assert_eq!(health.unwrap().status, "healthy");
    }

    #[tokio::test]
    async fn test_run_discovery_unreachable_account_does_not_block() {
        let tmp = create_test_project("unreachable_nonblock", &[
            (
                "production",
                Some("111111111111"),
                Some("arn:aws:iam::111111111111:role/GoodRole"),
                &[("us-east-1", &["job-a"])],
            ),
            (
                "staging",
                Some("222222222222"),
                Some("arn:aws:iam::222222222222:role/BadRole"),
                &[("eu-west-1", &["job-b"])],
            ),
        ]);

        let mut results = std::collections::HashMap::new();
        results.insert("111111111111".to_string(), Ok(()));
        results.insert(
            "222222222222".to_string(),
            Err("access denied".to_string()),
        );
        let mock_creds = MockCredentialProvider::with_results(results);

        let db = InMemoryDb::new();

        run_discovery(&db, &mock_creds, tmp.path()).await.unwrap();

        // Both environments should be persisted regardless of credential health
        let envs = db.list_environments().await.unwrap();
        assert_eq!(envs.len(), 2, "both environments should be persisted");

        let env_names: Vec<&str> = envs.iter().map(|e| e.name.as_str()).collect();
        assert!(env_names.contains(&"production"), "production should exist");
        assert!(env_names.contains(&"staging"), "staging should exist");

        // Check credential health statuses
        let health_good = db
            .get_account_health("111111111111")
            .await
            .unwrap()
            .expect("good account health should be set");
        assert_eq!(health_good.status, "healthy");

        let health_bad = db
            .get_account_health("222222222222")
            .await
            .unwrap()
            .expect("bad account health should be set");
        assert_eq!(health_bad.status, "unreachable");
        assert!(
            health_bad.error_message.is_some(),
            "unreachable account should have error message"
        );
    }

    #[tokio::test]
    async fn test_run_discovery_no_role_arn_marks_degraded() {
        let tmp = create_test_project("degraded", &[(
            "dev",
            Some("333333333333"),
            None, // no role_arn
            &[("us-west-2", &[])],
        )]);

        let db = InMemoryDb::new();
        let mock_creds = MockCredentialProvider::new();

        run_discovery(&db, &mock_creds, tmp.path()).await.unwrap();

        let health = db
            .get_account_health("333333333333")
            .await
            .unwrap()
            .expect("degraded account health should be set");
        assert_eq!(health.status, "degraded");
    }
}
