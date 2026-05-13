//! Per-account credential management for yard-server.
//!
//! Provides the `CredentialProvider` trait (D-10) with:
//! - `StsCredentialProvider`: production implementation using AWS SDK `AssumeRoleProvider`
//!   with per-role-ARN caching (D-08). The server's own AWS identity comes from the
//!   SDK default credential chain (D-09).
//! - `MockCredentialProvider` (cfg(test) only): configurable success/failure per account
//!   for unit testing.
//!
//! Security notes:
//! - Cache key is role_arn (not account_id) to prevent credential confusion (T-40-04).
//! - Error messages include account_id and role_arn but never credential values (T-40-06).
//! - SDK's built-in LazyCache handles credential refresh automatically (T-40-05).

use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::RwLock;

/// Trait for obtaining per-account AWS configurations with assumed-role credentials.
///
/// Implementations must be `Send + Sync` for use in async contexts.
/// The returned `SdkConfig` can be used to construct any AWS service client
/// (e.g., `aws_sdk_s3::Client`, `aws_sdk_glue::Client`) scoped to the
/// target account.
#[allow(dead_code)]
#[async_trait]
pub trait CredentialProvider: Send + Sync {
    /// Obtain an AWS `SdkConfig` for the given account, assuming the specified role.
    ///
    /// # Arguments
    /// * `account_id` - The AWS account ID (for logging/health tracking, not used as cache key)
    /// * `role_arn` - The IAM role ARN to assume (used as cache key per T-40-04)
    ///
    /// # Errors
    /// Returns an error if the role cannot be assumed or credentials cannot be resolved.
    /// Error messages include account_id and role_arn but never credential values.
    async fn get_credentials(
        &self,
        account_id: &str,
        role_arn: &str,
    ) -> anyhow::Result<aws_config::SdkConfig>;
}

/// Production credential provider using AWS STS AssumeRole with SDK-managed caching.
///
/// Creates one `AssumeRoleProvider`-backed `SdkConfig` per unique role ARN.
/// The SDK's `LazyCache` handles credential refresh automatically (D-08).
/// The base AWS identity comes from the SDK default credential chain (D-09).
#[allow(dead_code)]
pub struct StsCredentialProvider {
    /// Base config from default credential chain (ECS task role / instance profile / env vars).
    base_config: aws_config::SdkConfig,
    /// Cached per-role-ARN configs. Key = role_arn (NOT account_id) per T-40-04.
    providers: RwLock<HashMap<String, aws_config::SdkConfig>>,
}

#[allow(dead_code)]
impl StsCredentialProvider {
    /// Create a new provider, loading the base AWS config from the default credential chain.
    pub async fn new() -> Self {
        let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        Self {
            base_config,
            providers: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a cached `SdkConfig` for the given role ARN.
    ///
    /// On cache miss, builds an `AssumeRoleProvider` with session name "yard-server"
    /// and configures it against the base config. The resulting `SdkConfig` is cached
    /// keyed by role ARN.
    async fn get_or_create_config(&self, role_arn: &str) -> aws_config::SdkConfig {
        // Check cache (read lock -- fast path)
        {
            let cache = self.providers.read().await;
            if let Some(config) = cache.get(role_arn) {
                return config.clone();
            }
        }

        // Cache miss: create new config with AssumeRoleProvider (write lock)
        let provider = aws_config::sts::AssumeRoleProvider::builder(role_arn)
            .session_name("yard-server")
            .configure(&self.base_config)
            .build()
            .await;

        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .credentials_provider(provider)
            .load()
            .await;

        let mut cache = self.providers.write().await;
        // Double-check: another task may have populated while we waited for write lock
        if let Some(existing) = cache.get(role_arn) {
            return existing.clone();
        }
        cache.insert(role_arn.to_string(), config.clone());
        config
    }
}

#[async_trait]
impl CredentialProvider for StsCredentialProvider {
    async fn get_credentials(
        &self,
        _account_id: &str,
        role_arn: &str,
    ) -> anyhow::Result<aws_config::SdkConfig> {
        // T-40-06: error messages include account_id and role_arn but never credential values
        let config = self.get_or_create_config(role_arn).await;
        Ok(config)
    }
}

// ---- Test Support ----

#[cfg(test)]
pub mod test_support {
    use super::*;

    /// Mock credential provider for unit tests.
    ///
    /// Configurable per-account success/failure. Default constructor succeeds for all accounts.
    pub struct MockCredentialProvider {
        /// Map of account_id -> Result. None = succeed with a default config.
        results: HashMap<String, Result<(), String>>,
        /// Whether to succeed by default for accounts not in the results map.
        default_success: bool,
    }

    impl MockCredentialProvider {
        /// Create a mock that succeeds for any account.
        pub fn new() -> Self {
            Self {
                results: HashMap::new(),
                default_success: true,
            }
        }

        /// Create a mock with configured per-account results.
        ///
        /// Accounts not in the map will fail with "account not configured".
        pub fn with_results(results: HashMap<String, Result<(), String>>) -> Self {
            Self {
                results,
                default_success: false,
            }
        }
    }

    #[async_trait]
    impl CredentialProvider for MockCredentialProvider {
        async fn get_credentials(
            &self,
            account_id: &str,
            role_arn: &str,
        ) -> anyhow::Result<aws_config::SdkConfig> {
            match self.results.get(account_id) {
                Some(Ok(())) => {
                    // Return a test SdkConfig
                    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .no_credentials()
                        .load()
                        .await;
                    Ok(config)
                }
                Some(Err(msg)) => {
                    anyhow::bail!(
                        "credential resolution failed for account {account_id} \
                         (role_arn={role_arn}): {msg}"
                    )
                }
                None => {
                    if self.default_success {
                        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                            .no_credentials()
                            .load()
                            .await;
                        Ok(config)
                    } else {
                        anyhow::bail!(
                            "account {account_id} not configured in MockCredentialProvider"
                        )
                    }
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::test_support::MockCredentialProvider;
    use super::*;

    #[tokio::test]
    async fn test_mock_credential_provider_returns_success() {
        let mock = MockCredentialProvider::new();
        let result = mock
            .get_credentials("123456789012", "arn:aws:iam::123456789012:role/TestRole")
            .await;
        assert!(result.is_ok(), "default mock should succeed for any account");
    }

    #[tokio::test]
    async fn test_mock_credential_provider_configured_failure() {
        let mut results = HashMap::new();
        results.insert(
            "bad-account".to_string(),
            Err("access denied".to_string()),
        );
        results.insert("good-account".to_string(), Ok(()));

        let mock = MockCredentialProvider::with_results(results);

        let bad_result = mock
            .get_credentials("bad-account", "arn:aws:iam::bad-account:role/TestRole")
            .await;
        assert!(bad_result.is_err(), "bad-account should fail");
        let err_msg = bad_result.unwrap_err().to_string();
        assert!(
            err_msg.contains("bad-account"),
            "error should mention account_id: {err_msg}"
        );
        assert!(
            err_msg.contains("access denied"),
            "error should contain the configured message: {err_msg}"
        );

        let good_result = mock
            .get_credentials("good-account", "arn:aws:iam::good-account:role/TestRole")
            .await;
        assert!(
            good_result.is_ok(),
            "good-account should succeed"
        );
    }

    #[tokio::test]
    async fn test_mock_credential_provider_unconfigured_account_fails() {
        let results = HashMap::new();
        let mock = MockCredentialProvider::with_results(results);

        let result = mock
            .get_credentials("unknown", "arn:aws:iam::unknown:role/TestRole")
            .await;
        assert!(
            result.is_err(),
            "with_results mock should fail for unconfigured accounts"
        );
    }
}
