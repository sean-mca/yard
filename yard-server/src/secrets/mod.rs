//! Secret resolution abstraction (SRV-02).
//!
//! `SecretStore::resolve` takes a Secrets Manager ARN (or any string the
//! backend understands) and returns the plaintext secret string. The two
//! impls are:
//!   - `AwsSecretStore` — production; calls `secretsmanager:GetSecretValue`.
//!   - `InMemorySecretStore` (cfg(test) `test_support`) — wraps a
//!     `HashMap<String, String>` for unit / integration tests.
//!
//! Mirror of the `Database` trait + `InMemoryDb` shape in
//! `yard-server/src/db/mod.rs:75-232` (D-14).

use async_trait::async_trait;

#[async_trait]
pub trait SecretStore: Send + Sync {
    /// Resolve the given Secrets Manager ARN to its plaintext secret value.
    /// Returns Err on AWS error, network error, or missing/binary secret.
    async fn resolve(&self, secret_arn: &str) -> anyhow::Result<String>;
}

pub struct AwsSecretStore {
    client: aws_sdk_secretsmanager::Client,
}

impl AwsSecretStore {
    pub fn new(sdk_config: &aws_config::SdkConfig) -> Self {
        Self {
            client: aws_sdk_secretsmanager::Client::new(sdk_config),
        }
    }
}

#[async_trait]
impl SecretStore for AwsSecretStore {
    async fn resolve(&self, secret_arn: &str) -> anyhow::Result<String> {
        // The ARN is intentionally omitted from these error messages — callers
        // log it as a structured `arn = %arn` field, so embedding it here
        // would only produce duplicate ARN entries in every alert-resolve
        // failure log line. Keep the ARN as a structural caller concern;
        // keep the error string focused on what failed.
        let out = self
            .client
            .get_secret_value()
            .secret_id(secret_arn)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Secrets Manager GetSecretValue failed: {e}"))?;

        out.secret_string()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Secrets Manager secret has no string value (binary secrets are not supported)"
                )
            })
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;
    use std::collections::HashMap;

    pub struct InMemorySecretStore {
        entries: HashMap<String, String>,
    }

    impl InMemorySecretStore {
        pub fn new(entries: HashMap<String, String>) -> Self {
            Self { entries }
        }
    }

    #[async_trait]
    impl SecretStore for InMemorySecretStore {
        async fn resolve(&self, secret_arn: &str) -> anyhow::Result<String> {
            self.entries
                .get(secret_arn)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no test mapping for {secret_arn}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::InMemorySecretStore;
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn in_memory_resolve_returns_value_for_known_arn() {
        let mut entries = HashMap::new();
        entries.insert(
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:yard/slack-AbCdEf".to_string(),
            "https://hooks.slack.com/services/T000/B000/abc123".to_string(),
        );
        let store = InMemorySecretStore::new(entries);

        let value = store
            .resolve("arn:aws:secretsmanager:us-east-1:111111111111:secret:yard/slack-AbCdEf")
            .await
            .unwrap();
        assert_eq!(value, "https://hooks.slack.com/services/T000/B000/abc123");
    }

    #[tokio::test]
    async fn in_memory_resolve_returns_err_for_unknown_arn() {
        let store = InMemorySecretStore::new(HashMap::new());
        let err = store.resolve("arn:aws:secretsmanager:us-east-1:000:missing").await;
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(
            msg.contains("missing"),
            "error should reference the missing ARN: {msg}"
        );
    }

    // No live-AWS test for AwsSecretStore (compile-checked only). The integration
    // test that exercises the resolve→post_slack_alert wire is in alerting/slack.rs
    // via the InMemorySecretStore + tokio::net::TcpListener fake Slack responder.
    #[test]
    fn aws_secret_store_compiles_against_sdk_config() {
        // Type-only assertion: AwsSecretStore::new takes a &SdkConfig.
        // Doesn't construct one (no I/O in tests).
        fn _accepts_sdk_config(cfg: &aws_config::SdkConfig) -> AwsSecretStore {
            AwsSecretStore::new(cfg)
        }
        let _ = _accepts_sdk_config;
    }
}
