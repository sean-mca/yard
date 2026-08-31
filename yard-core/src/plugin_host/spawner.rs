//! Plugin process lifecycle management.
//!
//! [`PluginSpawner`] handles spawning a plugin binary, validating the
//! JSON-over-stdio handshake, sending a [`PluginRequest`], reading
//! progress lines and the final response, enforcing a configurable
//! timeout, and verifying the binary's SHA-256 checksum against a lock
//! file before execution.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use yard_structs::{
    CodegenResponse, DeployResponse, DestroyResponse, HandshakeMessage, PluginOperation,
    PluginRequest, PluginValidationError, ProgressMessage, Resource, ResourceStatus, SchemaField,
    SchemaResponse, ValidateResponse, VerifyResponse, PROTOCOL_VERSION,
};

/// Runtime configuration for the plugin host.
///
/// Controls where plugin binaries are cached, how long operations may
/// run before being killed, and where the lock file lives.
#[derive(Debug, Clone)]
pub struct PluginHostConfig {
    /// Directory where plugin binaries are cached (default: `.yard/plugins/`).
    pub plugins_dir: PathBuf,
    /// Maximum seconds a single plugin operation may run before being
    /// killed (default: 300).
    pub timeout_secs: u64,
    /// Path to the lock file containing expected checksums. When `None`,
    /// defaults to `yard.lock` in the project root.
    pub lock_file_path: Option<PathBuf>,
}

impl Default for PluginHostConfig {
    fn default() -> Self {
        Self {
            plugins_dir: PathBuf::from(".yard/plugins"),
            timeout_secs: 300,
            lock_file_path: None,
        }
    }
}

/// Manages the lifecycle of a single plugin binary.
///
/// Each call to [`call_operation`](PluginSpawner::call_operation) spawns
/// a fresh child process (D-05: spawn-per-op), validates the handshake,
/// exchanges a single request/response, and enforces the configured
/// timeout.
#[derive(Debug, Clone)]
pub struct PluginSpawner {
    /// Absolute or relative path to the plugin binary.
    binary_path: PathBuf,
    /// Human-readable plugin name (used in error messages).
    plugin_name: String,
    /// Runtime configuration.
    config: PluginHostConfig,
}

impl PluginSpawner {
    /// Create a new spawner for the given plugin binary.
    pub fn new(binary_path: PathBuf, plugin_name: String, config: PluginHostConfig) -> Self {
        Self {
            binary_path,
            plugin_name,
            config,
        }
    }

    /// Core operation dispatcher -- spawns the plugin, validates the
    /// handshake, sends the request, collects progress, and returns the
    /// raw response JSON.
    ///
    /// The entire exchange is wrapped in a configurable timeout (HOST-03).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The binary checksum does not match the lock file (T-66-02)
    /// - The binary cannot be spawned
    /// - The handshake protocol version mismatches (D-02)
    /// - The requested operation is not in the plugin's capabilities (D-03)
    /// - The plugin returns an error response
    /// - The plugin process exits with a non-zero code (D-07)
    /// - The operation exceeds the configured timeout (HOST-03)
    pub async fn call_operation(
        &self,
        operation: PluginOperation,
        request: PluginRequest,
        progress_callback: impl Fn(ProgressMessage) + Send,
    ) -> Result<Value> {
        // Optionally verify binary checksum (D-17: skip if no lock file)
        if let Some(ref lock_path) = self.config.lock_file_path {
            if lock_path.exists() {
                verify_checksum(&self.binary_path, lock_path, &self.plugin_name)
                    .await
                    .with_context(|| {
                        format!(
                            "checksum verification failed for plugin '{}'",
                            self.plugin_name
                        )
                    })?;
            }
        }

        let timeout_duration =
            std::time::Duration::from_secs(self.config.timeout_secs);
        let plugin_name = self.plugin_name.clone();
        let binary_path = self.binary_path.clone();

        tokio::time::timeout(timeout_duration, async {
            self.run_exchange(operation, request, progress_callback)
                .await
        })
        .await
        .map_err(|_elapsed| {
            anyhow!(
                "plugin '{}' timed out after {}s during {:?} operation \
                 (increase timeout via plugin_timeout config)",
                plugin_name,
                timeout_duration.as_secs(),
                operation,
            )
        })?
        .with_context(|| {
            format!(
                "plugin '{}' (binary: {}) failed during {:?} operation",
                plugin_name,
                binary_path.display(),
                operation,
            )
        })
    }

    /// Internal: run the full spawn-handshake-request-response exchange
    /// without timeout wrapping.
    async fn run_exchange(
        &self,
        operation: PluginOperation,
        request: PluginRequest,
        progress_callback: impl Fn(ProgressMessage) + Send,
    ) -> Result<Value> {
        // Spawn the plugin binary (HOST-01)
        let mut child = Command::new(&self.binary_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .with_context(|| {
                format!(
                    "failed to spawn plugin binary '{}'",
                    self.binary_path.display()
                )
            })?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("failed to capture stdout from plugin '{}'", self.plugin_name))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("failed to capture stdin for plugin '{}'", self.plugin_name))?;

        let mut reader = BufReader::new(stdout).lines();

        // Read handshake line (PROTO-02, D-08)
        let handshake_line = reader
            .next_line()
            .await
            .with_context(|| {
                format!(
                    "failed to read handshake from plugin '{}'",
                    self.plugin_name
                )
            })?
            .ok_or_else(|| {
                anyhow!(
                    "plugin '{}' closed stdout before sending handshake",
                    self.plugin_name
                )
            })?;

        let handshake: HandshakeMessage =
            serde_json::from_str(&handshake_line).with_context(|| {
                format!(
                    "invalid handshake JSON from plugin '{}': {}",
                    self.plugin_name, handshake_line
                )
            })?;

        // Validate protocol version (D-02)
        if handshake.protocol_version != PROTOCOL_VERSION {
            child.kill().await.ok();
            bail!(
                "protocol version mismatch for plugin '{}': \
                 host speaks v{}, plugin speaks v{} -- \
                 update the plugin or yard to a compatible version",
                self.plugin_name,
                PROTOCOL_VERSION,
                handshake.protocol_version,
            );
        }

        // Check capability (D-03)
        if !handshake.capabilities.contains(&operation) {
            child.kill().await.ok();
            bail!(
                "plugin '{}' (v{}) does not support the {:?} operation \
                 (capabilities: {:?})",
                self.plugin_name,
                handshake.version,
                operation,
                handshake.capabilities,
            );
        }

        // Write request to stdin, then close stdin (D-05, HOST-02)
        let request_json = serde_json::to_string(&request).with_context(|| {
            format!(
                "failed to serialize request for plugin '{}'",
                self.plugin_name
            )
        })?;

        {
            let mut stdin_handle = stdin;
            stdin_handle
                .write_all(request_json.as_bytes())
                .await
                .with_context(|| {
                    format!("failed to write request to plugin '{}'", self.plugin_name)
                })?;
            stdin_handle.write_all(b"\n").await.with_context(|| {
                format!(
                    "failed to write newline to plugin '{}'",
                    self.plugin_name
                )
            })?;
            stdin_handle.shutdown().await.with_context(|| {
                format!(
                    "failed to close stdin for plugin '{}'",
                    self.plugin_name
                )
            })?;
            // stdin_handle is dropped here, sending EOF
        }

        // Read stdout lines sequentially (PROTO-04, D-06)
        let mut response_value: Option<Value> = None;

        while let Some(line) = reader.next_line().await.with_context(|| {
            format!(
                "failed to read response line from plugin '{}'",
                self.plugin_name
            )
        })? {
            let parsed: Value = serde_json::from_str(&line).with_context(|| {
                format!(
                    "malformed JSON line from plugin '{}': {}",
                    self.plugin_name, line
                )
            })?;

            match parsed.get("type").and_then(|t| t.as_str()) {
                Some("progress") => {
                    let progress: ProgressMessage =
                        serde_json::from_value(parsed).with_context(|| {
                            format!(
                                "invalid progress message from plugin '{}'",
                                self.plugin_name
                            )
                        })?;
                    progress_callback(progress);
                }
                Some("error") => {
                    let message = parsed
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("unknown error");
                    bail!(
                        "plugin '{}' returned error: {}",
                        self.plugin_name,
                        message
                    );
                }
                _ => {
                    // This is the operation response
                    response_value = Some(parsed);
                }
            }
        }

        // Wait for child process exit (D-07)
        let status = child.wait().await.with_context(|| {
            format!(
                "failed to wait for plugin '{}' to exit",
                self.plugin_name
            )
        })?;

        if !status.success() {
            bail!(
                "plugin '{}' exited with {} during {:?} operation",
                self.plugin_name,
                status,
                operation,
            );
        }

        response_value.ok_or_else(|| {
            anyhow!(
                "plugin '{}' exited successfully but produced no response \
                 for {:?} operation",
                self.plugin_name,
                operation,
            )
        })
    }

    // ---- Typed convenience methods ----

    /// Run the validate operation and return typed validation errors.
    ///
    /// # Errors
    ///
    /// Returns an error if the plugin process fails or returns invalid JSON.
    pub async fn call_validate(
        &self,
        job_name: &str,
        job_config: &Value,
    ) -> Result<Vec<PluginValidationError>> {
        let request = PluginRequest {
            operation: PluginOperation::Validate,
            job_name: Some(job_name.to_string()),
            job_config: Some(job_config.clone()),
            resources: None,
            artifact: None,
        };
        let raw = self
            .call_operation(PluginOperation::Validate, request, |p| {
                eprintln!("[{}] {}", self.plugin_name, p.message);
            })
            .await?;
        let resp: ValidateResponse = serde_json::from_value(raw)
            .context("failed to deserialize validate response")?;
        Ok(resp.errors)
    }

    /// Run the codegen operation and return the generated script content.
    ///
    /// # Errors
    ///
    /// Returns an error if the plugin process fails or returns invalid JSON.
    pub async fn call_codegen(
        &self,
        job_name: &str,
        job_config: &Value,
    ) -> Result<Option<String>> {
        let request = PluginRequest {
            operation: PluginOperation::Codegen,
            job_name: Some(job_name.to_string()),
            job_config: Some(job_config.clone()),
            resources: None,
            artifact: None,
        };
        let raw = self
            .call_operation(PluginOperation::Codegen, request, |p| {
                eprintln!("[{}] {}", self.plugin_name, p.message);
            })
            .await?;
        let resp: CodegenResponse = serde_json::from_value(raw)
            .context("failed to deserialize codegen response")?;
        Ok(resp.script)
    }

    /// Run the deploy operation and return created/updated resources.
    ///
    /// # Errors
    ///
    /// Returns an error if the plugin process fails or returns invalid JSON.
    pub async fn call_deploy(
        &self,
        job_name: &str,
        artifact: &str,
        job_config: &Value,
    ) -> Result<Vec<Resource>> {
        let request = PluginRequest {
            operation: PluginOperation::Deploy,
            job_name: Some(job_name.to_string()),
            job_config: Some(job_config.clone()),
            resources: None,
            artifact: Some(artifact.to_string()),
        };
        let raw = self
            .call_operation(PluginOperation::Deploy, request, |p| {
                eprintln!("[{}] {}", self.plugin_name, p.message);
            })
            .await?;
        let resp: DeployResponse = serde_json::from_value(raw)
            .context("failed to deserialize deploy response")?;
        Ok(resp.resources)
    }

    /// Run the destroy operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the plugin process fails or returns invalid JSON.
    pub async fn call_destroy(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Result<()> {
        let request = PluginRequest {
            operation: PluginOperation::Destroy,
            job_name: Some(job_name.to_string()),
            job_config: None,
            resources: Some(resources.to_vec()),
            artifact: None,
        };
        let raw = self
            .call_operation(PluginOperation::Destroy, request, |p| {
                eprintln!("[{}] {}", self.plugin_name, p.message);
            })
            .await?;
        let _resp: DestroyResponse = serde_json::from_value(raw)
            .context("failed to deserialize destroy response")?;
        Ok(())
    }

    /// Run the verify operation and return per-resource existence checks.
    ///
    /// # Errors
    ///
    /// Returns an error if the plugin process fails or returns invalid JSON.
    pub async fn call_verify(
        &self,
        job_name: &str,
        resources: &[Resource],
    ) -> Result<Vec<ResourceStatus>> {
        let request = PluginRequest {
            operation: PluginOperation::Verify,
            job_name: Some(job_name.to_string()),
            job_config: None,
            resources: Some(resources.to_vec()),
            artifact: None,
        };
        let raw = self
            .call_operation(PluginOperation::Verify, request, |p| {
                eprintln!("[{}] {}", self.plugin_name, p.message);
            })
            .await?;
        let resp: VerifyResponse = serde_json::from_value(raw)
            .context("failed to deserialize verify response")?;
        Ok(resp.statuses)
    }

    /// Run the schema operation and return config field descriptors.
    ///
    /// # Errors
    ///
    /// Returns an error if the plugin process fails or returns invalid JSON.
    pub async fn call_schema(&self) -> Result<Vec<SchemaField>> {
        let request = PluginRequest {
            operation: PluginOperation::Schema,
            job_name: None,
            job_config: None,
            resources: None,
            artifact: None,
        };
        let raw = self
            .call_operation(PluginOperation::Schema, request, |p| {
                eprintln!("[{}] {}", self.plugin_name, p.message);
            })
            .await?;
        let resp: SchemaResponse = serde_json::from_value(raw)
            .context("failed to deserialize schema response")?;
        Ok(resp.fields)
    }
}

/// A single plugin entry in the lock file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockEntry {
    /// Plugin name (e.g. `"yard-plugin-databricks"`).
    pub name: String,
    /// Plugin version (e.g. `"0.3.1"`).
    pub version: String,
    /// Platform-to-SHA-256 hex digest map (e.g. `{"aarch64-macos": "abc..."}`).
    pub checksums: HashMap<String, String>,
}

/// Lock file format for plugin binary checksums (D-16).
///
/// Stored as `yard.lock` at the project root. Phase 69 populates this
/// file at download time; Phase 66 wires the verification check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFile {
    /// Plugin entries with per-platform checksums.
    pub plugins: Vec<LockEntry>,
}

/// Verify that a plugin binary's SHA-256 checksum matches the lock file
/// entry (D-16, D-17).
///
/// Returns `Ok(())` if:
/// - The lock file does not exist (Phase 69 has not populated it yet)
/// - The lock file has no entry for this plugin name
/// - The checksum matches
///
/// # Errors
///
/// Returns an error if the lock file exists and contains an entry for
/// this plugin but the computed checksum does not match the expected
/// value.
pub async fn verify_checksum(
    binary_path: &Path,
    lock_file_path: &Path,
    plugin_name: &str,
) -> Result<()> {
    if !lock_file_path.exists() {
        return Ok(());
    }

    let lock_contents =
        tokio::fs::read_to_string(lock_file_path)
            .await
            .with_context(|| {
                format!("failed to read lock file at {}", lock_file_path.display())
            })?;

    let lock_file: LockFile = serde_json::from_str(&lock_contents).with_context(|| {
        format!(
            "failed to parse lock file at {}",
            lock_file_path.display()
        )
    })?;

    let entry = lock_file
        .plugins
        .iter()
        .find(|e| e.name == plugin_name);

    let entry = match entry {
        Some(e) => e,
        None => return Ok(()), // No entry for this plugin -- skip (D-17)
    };

    let platform = platform_key();
    let expected_checksum = match entry.checksums.get(&platform) {
        Some(c) => c,
        None => {
            return Ok(()); // No checksum for this platform -- skip
        }
    };

    // Compute SHA-256 in a blocking task to avoid starving the async runtime
    let binary_path_owned = binary_path.to_path_buf();
    let computed = tokio::task::spawn_blocking(move || -> Result<String> {
        let mut file = std::fs::File::open(&binary_path_owned).with_context(|| {
            format!(
                "failed to open plugin binary at {}",
                binary_path_owned.display()
            )
        })?;
        let mut hasher = Sha256::new();
        std::io::copy(&mut file, &mut hasher).with_context(|| {
            format!(
                "failed to read plugin binary at {}",
                binary_path_owned.display()
            )
        })?;
        let digest = hasher.finalize();
        Ok(format!("{digest:x}"))
    })
    .await
    .with_context(|| "checksum computation task panicked")?
    .with_context(|| {
        format!(
            "failed to compute SHA-256 for plugin binary '{}'",
            plugin_name
        )
    })?;

    if computed != *expected_checksum {
        bail!(
            "SHA-256 checksum mismatch for plugin '{}' on {}: \
             expected {}, got {} -- \
             the binary may have been tampered with; \
             run `yard plugin update {}` to re-download",
            plugin_name,
            platform,
            expected_checksum,
            computed,
            plugin_name,
        );
    }

    Ok(())
}

/// Remove all files in the plugin cache directory (D-14).
///
/// Deletes the directory and recreates it empty. Silently succeeds if
/// the directory does not exist.
///
/// # Errors
///
/// Returns an error if directory removal or recreation fails due to
/// filesystem permissions.
pub async fn cleanup_plugin_cache(plugins_dir: &Path) -> Result<()> {
    if !plugins_dir.exists() {
        return Ok(());
    }

    tokio::fs::remove_dir_all(plugins_dir)
        .await
        .with_context(|| {
            format!(
                "failed to remove plugin cache at {}",
                plugins_dir.display()
            )
        })?;

    tokio::fs::create_dir_all(plugins_dir)
        .await
        .with_context(|| {
            format!(
                "failed to recreate plugin cache at {}",
                plugins_dir.display()
            )
        })?;

    Ok(())
}

/// Return the current platform key for lock file checksum lookup.
///
/// Format: `"{arch}-{os}"` (e.g. `"aarch64-macos"`, `"x86_64-linux"`).
#[must_use]
pub fn platform_key() -> String {
    format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS)
}
