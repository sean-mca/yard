//! Plugin binary download, caching, and lock-file management.
//!
//! This module handles the full lifecycle of fetching plugin binaries from
//! remote HTTPS endpoints, caching them locally, computing SHA-256
//! checksums, and recording those checksums in the `yard.lock` file using
//! a trust-on-first-use (TOFU) model.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use super::spawner::{LockEntry, LockFile, PluginHostConfig, platform_key};

/// Download and cache a plugin binary if not already present.
///
/// Returns the local path to the cached binary. On cache hit the binary is
/// returned immediately; on cache miss the binary is downloaded from the
/// URL template, made executable, checksummed, and recorded in the lock
/// file.
///
/// # Errors
///
/// Returns an error if the download fails, the HTTP response is non-2xx,
/// filesystem I/O fails, or the checksum cannot be computed.
pub async fn ensure_plugin_cached(
    plugin_name: &str,
    version: &str,
    source_url_template: &str,
    config: &PluginHostConfig,
) -> Result<PathBuf> {
    let platform = platform_key();
    let binary_name = format!("{plugin_name}-{version}-{platform}");
    let binary_path = config.plugins_dir.join(&binary_name);

    if binary_path.exists() {
        return Ok(binary_path);
    }

    let url = expand_url_template(source_url_template, plugin_name, version);

    eprintln!("Downloading {plugin_name} v{version}...");
    download_binary(&url, &binary_path).await?;
    set_executable(&binary_path).await?;

    let checksum = compute_sha256(&binary_path).await?;

    let lock_path = config.lock_file_path.as_deref();
    update_lock_file(lock_path, plugin_name, version, &platform, &checksum).await?;

    eprintln!("Done.");

    Ok(binary_path)
}

/// Replace `${name}`, `${version}`, `${os}`, and `${arch}` placeholders
/// in a URL template.
fn expand_url_template(template: &str, name: &str, version: &str) -> String {
    template
        .replace("${name}", name)
        .replace("${version}", version)
        .replace("${os}", std::env::consts::OS)
        .replace("${arch}", std::env::consts::ARCH)
}

/// Fetch a binary from `url` and write it to `dest`.
///
/// Creates parent directories as needed.
async fn download_binary(url: &str, dest: &Path) -> Result<()> {
    if !url.starts_with("https://") {
        bail!("plugin binary URL must use HTTPS (got: {url})");
    }

    let response = reqwest::get(url)
        .await
        .with_context(|| format!("failed to fetch plugin binary from {url}"))?;

    if !response.status().is_success() {
        bail!(
            "HTTP {} when downloading plugin binary from {url}",
            response.status()
        );
    }

    const MAX_PLUGIN_SIZE: u64 = 512 * 1024 * 1024; // 512 MB
    if let Some(content_length) = response.content_length() {
        if content_length > MAX_PLUGIN_SIZE {
            bail!(
                "plugin binary at {url} is {content_length} bytes, \
                 exceeding the {MAX_PLUGIN_SIZE} byte limit"
            );
        }
    }

    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;

    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let tmp_dest = dest.with_extension("downloading");
    tokio::fs::write(&tmp_dest, &bytes)
        .await
        .with_context(|| format!("failed to write plugin binary to {}", tmp_dest.display()))?;
    tokio::fs::rename(&tmp_dest, dest)
        .await
        .with_context(|| format!("failed to finalize plugin binary at {}", dest.display()))?;

    Ok(())
}

/// Set the executable permission bit on a file (Unix only).
#[cfg(unix)]
async fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let perms = std::fs::Permissions::from_mode(0o755);
    tokio::fs::set_permissions(path, perms)
        .await
        .with_context(|| format!("failed to set executable permission on {}", path.display()))?;
    Ok(())
}

/// No-op on non-Unix platforms.
#[cfg(not(unix))]
async fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}

/// Compute the SHA-256 hex digest of a file.
///
/// Runs in a blocking task to avoid starving the async runtime.
async fn compute_sha256(path: &Path) -> Result<String> {
    let path_owned = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut file = std::fs::File::open(&path_owned)
            .with_context(|| format!("failed to open {}", path_owned.display()))?;
        let mut hasher = Sha256::new();
        std::io::copy(&mut file, &mut hasher)
            .with_context(|| format!("failed to read {}", path_owned.display()))?;
        let digest = hasher.finalize();
        Ok(format!("{digest:x}"))
    })
    .await
    .with_context(|| "SHA-256 computation task panicked")?
}

/// Record a plugin binary's checksum in the lock file (TOFU model).
///
/// - New plugin: appends a fresh `LockEntry`.
/// - Existing plugin, same version: adds/updates the platform checksum.
/// - Existing plugin, different version: clears stale checksums, updates
///   the version, then records the new platform checksum.
///
/// When `lock_path` is `None`, defaults to `yard.lock` in the current
/// directory.
///
/// # Errors
///
/// Returns an error if reading or writing the lock file fails, or if
/// the existing lock file contains malformed JSON.
pub async fn update_lock_file(
    lock_path: Option<&Path>,
    plugin_name: &str,
    version: &str,
    platform: &str,
    checksum: &str,
) -> Result<()> {
    let default_path = PathBuf::from("yard.lock");
    let path = lock_path.unwrap_or(&default_path);

    let mut lock_file = if path.exists() {
        let contents = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("failed to read lock file at {}", path.display()))?;
        serde_json::from_str::<LockFile>(&contents)
            .with_context(|| format!("failed to parse lock file at {}", path.display()))?
    } else {
        LockFile {
            plugins: Vec::new(),
        }
    };

    if let Some(entry) = lock_file
        .plugins
        .iter_mut()
        .find(|e| e.name == plugin_name)
    {
        if entry.version != version {
            // Version bump -- stale checksums are invalid (D-19)
            entry.checksums.clear();
            entry.version = version.to_owned();
        }
        entry
            .checksums
            .insert(platform.to_owned(), checksum.to_owned());
    } else {
        let mut checksums = std::collections::HashMap::new();
        checksums.insert(platform.to_owned(), checksum.to_owned());
        lock_file.plugins.push(LockEntry {
            name: plugin_name.to_owned(),
            version: version.to_owned(),
            checksums,
        });
    }

    let json = serde_json::to_string_pretty(&lock_file)
        .context("failed to serialize lock file")?;

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let tmp_path = path.with_extension("lock.tmp");
    tokio::fs::write(&tmp_path, json.as_bytes())
        .await
        .with_context(|| format!("failed to write lock file at {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("failed to finalize lock file at {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_expand_url_template_replaces_all_placeholders() {
        let result = expand_url_template(
            "https://gh.com/${name}/v${version}/${name}-${version}-${os}-${arch}",
            "yard-plugin-glue",
            "0.3.1",
        );
        assert!(result.contains("yard-plugin-glue"));
        assert!(result.contains("0.3.1"));
        assert!(result.contains(std::env::consts::OS));
        assert!(result.contains(std::env::consts::ARCH));
        assert!(!result.contains("${"));
    }

    #[test]
    fn test_expand_url_template_no_placeholders_unchanged() {
        let template = "https://example.com/plugin/v1.0/binary";
        let result = expand_url_template(template, "foo", "1.0");
        assert_eq!(result, template);
    }

    #[tokio::test]
    async fn test_update_lock_file_creates_new_entry() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("yard.lock");

        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.3.1",
            "aarch64-macos",
            "abc123",
        )
        .await
        .unwrap();

        let contents = tokio::fs::read_to_string(&lock_path).await.unwrap();
        let lock: LockFile = serde_json::from_str(&contents).unwrap();
        assert_eq!(lock.plugins.len(), 1);
        assert_eq!(lock.plugins[0].name, "yard-plugin-glue");
        assert_eq!(lock.plugins[0].version, "0.3.1");
        assert_eq!(
            lock.plugins[0].checksums.get("aarch64-macos"),
            Some(&"abc123".to_owned())
        );
    }

    #[tokio::test]
    async fn test_update_lock_file_adds_platform_to_existing() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("yard.lock");

        // First write: one platform
        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.3.1",
            "aarch64-macos",
            "abc123",
        )
        .await
        .unwrap();

        // Second write: different platform, same plugin + version
        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.3.1",
            "x86_64-linux",
            "def456",
        )
        .await
        .unwrap();

        let contents = tokio::fs::read_to_string(&lock_path).await.unwrap();
        let lock: LockFile = serde_json::from_str(&contents).unwrap();
        assert_eq!(lock.plugins.len(), 1);
        assert_eq!(lock.plugins[0].checksums.len(), 2);
        assert_eq!(
            lock.plugins[0].checksums.get("aarch64-macos"),
            Some(&"abc123".to_owned())
        );
        assert_eq!(
            lock.plugins[0].checksums.get("x86_64-linux"),
            Some(&"def456".to_owned())
        );
    }

    #[tokio::test]
    async fn test_update_lock_file_clears_checksums_on_version_bump() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("yard.lock");

        // Seed with version 0.3.1, two platforms
        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.3.1",
            "aarch64-macos",
            "old_mac",
        )
        .await
        .unwrap();
        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.3.1",
            "x86_64-linux",
            "old_linux",
        )
        .await
        .unwrap();

        // Version bump to 0.4.0 with only one platform
        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.4.0",
            "aarch64-macos",
            "new_mac",
        )
        .await
        .unwrap();

        let contents = tokio::fs::read_to_string(&lock_path).await.unwrap();
        let lock: LockFile = serde_json::from_str(&contents).unwrap();
        assert_eq!(lock.plugins.len(), 1);
        assert_eq!(lock.plugins[0].version, "0.4.0");
        // Old checksums cleared; only the new platform remains
        assert_eq!(lock.plugins[0].checksums.len(), 1);
        assert_eq!(
            lock.plugins[0].checksums.get("aarch64-macos"),
            Some(&"new_mac".to_owned())
        );
        assert!(!lock.plugins[0].checksums.contains_key("x86_64-linux"));
    }
}
