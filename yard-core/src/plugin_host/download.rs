//! Plugin binary resolution, caching, and lock-file management.
//!
//! A job's `plugin_source` names where its plugin comes from. Three kinds
//! of source are understood (see [`PluginSource`]):
//!
//! - **Local files** -- `file:///abs/path`, `file://relative/path`, or a
//!   bare path (`./`, `../`, `/`, `~/`). Used directly; `plugin_version`
//!   is ignored and no checksum is recorded.
//! - **Git repositories** -- `git::<url>`, `git+https://`, `git@host:`,
//!   `ssh://`, `git://`, or any URL ending in `.git`, optionally followed
//!   by `//subdir`. Cloned at the ref named by `plugin_version`, then the
//!   plugin is located or built with cargo.
//! - **HTTP(S)** -- a GitHub repository (`https://github.com/org/repo`),
//!   from which the release asset for the current platform is derived; a
//!   directory URL ending in `/`; a URL template with `${name}`,
//!   `${version}`, `${target}`, `${os}`, `${arch}` placeholders; or a
//!   fully explicit URL. Downloads are cached and their SHA-256 recorded
//!   in `yard.lock` using a trust-on-first-use (TOFU) model.
//!
//! Users never spell out an operating system or architecture: yard
//! appends `<name>-<version>-<target>` itself, where `<target>` is the
//! Rust target triple of the running `yard` binary (see
//! [`target_triple`]).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use super::git;
use super::spawner::{LockEntry, LockFile, PluginHostConfig, platform_key, target_triple};

/// Where a plugin binary comes from, parsed from a job's `plugin_source`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginSource {
    /// A binary already on disk.
    Local(PathBuf),
    /// A git repository to clone, with an optional subdirectory.
    Git {
        /// Clone URL (scheme prefixes such as `git::` already stripped).
        url: String,
        /// Directory inside the checkout holding the plugin.
        subdir: Option<String>,
    },
    /// An HTTP(S) URL, URL template, directory URL, or GitHub repository.
    Http(String),
}

/// A plugin binary ready to spawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBinary {
    /// Path to the executable.
    pub path: PathBuf,
    /// Whether the binary's checksum is tracked in the lock file. Local
    /// files and git builds are not tracked; only downloads are.
    pub locked: bool,
}

/// Parse a `plugin_source` string into a [`PluginSource`].
///
/// Relative local paths are resolved against `project_root`.
///
/// # Errors
///
/// Returns an error if the source is empty.
pub fn parse_plugin_source(raw: &str, project_root: &Path) -> Result<PluginSource> {
    let source = raw.trim();
    if source.is_empty() {
        bail!("plugin_source must not be empty");
    }

    if let Some(rest) = source.strip_prefix("file://") {
        return Ok(PluginSource::Local(local_path(rest, project_root)));
    }
    if source.starts_with('/')
        || source.starts_with("./")
        || source.starts_with("../")
        || source.starts_with("~/")
    {
        return Ok(PluginSource::Local(local_path(source, project_root)));
    }

    if let Some(rest) = source.strip_prefix("git::") {
        return Ok(git_source(rest));
    }
    if let Some(rest) = source.strip_prefix("git+") {
        return Ok(git_source(rest));
    }
    if source.starts_with("git://") || source.starts_with("ssh://") || source.starts_with("git@") {
        return Ok(git_source(source));
    }
    let (repo, subdir) = split_subdir(source);
    if repo.trim_end_matches('/').ends_with(".git") {
        return Ok(git_source(source));
    }
    if source.contains("://") && subdir.is_some() {
        return Ok(git_source(source));
    }

    Ok(PluginSource::Http(source.to_owned()))
}

/// Expand `~/` and resolve relative paths against the project root.
fn local_path(raw: &str, project_root: &Path) -> PathBuf {
    if let Some(rest) = raw.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        project_root.join(path)
    }
}

fn git_source(raw: &str) -> PluginSource {
    let (url, subdir) = split_subdir(raw);
    PluginSource::Git {
        url: url.to_owned(),
        subdir: subdir.map(str::to_owned),
    }
}

/// Split `<repo>//<subdir>` into its parts, ignoring the `scheme://`
/// separator.
fn split_subdir(source: &str) -> (&str, Option<&str>) {
    let scheme_end = source.find("://").map_or(0, |i| i + 3);
    match source[scheme_end..].find("//") {
        Some(i) => {
            let split = scheme_end + i;
            let subdir = source[split + 2..].trim_matches('/');
            let subdir = if subdir.is_empty() {
                None
            } else {
                Some(subdir)
            };
            (&source[..split], subdir)
        }
        None => (source, None),
    }
}

/// Resolve a plugin binary, downloading or building it when not cached.
///
/// `version` is required for git and HTTP sources and ignored for local
/// files.
///
/// # Errors
///
/// Returns an error if the source cannot be parsed, a required version is
/// missing, the download or build fails, or filesystem I/O fails.
pub async fn ensure_plugin_cached(
    plugin_name: &str,
    version: Option<&str>,
    source: &str,
    config: &PluginHostConfig,
) -> Result<ResolvedBinary> {
    match parse_plugin_source(source, &config.project_root)? {
        PluginSource::Local(path) => {
            if !path.is_file() {
                bail!(
                    "plugin_source points at {} but no such file exists",
                    path.display()
                );
            }
            Ok(ResolvedBinary {
                path,
                locked: false,
            })
        }
        PluginSource::Git { url, subdir } => {
            let Some(reference) = version else {
                bail!(
                    "plugin_version is required for git sources -- \
                     set it to the tag, branch, or commit to check out"
                )
            };
            let binary_name = format!(
                "{plugin_name}-git-{}-{}",
                git::sanitize_ref(reference),
                platform_key()
            );
            let binary_path = config.plugins_dir.join(&binary_name);
            if !binary_path.exists() {
                git::fetch_and_build(
                    &url,
                    subdir.as_deref(),
                    reference,
                    plugin_name,
                    &binary_path,
                    &config.plugins_dir.join("src"),
                )
                .await?;
                set_executable(&binary_path).await?;
                eprintln!("Done.");
            }
            Ok(ResolvedBinary {
                path: binary_path,
                locked: false,
            })
        }
        PluginSource::Http(url) => {
            let Some(version) = version else {
                bail!("plugin_version is required when plugin_source is a URL")
            };
            let platform = platform_key();
            let binary_name = format!("{plugin_name}-{version}-{platform}");
            let binary_path = config.plugins_dir.join(&binary_name);
            if binary_path.exists() {
                return Ok(ResolvedBinary {
                    path: binary_path,
                    locked: true,
                });
            }

            eprintln!("Downloading {plugin_name} v{version}...");
            let candidates = resolve_download_urls(&url, plugin_name, version);
            let mut not_found: Vec<&str> = Vec::with_capacity(candidates.len());
            let mut downloaded = false;
            for candidate in &candidates {
                match download_binary(candidate, &binary_path).await? {
                    DownloadOutcome::Saved => {
                        downloaded = true;
                        break;
                    }
                    DownloadOutcome::NotFound => not_found.push(candidate),
                }
            }
            if !downloaded {
                bail!(
                    "no plugin binary found for {plugin_name} v{version} on {} -- tried:\n  {}",
                    target_triple(),
                    not_found.join("\n  ")
                );
            }
            set_executable(&binary_path).await?;

            let checksum = compute_sha256(&binary_path).await?;
            let lock_path = config.lock_file_path.as_deref();
            update_lock_file(lock_path, plugin_name, version, &platform, &checksum).await?;

            eprintln!("Done.");
            Ok(ResolvedBinary {
                path: binary_path,
                locked: true,
            })
        }
    }
}

/// Turn an HTTP `plugin_source` into the concrete URL(s) to try, in order.
///
/// - A template containing `${...}` is expanded.
/// - `https://github.com/<org>/<repo>` becomes the release asset
///   `<name>-<version>-<target>` under tags `v<version>` then `<version>`.
/// - A URL ending in `/` gets `<name>-<version>-<target>` appended.
/// - Anything else is used verbatim.
#[must_use]
pub(crate) fn resolve_download_urls(source: &str, name: &str, version: &str) -> Vec<String> {
    if source.contains("${") {
        return vec![expand_url_template(source, name, version)];
    }

    let asset = format!("{name}-{version}-{}", target_triple());
    if let Some((org, repo)) = github_repo(source) {
        return vec![
            format!("https://github.com/{org}/{repo}/releases/download/v{version}/{asset}"),
            format!("https://github.com/{org}/{repo}/releases/download/{version}/{asset}"),
        ];
    }
    if source.ends_with('/') {
        return vec![format!("{source}{asset}")];
    }
    vec![source.to_owned()]
}

/// Return `(org, repo)` when `source` is a bare GitHub repository URL.
fn github_repo(source: &str) -> Option<(&str, &str)> {
    let rest = source
        .strip_prefix("https://github.com/")
        .or_else(|| source.strip_prefix("http://github.com/"))?;
    let rest = rest.trim_end_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    let mut parts = rest.split('/');
    let org = parts.next().filter(|s| !s.is_empty())?;
    let repo = parts.next().filter(|s| !s.is_empty())?;
    if parts.next().is_some() {
        return None;
    }
    Some((org, repo))
}

/// Replace `${name}`, `${version}`, `${target}`, `${platform}`, `${os}`,
/// and `${arch}` placeholders in a URL template.
#[must_use]
pub(crate) fn expand_url_template(template: &str, name: &str, version: &str) -> String {
    let mut url = template
        .replace("${name}", name)
        .replace("${version}", version)
        .replace("${os}", std::env::consts::OS)
        .replace("${arch}", std::env::consts::ARCH);
    if url.contains("${target}") {
        url = url.replace("${target}", &target_triple());
    }
    if url.contains("${platform}") {
        url = url.replace("${platform}", &platform_key());
    }
    url
}

/// Result of a single download attempt.
enum DownloadOutcome {
    /// The file was written to the destination.
    Saved,
    /// The server answered 404; the caller may try another URL.
    NotFound,
}

/// Fetch a binary from `url` and write it to `dest`.
///
/// Creates parent directories as needed.
async fn download_binary(url: &str, dest: &Path) -> Result<DownloadOutcome> {
    let is_loopback = url.starts_with("http://127.0.0.1")
        || url.starts_with("http://localhost")
        || url.starts_with("http://[::1]");
    if !url.starts_with("https://") && !is_loopback {
        bail!("plugin binary URL must use HTTPS (got: {url})");
    }

    let response = reqwest::get(url)
        .await
        .with_context(|| format!("failed to fetch plugin binary from {url}"))?;

    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(DownloadOutcome::NotFound);
    }
    if !response.status().is_success() {
        bail!(
            "HTTP {} when downloading plugin binary from {url}",
            response.status()
        );
    }

    const MAX_PLUGIN_SIZE: u64 = 512 * 1024 * 1024; // 512 MB
    if let Some(content_length) = response.content_length()
        && content_length > MAX_PLUGIN_SIZE
    {
        bail!(
            "plugin binary at {url} is {content_length} bytes, \
             exceeding the {MAX_PLUGIN_SIZE} byte limit"
        );
    }

    let bytes = response
        .bytes()
        .await
        .with_context(|| format!("failed to read response body from {url}"))?;

    if is_html_payload(&bytes) {
        bail!(
            "{url} returned an HTML page, not a plugin binary -- \
             if this is a git repository, prefix the source with `git::`"
        );
    }

    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let tmp_dest = temp_sibling(dest, "downloading");
    tokio::fs::write(&tmp_dest, &bytes)
        .await
        .with_context(|| format!("failed to write plugin binary to {}", tmp_dest.display()))?;
    tokio::fs::rename(&tmp_dest, dest)
        .await
        .with_context(|| format!("failed to finalize plugin binary at {}", dest.display()))?;

    Ok(DownloadOutcome::Saved)
}

/// True when the payload starts like an HTML document.
fn is_html_payload(bytes: &[u8]) -> bool {
    let head = bytes.trim_ascii_start();
    let starts_with = |prefix: &[u8]| {
        head.len() >= prefix.len() && head[..prefix.len()].eq_ignore_ascii_case(prefix)
    };
    starts_with(b"<!doctype") || starts_with(b"<html")
}

/// Build `<dest>.<suffix>` next to `dest` without touching any dots already
/// in the file name (`with_extension` would truncate `plugin-0.1.0`).
#[must_use]
pub(crate) fn temp_sibling(dest: &Path, suffix: &str) -> PathBuf {
    let name = dest
        .file_name()
        .map(|n| n.to_string_lossy())
        .unwrap_or_default();
    dest.with_file_name(format!("{name}.{suffix}"))
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

    if let Some(entry) = lock_file.plugins.iter_mut().find(|e| e.name == plugin_name) {
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

    let json = serde_json::to_string_pretty(&lock_file).context("failed to serialize lock file")?;

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

    fn root() -> PathBuf {
        PathBuf::from("/proj")
    }

    #[test]
    fn parse_file_url_absolute() {
        let src = parse_plugin_source("file:///opt/bin/yard-plugin-glue", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Local(PathBuf::from("/opt/bin/yard-plugin-glue"))
        );
    }

    #[test]
    fn parse_file_url_relative_resolves_against_project_root() {
        let src = parse_plugin_source("file://target/release/yard-plugin-glue", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Local(PathBuf::from("/proj/target/release/yard-plugin-glue"))
        );
    }

    #[test]
    fn parse_bare_relative_path_is_local() {
        let src = parse_plugin_source("./build/yard-plugin-glue", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Local(PathBuf::from("/proj/./build/yard-plugin-glue"))
        );
        let src = parse_plugin_source("/abs/yard-plugin-glue", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Local(PathBuf::from("/abs/yard-plugin-glue"))
        );
    }

    #[test]
    fn parse_git_prefix_forms() {
        for raw in [
            "git::https://my-repo/plugins/whatever",
            "git+https://my-repo/plugins/whatever",
        ] {
            let src = parse_plugin_source(raw, &root()).unwrap();
            assert_eq!(
                src,
                PluginSource::Git {
                    url: "https://my-repo/plugins/whatever".into(),
                    subdir: None
                },
                "{raw}"
            );
        }
    }

    #[test]
    fn parse_git_suffix_and_ssh_forms() {
        let src = parse_plugin_source("https://my-repo/plugins/whatever.git", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Git {
                url: "https://my-repo/plugins/whatever.git".into(),
                subdir: None
            }
        );
        let src = parse_plugin_source("git@github.com:org/plugins.git", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Git {
                url: "git@github.com:org/plugins.git".into(),
                subdir: None
            }
        );
        let src = parse_plugin_source("ssh://git@host/org/plugins", &root()).unwrap();
        assert!(matches!(src, PluginSource::Git { .. }));
    }

    #[test]
    fn parse_git_with_subdir() {
        let src = parse_plugin_source(
            "https://github.com/org/yard-plugins.git//yard-plugin-glue",
            &root(),
        )
        .unwrap();
        assert_eq!(
            src,
            PluginSource::Git {
                url: "https://github.com/org/yard-plugins.git".into(),
                subdir: Some("yard-plugin-glue".into())
            }
        );
        // A `//subdir` marker alone marks a git source.
        let src = parse_plugin_source("https://my-repo/plugins//glue", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Git {
                url: "https://my-repo/plugins".into(),
                subdir: Some("glue".into())
            }
        );
    }

    #[test]
    fn parse_plain_https_is_http() {
        let src = parse_plugin_source("https://github.com/org/yard-plugin-glue", &root()).unwrap();
        assert_eq!(
            src,
            PluginSource::Http("https://github.com/org/yard-plugin-glue".into())
        );
    }

    #[test]
    fn parse_empty_source_errors() {
        assert!(parse_plugin_source("   ", &root()).is_err());
    }

    #[test]
    fn resolve_github_repo_derives_release_asset() {
        let urls = resolve_download_urls(
            "https://github.com/org/yard-plugin-glue",
            "yard-plugin-glue",
            "0.1.0",
        );
        let asset = format!("yard-plugin-glue-0.1.0-{}", target_triple());
        assert_eq!(
            urls,
            vec![
                format!("https://github.com/org/yard-plugin-glue/releases/download/v0.1.0/{asset}"),
                format!("https://github.com/org/yard-plugin-glue/releases/download/0.1.0/{asset}"),
            ]
        );
    }

    #[test]
    fn resolve_directory_url_appends_asset_name() {
        let urls =
            resolve_download_urls("https://cdn.example.com/plugins/", "yard-plugin-x", "2.0");
        assert_eq!(
            urls,
            vec![format!(
                "https://cdn.example.com/plugins/yard-plugin-x-2.0-{}",
                target_triple()
            )]
        );
    }

    #[test]
    fn resolve_explicit_url_is_verbatim() {
        let url = "https://github.com/org/repo/releases/download/v1/yard-plugin-x-1-custom";
        assert_eq!(
            resolve_download_urls(url, "yard-plugin-x", "1"),
            vec![url.to_owned()]
        );
    }

    #[test]
    fn github_repo_rejects_deeper_paths() {
        assert_eq!(
            github_repo("https://github.com/org/repo"),
            Some(("org", "repo"))
        );
        assert_eq!(
            github_repo("https://github.com/org/repo/"),
            Some(("org", "repo"))
        );
        assert_eq!(
            github_repo("https://github.com/org/repo/releases/download/v1/x"),
            None
        );
        assert_eq!(github_repo("https://gitlab.com/org/repo"), None);
    }

    #[test]
    fn test_expand_url_template_replaces_all_placeholders() {
        let result = expand_url_template(
            "https://gh.com/${name}/v${version}/${name}-${version}-${os}-${arch}-${target}-${platform}",
            "yard-plugin-glue",
            "0.3.1",
        );
        assert!(result.contains("yard-plugin-glue"));
        assert!(result.contains("0.3.1"));
        assert!(result.contains(std::env::consts::OS));
        assert!(result.contains(std::env::consts::ARCH));
        assert!(result.contains(&target_triple()));
        assert!(result.contains(&platform_key()));
        assert!(!result.contains("${"));
    }

    #[test]
    fn test_expand_url_template_no_placeholders_unchanged() {
        let template = "https://example.com/plugin/v1.0/binary";
        let result = expand_url_template(template, "foo", "1.0");
        assert_eq!(result, template);
    }

    #[test]
    fn is_html_payload_detects_pages() {
        assert!(is_html_payload(b"  <!DOCTYPE html><html>"));
        assert!(is_html_payload(b"<html lang=en>"));
        assert!(!is_html_payload(b"\x7fELF binary"));
        assert!(!is_html_payload(b"#!/usr/bin/env python3\n"));
        assert!(!is_html_payload(b"<ht"));
    }

    #[test]
    fn temp_sibling_keeps_dotted_file_names_intact() {
        let dest = Path::new("/cache/yard-plugin-x-0.1.0-aarch64-macos");
        assert_eq!(
            temp_sibling(dest, "downloading"),
            PathBuf::from("/cache/yard-plugin-x-0.1.0-aarch64-macos.downloading")
        );
    }

    #[tokio::test]
    async fn ensure_plugin_cached_local_file_ignores_version() {
        let dir = tempdir().unwrap();
        let bin = dir.path().join("yard-plugin-demo");
        std::fs::write(&bin, "#!/bin/sh\n").unwrap();
        let config = PluginHostConfig {
            plugins_dir: dir.path().join("plugins"),
            project_root: dir.path().to_path_buf(),
            ..Default::default()
        };

        let resolved = ensure_plugin_cached(
            "yard-plugin-demo",
            None,
            &format!("file://{}", bin.display()),
            &config,
        )
        .await
        .unwrap();
        assert_eq!(
            resolved,
            ResolvedBinary {
                path: bin.clone(),
                locked: false
            }
        );

        let relative = ensure_plugin_cached(
            "yard-plugin-demo",
            Some("9.9.9"),
            "./yard-plugin-demo",
            &config,
        )
        .await
        .unwrap();
        assert_eq!(relative.path, dir.path().join("./yard-plugin-demo"));
        assert!(!relative.locked);
    }

    #[tokio::test]
    async fn ensure_plugin_cached_local_missing_file_errors() {
        let dir = tempdir().unwrap();
        let config = PluginHostConfig {
            project_root: dir.path().to_path_buf(),
            ..Default::default()
        };
        let err = ensure_plugin_cached("yard-plugin-demo", None, "file:///nope/missing", &config)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no such file"), "{err}");
    }

    #[tokio::test]
    async fn ensure_plugin_cached_http_requires_version() {
        let config = PluginHostConfig::default();
        let err = ensure_plugin_cached("yard-plugin-demo", None, "https://github.com/o/r", &config)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("plugin_version is required"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn ensure_plugin_cached_git_requires_version() {
        let config = PluginHostConfig::default();
        let err = ensure_plugin_cached("yard-plugin-demo", None, "git::https://h/r", &config)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("plugin_version is required"),
            "{err}"
        );
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

        update_lock_file(
            Some(&lock_path),
            "yard-plugin-glue",
            "0.3.1",
            "aarch64-macos",
            "abc123",
        )
        .await
        .unwrap();

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
        assert_eq!(lock.plugins[0].checksums.len(), 1);
        assert_eq!(
            lock.plugins[0].checksums.get("aarch64-macos"),
            Some(&"new_mac".to_owned())
        );
        assert!(!lock.plugins[0].checksums.contains_key("x86_64-linux"));
    }
}
