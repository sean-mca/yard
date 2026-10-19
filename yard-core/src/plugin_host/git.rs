//! Fetch a plugin from a git repository and produce a runnable binary.
//!
//! The repository is cloned at the ref named by `plugin_version` (a tag,
//! branch, or commit), then the plugin executable is located inside the
//! checkout. A Rust plugin is built with `cargo build --release`; a script
//! plugin (for example a Python file) is used as-is. The resulting binary
//! is copied into the plugin cache and the checkout is removed.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result, anyhow, bail};
use tokio::process::Command;

use super::download::temp_sibling;

/// Clone `url` at `reference`, locate or build the plugin named
/// `plugin_name`, and copy the resulting executable to `dest`.
///
/// `subdir`, when set, selects a directory inside the checkout (the
/// `//path` suffix of a git source). `scratch_dir` is where the checkout
/// lives while building.
///
/// # Errors
///
/// Returns an error if `git` or `cargo` is unavailable, the ref cannot be
/// fetched, no plugin executable can be found or built in the checkout, or
/// copying the binary fails. The checkout is removed on every outcome.
pub(crate) async fn fetch_and_build(
    url: &str,
    subdir: Option<&str>,
    reference: &str,
    plugin_name: &str,
    dest: &Path,
    scratch_dir: &Path,
) -> Result<()> {
    let checkout = scratch_dir.join(format!("{plugin_name}-{}", sanitize_ref(reference)));
    if checkout.exists() {
        tokio::fs::remove_dir_all(&checkout)
            .await
            .with_context(|| format!("failed to clear stale checkout at {}", checkout.display()))?;
    }
    tokio::fs::create_dir_all(scratch_dir)
        .await
        .with_context(|| format!("failed to create {}", scratch_dir.display()))?;

    eprintln!("Fetching {plugin_name} from {url} at {reference}...");
    clone_at_ref(url, reference, &checkout).await?;

    let result = install_from_checkout(&checkout, subdir, url, plugin_name, dest).await;

    // Best effort, on success and failure alike: a cargo target directory
    // can be very large, and a stale checkout is cleared on the next attempt.
    if let Err(e) = tokio::fs::remove_dir_all(&checkout).await {
        eprintln!(
            "Warning: could not remove checkout {}: {e}",
            checkout.display()
        );
    }

    result
}

/// Locate or build the plugin inside a finished checkout and copy it to `dest`.
async fn install_from_checkout(
    checkout: &Path,
    subdir: Option<&str>,
    url: &str,
    plugin_name: &str,
    dest: &Path,
) -> Result<()> {
    let work_dir = match subdir {
        Some(sub) => checkout.join(sub.trim_matches('/')),
        None => checkout.to_path_buf(),
    };
    if !work_dir.is_dir() {
        bail!(
            "subdirectory '{}' does not exist in repository {url}",
            subdir.unwrap_or_default()
        );
    }

    let built = locate_or_build(&work_dir, plugin_name).await?;

    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let tmp_dest = temp_sibling(dest, "installing");
    tokio::fs::copy(&built, &tmp_dest).await.with_context(|| {
        format!(
            "failed to copy {} to {}",
            built.display(),
            tmp_dest.display()
        )
    })?;
    tokio::fs::rename(&tmp_dest, dest)
        .await
        .with_context(|| format!("failed to finalize plugin binary at {}", dest.display()))?;

    Ok(())
}

/// Clone the repository at the given ref, trying `v<ref>` and `<ref>` as
/// tag or branch names, then falling back to fetching `<ref>` as a commit.
async fn clone_at_ref(url: &str, reference: &str, checkout: &Path) -> Result<()> {
    let mut attempts: Vec<String> = Vec::with_capacity(3);
    let mut candidates: Vec<String> = Vec::with_capacity(2);
    if !reference.starts_with('v') {
        candidates.push(format!("v{reference}"));
    }
    candidates.push(reference.to_owned());

    for candidate in &candidates {
        let output = Command::new("git")
            .args([
                "clone", "--quiet", "--depth", "1", "--branch", candidate, url,
            ])
            .arg(checkout)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("failed to run git (is git installed and on PATH)")?;
        if output.status.success() {
            return Ok(());
        }
        attempts.push(format!(
            "--branch {candidate}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
        // A failed clone may leave a partial directory behind.
        let _ = tokio::fs::remove_dir_all(checkout).await;
    }

    if is_hex_ref(reference) {
        match fetch_commit(url, reference, checkout).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                attempts.push(format!("commit {reference}: {e:#}"));
                // A failed fetch leaves a partial directory behind.
                let _ = tokio::fs::remove_dir_all(checkout).await;
            }
        }
    }

    Err(anyhow!(
        "could not fetch '{reference}' from {url} -- tried:\n  {}",
        attempts.join("\n  ")
    ))
}

/// Fetch a single commit into a fresh repository (shallow).
async fn fetch_commit(url: &str, sha: &str, checkout: &Path) -> Result<()> {
    tokio::fs::create_dir_all(checkout)
        .await
        .with_context(|| format!("failed to create {}", checkout.display()))?;
    run_git(checkout, &["init", "--quiet"]).await?;
    run_git(checkout, &["remote", "add", "origin", url]).await?;
    run_git(
        checkout,
        &["fetch", "--quiet", "--depth", "1", "origin", sha],
    )
    .await?;
    run_git(checkout, &["checkout", "--quiet", "FETCH_HEAD"]).await?;
    Ok(())
}

async fn run_git(cwd: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("failed to run git (is git installed and on PATH)")?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Find the plugin executable inside a checkout, building it when the
/// directory is a Cargo project.
async fn locate_or_build(work_dir: &Path, plugin_name: &str) -> Result<PathBuf> {
    // 1. A ready-made executable or script named after the plugin.
    if let Some(found) = find_named_file(work_dir, plugin_name).await? {
        return Ok(found);
    }
    let bin_dir = work_dir.join("bin");
    if bin_dir.is_dir()
        && let Some(found) = find_named_file(&bin_dir, plugin_name).await?
    {
        return Ok(found);
    }

    // 2. A Rust project: build it.
    if work_dir.join("Cargo.toml").is_file() {
        return cargo_build(work_dir, plugin_name).await;
    }

    bail!(
        "no plugin executable found in {}: expected a file named '{plugin_name}' \
         (or '{plugin_name}.<ext>'), a 'bin/{plugin_name}', or a Cargo.toml to build",
        work_dir.display()
    )
}

/// Return a regular file in `dir` named exactly `name` or `name.<ext>`.
async fn find_named_file(dir: &Path, name: &str) -> Result<Option<PathBuf>> {
    let exact = dir.join(name);
    if exact.is_file() {
        return Ok(Some(exact));
    }
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .with_context(|| format!("failed to read directory {}", dir.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let path = entry.path();
        if path.is_file()
            && path.file_stem().and_then(|s| s.to_str()) == Some(name)
            && path.extension().is_some()
        {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// Build the plugin with cargo and return the release binary path.
async fn cargo_build(work_dir: &Path, plugin_name: &str) -> Result<PathBuf> {
    eprintln!("Building {plugin_name} with cargo (this can take a while)...");

    // Prefer the package named after the plugin; fall back to a bare bin
    // target of that name (e.g. a single-package repo named differently).
    let package_ok = run_cargo(work_dir, &["build", "--release", "-p", plugin_name]).await?;
    if !package_ok {
        let bin_ok = run_cargo(work_dir, &["build", "--release", "--bin", plugin_name]).await?;
        if !bin_ok {
            bail!(
                "cargo build of '{plugin_name}' failed in {} (see cargo output above; \
                 the package or bin target must be named '{plugin_name}')",
                work_dir.display()
            );
        }
    }

    let target_dir = cargo_target_dir(work_dir).await?;
    let binary = target_dir
        .join("release")
        .join(format!("{plugin_name}{}", std::env::consts::EXE_SUFFIX));
    if !binary.is_file() {
        bail!(
            "cargo build succeeded but {} does not exist",
            binary.display()
        );
    }
    Ok(binary)
}

/// Run cargo with inherited stderr so build progress is visible. Returns
/// `Ok(false)` on a non-zero exit so the caller can try another target.
async fn run_cargo(cwd: &Path, args: &[&str]) -> Result<bool> {
    let status = Command::new("cargo")
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .await
        .context("failed to run cargo (is a Rust toolchain installed)")?;
    Ok(status.success())
}

/// Ask cargo where it put build output (handles workspaces whose target
/// directory lives above the plugin's own directory).
async fn cargo_target_dir(cwd: &Path) -> Result<PathBuf> {
    let output = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("failed to run cargo metadata")?;
    if !output.status.success() {
        bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("failed to parse cargo metadata")?;
    metadata
        .get("target_directory")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("cargo metadata did not report a target_directory"))
}

/// True when `reference` could be an abbreviated or full commit hash.
fn is_hex_ref(reference: &str) -> bool {
    reference.len() >= 7
        && reference.len() <= 40
        && reference.chars().all(|c| c.is_ascii_hexdigit())
}

/// Make a git ref safe to use as a path component.
#[must_use]
pub(crate) fn sanitize_ref(reference: &str) -> String {
    reference
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_ref_replaces_path_separators() {
        assert_eq!(sanitize_ref("feature/foo bar"), "feature_foo_bar");
        assert_eq!(sanitize_ref("v1.2.3"), "v1.2.3");
    }

    #[test]
    fn is_hex_ref_accepts_hex_hashes_only() {
        assert!(is_hex_ref("abc1234"));
        assert!(is_hex_ref("0123456789abcdef0123456789abcdef01234567"));
        assert!(!is_hex_ref("v1.0.0"));
        assert!(!is_hex_ref("main"));
        assert!(!is_hex_ref("abc"));
    }

    #[tokio::test]
    async fn find_named_file_matches_exact_and_with_extension() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("yard-plugin-x.py"), "#!/bin/sh\n").unwrap();

        let found = find_named_file(dir.path(), "yard-plugin-x").await.unwrap();
        assert_eq!(found, Some(dir.path().join("yard-plugin-x.py")));

        let missing = find_named_file(dir.path(), "yard-plugin-y").await.unwrap();
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn fetch_and_build_uses_script_from_local_git_repo() {
        // git is a hard requirement of this code path, so a missing binary
        // is a test failure, not a skip.
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(repo.join("plugins")).unwrap();
        std::fs::write(
            repo.join("plugins/yard-plugin-demo.py"),
            "#!/bin/sh\necho hi\n",
        )
        .unwrap();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(args)
                .current_dir(&repo)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@t")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@t")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git(&["init", "--quiet", "-b", "main"]);
        git(&["add", "."]);
        git(&["commit", "--quiet", "-m", "init"]);
        git(&["tag", "v0.1.0"]);

        let dest = tmp.path().join("cache/yard-plugin-demo-git-0.1.0");
        fetch_and_build(
            repo.to_str().unwrap(),
            Some("plugins"),
            "0.1.0",
            "yard-plugin-demo",
            &dest,
            &tmp.path().join("cache/src"),
        )
        .await
        .unwrap();

        assert!(dest.is_file());
        assert_eq!(
            std::fs::read_to_string(&dest).unwrap(),
            "#!/bin/sh\necho hi\n"
        );
        assert!(!tmp.path().join("cache/src/yard-plugin-demo-0.1.0").exists());
    }

    #[tokio::test]
    async fn fetch_and_build_removes_checkout_when_plugin_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("README.md"), "nothing here\n").unwrap();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(args)
                .current_dir(&repo)
                .env("GIT_AUTHOR_NAME", "t")
                .env("GIT_AUTHOR_EMAIL", "t@t")
                .env("GIT_COMMITTER_NAME", "t")
                .env("GIT_COMMITTER_EMAIL", "t@t")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        git(&["init", "--quiet", "-b", "main"]);
        git(&["add", "."]);
        git(&["commit", "--quiet", "-m", "init"]);

        let scratch = tmp.path().join("cache/src");
        let err = fetch_and_build(
            repo.to_str().unwrap(),
            None,
            "main",
            "yard-plugin-demo",
            &tmp.path().join("cache/yard-plugin-demo"),
            &scratch,
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string().contains("no plugin executable found"),
            "{err}"
        );
        assert!(
            !scratch.join("yard-plugin-demo-main").exists(),
            "checkout should be removed on failure"
        );
    }
}
