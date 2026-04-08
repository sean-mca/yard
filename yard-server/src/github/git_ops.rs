use std::path::{Path, PathBuf};
use tokio::process::Command;
use tracing::warn;

/// Build a git `Command` with optional token-based auth.
/// When a token is provided, authentication is injected via environment
/// variables (`GIT_CONFIG_COUNT` + `GIT_CONFIG_KEY/VALUE`) so the token
/// never appears in command-line arguments (visible via `ps`) or in URLs
/// (which may be logged or persisted).
fn git_command(token: Option<&str>) -> Command {
    let mut cmd = Command::new("git");
    if let Some(token) = token {
        use base64::Engine;
        let credentials = format!("x-access-token:{token}");
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
        let header_value = format!("Authorization: basic {encoded}");
        cmd.env("GIT_CONFIG_COUNT", "1");
        cmd.env("GIT_CONFIG_KEY_0", "http.extraheader");
        cmd.env("GIT_CONFIG_VALUE_0", header_value);
    }
    cmd
}

/// Clone a repo at a specific SHA into a temp directory, return the path.
/// Caller is responsible for cleaning up via `cleanup_workdir`.
///
/// When `token` is provided, auth is injected via git config env vars —
/// the token never appears in the clone URL or process arguments.
pub async fn clone_at_sha(
    clone_url: &str,
    sha: &str,
    token: Option<&str>,
) -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join(format!("yard-{sha}"));

    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .map_err(|e| format!("Failed to clean existing workdir: {e}"))?;
    }
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create workdir: {e}"))?;

    let output = git_command(token)
        .args(["clone", "--depth", "1", clone_url, "."])
        .current_dir(&dir)
        .output()
        .await
        .map_err(|e| format!("git clone failed: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "git clone failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let output = git_command(token)
        .args(["fetch", "origin", sha, "--depth", "1"])
        .current_dir(&dir)
        .output()
        .await
        .map_err(|e| format!("git fetch failed: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "git fetch failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let output = Command::new("git")
        .args(["checkout", sha])
        .current_dir(&dir)
        .output()
        .await
        .map_err(|e| format!("git checkout failed: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "git checkout failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(dir)
}

pub fn cleanup_workdir(dir: &Path) {
    if let Err(e) = std::fs::remove_dir_all(dir) {
        warn!("Failed to clean up workdir {}: {e}", dir.display());
    }
}
