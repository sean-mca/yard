use std::path::{Path, PathBuf};
use tokio::process::Command;
use tracing::warn;

/// Clone a repo at a specific SHA into a temp directory, return the path.
/// Caller is responsible for cleaning up via `cleanup_workdir`.
pub async fn clone_at_sha(clone_url: &str, sha: &str) -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join(format!("yard-{sha}"));

    if dir.exists() {
        std::fs::remove_dir_all(&dir)
            .map_err(|e| format!("Failed to clean existing workdir: {e}"))?;
    }
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("Failed to create workdir: {e}"))?;

    let output = Command::new("git")
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

    let output = Command::new("git")
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

/// Run `yard plan` or `yard apply` in the given directory, return stdout.
pub async fn run_yard(command: &str, workdir: &Path) -> Result<String, String> {
    let output = Command::new("yard")
        .arg(command)
        .env("NO_COLOR", "1")
        .current_dir(workdir)
        .output()
        .await
        .map_err(|e| format!("Failed to run yard {command}: {e}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() {
        return Err(format!("yard {command} failed:\n{stdout}\n{stderr}"));
    }

    Ok(if stdout.is_empty() { stderr } else { stdout })
}
