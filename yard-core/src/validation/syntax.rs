use std::process::Command;

/// Validate that a generated Python script is syntactically valid.
/// Shells out to `python3` using `ast.parse`. Returns None if valid,
/// or Some(error message) if the script has a syntax error.
pub fn validate_python_syntax(script: &str) -> Option<String> {
    let result = Command::new("python3")
        .args(["-c", "import ast, sys; ast.parse(sys.stdin.read())"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn();

    let mut child = match result {
        Ok(child) => child,
        Err(e) => {
            return Some(format!("Failed to run python3: {e}. Is python3 installed?"));
        }
    };

    // Write script to stdin
    if let Some(ref mut stdin) = child.stdin {
        use std::io::Write;
        let _ = stdin.write_all(script.as_bytes());
    }
    // Drop stdin to close the pipe so python reads EOF
    child.stdin.take();

    let output = match child.wait_with_output() {
        Ok(o) => o,
        Err(e) => return Some(format!("Failed to wait on python3: {e}")),
    };

    if output.status.success() {
        None
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Extract just the meaningful part of the syntax error
        let msg = stderr
            .lines()
            .rev()
            .find(|l| l.contains("SyntaxError") || l.contains("Error"))
            .map(|l| l.trim().to_string())
            .unwrap_or_else(|| "Unknown syntax error".to_string());
        Some(msg)
    }
}
