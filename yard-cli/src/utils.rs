//! Terminal color helpers and user-confirmation prompt.
//!
//! Provides TTY-aware ANSI coloring via a process-wide [`AtomicU8`]
//! color mode (`0` = normal, `1` = no color, `2` = colorblind palette).
//! The mode is set once at startup from `--no-color` / `--colorblind`
//! flags and the `NO_COLOR` env var.

use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicU8, Ordering};

/// Color mode: `0` = normal ANSI, `1` = plain (no escapes), `2` = colorblind palette.
static COLOR_MODE: AtomicU8 = AtomicU8::new(0);

/// Switch to plain output (no ANSI escape codes).
pub fn disable_color() {
    COLOR_MODE.store(1, Ordering::Relaxed);
}

/// Switch to the colorblind-friendly palette (cyan/blue/magenta).
pub fn enable_colorblind_mode() {
    COLOR_MODE.store(2, Ordering::Relaxed);
}

fn colorize(s: &str, normal_code: &str, colorblind_code: &str) -> String {
    let mode = COLOR_MODE.load(Ordering::Relaxed);
    if mode == 1 || !io::stdout().is_terminal() {
        return s.to_string();
    }
    let code = if mode == 2 {
        colorblind_code
    } else {
        normal_code
    };
    format!("\x1b[{code}m{s}\x1b[0m")
}

/// Creates: green (normal), cyan (colorblind)
pub fn color_create(s: &str) -> String {
    colorize(s, "32", "36")
}

/// Modifies: yellow (normal), blue (colorblind)
pub fn color_modify(s: &str) -> String {
    colorize(s, "33", "34")
}

/// Deletes: red (normal), magenta (colorblind)
pub fn color_delete(s: &str) -> String {
    colorize(s, "31", "35")
}

/// Wraps `s` in ANSI bold (identical in normal and colorblind mode).
pub fn bold(s: &str) -> String {
    colorize(s, "1", "1")
}

/// Prompt the user for confirmation. Returns true if they enter "y" or "yes".
pub fn confirm(prompt: &str) -> io::Result<bool> {
    print!("{prompt} ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim().to_lowercase().as_str(), "y" | "yes"))
}

/// Re-export of the YAML-to-JSON conversion helper from [`yard_core`].
pub use yard_core::resolve::yaml_to_json;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use yaml_rust2::YamlLoader;

    fn parse_yaml(s: &str) -> yaml_rust2::Yaml {
        YamlLoader::load_from_str(s).unwrap().remove(0)
    }

    #[test]
    fn converts_string() {
        let yaml = parse_yaml("hello");
        assert_eq!(yaml_to_json(&yaml), serde_json::json!("hello"));
    }

    #[test]
    fn converts_integer() {
        let yaml = parse_yaml("42");
        assert_eq!(yaml_to_json(&yaml), serde_json::json!(42));
    }

    #[test]
    fn converts_boolean() {
        let yaml = parse_yaml("true");
        assert_eq!(yaml_to_json(&yaml), serde_json::json!(true));
    }

    #[test]
    fn converts_null() {
        let yaml = parse_yaml("~");
        assert_eq!(yaml_to_json(&yaml), serde_json::Value::Null);
    }

    #[test]
    fn converts_array() {
        let yaml = parse_yaml("[1, 2, 3]");
        assert_eq!(yaml_to_json(&yaml), serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn converts_hash() {
        let yaml = parse_yaml("name: test\nvalue: 42");
        let json = yaml_to_json(&yaml);
        assert_eq!(json["name"], serde_json::json!("test"));
        assert_eq!(json["value"], serde_json::json!(42));
    }

    #[test]
    fn converts_nested_structure() {
        let yaml = parse_yaml("outer:\n  inner: deep\n  list:\n    - a\n    - b");
        let json = yaml_to_json(&yaml);
        assert_eq!(json["outer"]["inner"], serde_json::json!("deep"));
        assert_eq!(json["outer"]["list"], serde_json::json!(["a", "b"]));
    }
}
