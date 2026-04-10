use std::io::{self, IsTerminal, Write};
use std::sync::atomic::{AtomicU8, Ordering};

// 0 = normal colors, 1 = no color, 2 = colorblind mode
static COLOR_MODE: AtomicU8 = AtomicU8::new(0);

pub fn disable_color() {
    COLOR_MODE.store(1, Ordering::Relaxed);
}

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

pub fn yaml_to_json(yaml: &yaml_rust2::Yaml) -> serde_json::Value {
    match yaml {
        yaml_rust2::Yaml::Real(s) | yaml_rust2::Yaml::String(s) => {
            serde_json::Value::String(s.clone())
        }
        yaml_rust2::Yaml::Integer(i) => serde_json::Value::Number((*i).into()),
        yaml_rust2::Yaml::Boolean(b) => serde_json::Value::Bool(*b),
        yaml_rust2::Yaml::Array(a) => {
            serde_json::Value::Array(a.iter().map(yaml_to_json).collect())
        }
        yaml_rust2::Yaml::Hash(h) => {
            let mut map = serde_json::Map::new();
            for (k, v) in h {
                if let Some(key_str) = k.as_str() {
                    map.insert(key_str.to_string(), yaml_to_json(v));
                }
            }
            serde_json::Value::Object(map)
        }
        yaml_rust2::Yaml::Null => serde_json::Value::Null,
        _ => serde_json::Value::Null,
    }
}

#[cfg(test)]
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
