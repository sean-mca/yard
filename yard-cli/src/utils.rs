use std::path::{Path, PathBuf};
// use yard_core::utils::get_current_context;

// pub fn interpolate_value(raw: &str) -> String {
//     let mut result = raw.to_string();

//     // Define the tokens we support
//     let tokens = ["${get_context_id()}"];

//     for token in tokens {
//         if result.contains(token) {
//             let replacement = match token {
//                 "${get_context_id()}" => get_current_context().unwrap_or_else(|_| "unknown".into()),
//                 _ => continue, // Should never happen with the array above
//             };

//             result = result.replace(token, &replacement);
//         }
//     }

//     result
// }

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

pub fn find_in_parent_folders(start_path: &Path, filename: &str) -> Option<PathBuf> {
    let mut current = start_path.to_path_buf();

    loop {
        let target = current.join(filename);
        if target.exists() {
            return Some(target);
        }
        // Move up one level. If pop() returns false, we hit the filesystem root.
        if !current.pop() {
            break;
        }
    }
    None
}
