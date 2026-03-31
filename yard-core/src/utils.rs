use anyhow::{Error, Result, anyhow};
use regex::Regex;
use serde_json::Value;
use yard_structs::YARDContext;

pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> String {
    let hash = blake3::hash(data.as_ref());
    hash.to_hex().to_string()
}

pub fn calculate_json_hash(val: &Value) -> String {
    // We use a stable serialization to ensure the same JSON
    // always produces the same hash regardless of key order.
    let s = serde_json::to_string(val).unwrap_or_default();
    calculate_hash(s)
}

pub fn resolve_variables(raw_yaml: &str, ctx: &YARDContext) -> Result<String> {
    let re = Regex::new(r"\$\{(?P<key>[^}]+)\}")?;
    let mut result = raw_yaml.to_string();

    // Find all ${account.id} or ${transforms.my_func} patterns
    for caps in re.captures_iter(raw_yaml) {
        let full_match = &caps[0];
        let path = &caps["key"];

        // Resolve the path against our context JSON
        match resolve_json_path(ctx, path) {
            Some(value) => {
                let val_str = match value {
                    serde_json::Value::String(s) => s.to_string(),
                    _ => value.to_string(),
                };
                result = result.replace(full_match, &val_str);
            }
            // The Second Possibility: Throwing the explicit Error
            None => {
                return Err(anyhow!(
                    "Missing Variable: Could not find '{}' in the provided context (account/region/transforms)",
                    path
                ));
            }
        }
    }

    Ok(result)
}

fn resolve_json_path(ctx: &YARDContext, path: &str) -> Option<serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    let root = match parts[0] {
        "account" => &ctx.account,
        "region" => &ctx.region,
        "transforms" => &ctx.transforms,
        _ => return None,
    };

    let mut current = root;
    for &part in &parts[1..] {
        current = current.get(part)?;
    }
    Some(current.clone())
}
