use anyhow::{Error, Result, anyhow};
use regex::Regex;
use serde_json::Value;
use yard_structs::YARDContext;

pub fn calculate_hash(config: &serde_json::Value) -> String {
    let serialized = serde_json::to_string(config).unwrap_or_default();
    blake3::hash(serialized.as_bytes()).to_hex().to_string()
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
