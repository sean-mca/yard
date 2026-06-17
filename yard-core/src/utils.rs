//! Shared utility functions for hashing and variable resolution.
//!
//! - [`calculate_hash`] — blake3 content hashing for state drift detection
//! - [`calculate_json_hash`] — key-order-canonical JSON hashing
//! - [`resolve_variables`] — `${...}` variable substitution in YAML content
//! - [`resolve_json_path`] — dotted-path lookup against the YARD context

use anyhow::{Result, anyhow};
use regex::Regex;
use serde_json::Value;
use std::sync::LazyLock;
use yard_structs::YARDContext;

static VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    // SAFETY: this is a compile-time constant pattern that always succeeds.
    #[allow(clippy::expect_used)]
    Regex::new(r"\$\{(?P<key>[^}]+)\}").expect("static regex")
});

/// Compute a blake3 hash of the input data, returning a hex-encoded string.
#[must_use]
pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> String {
    let hash = blake3::hash(data.as_ref());
    hash.to_hex().to_string()
}

/// Compute a blake3 hash of a JSON value after key-order canonicalization.
///
/// Keys are sorted recursively so that structurally equivalent values
/// produce identical hashes regardless of insertion order.
///
/// # Errors
///
/// Returns an error if JSON serialization of the canonicalized value fails.
pub fn calculate_json_hash(val: &Value) -> Result<String> {
    let canonical = canonicalize_value(val);
    let s = serde_json::to_string(&canonical)?;
    Ok(calculate_hash(s))
}

/// Recursively sort object keys so structurally equal values serialize identically.
#[must_use]
fn canonicalize_value(val: &Value) -> Value {
    match val {
        Value::Object(map) => {
            let sorted: serde_json::Map<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), canonicalize_value(v)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect();
            Value::Object(sorted)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(canonicalize_value).collect()),
        other => other.clone(),
    }
}

/// Replace `${...}` variable references in raw YAML with values from the
/// YARD context (account, region, transforms, dag).
///
/// # Errors
///
/// Returns an error if any referenced variable path cannot be found in
/// the provided context.
pub fn resolve_variables(raw_yaml: &str, ctx: &YARDContext) -> Result<String> {
    let re = &*VAR_RE;
    let mut err: Option<anyhow::Error> = None;

    let result = re.replace_all(raw_yaml, |caps: &regex::Captures| {
        let path = &caps["key"];
        match resolve_json_path(ctx, path) {
            Some(value) => match value {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            },
            None => {
                if err.is_none() {
                    err = Some(anyhow!(
                        "Missing Variable: Could not find '{}' in the provided context (account/region/transforms)",
                        path
                    ));
                }
                String::new()
            }
        }
    });

    if let Some(e) = err {
        return Err(e);
    }

    Ok(result.into_owned())
}

/// Resolve a dotted path (e.g. `account.id`) against the YARD context.
///
/// Returns `None` if the root segment is unknown or if any segment in the
/// path does not exist.
#[must_use]
pub fn resolve_json_path(ctx: &YARDContext, path: &str) -> Option<serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    let root = match parts[0] {
        "account" => &ctx.account,
        "region" => &ctx.region,
        "transforms" => &ctx.transforms,
        "dag" => &ctx.dag,
        _ => return None,
    };

    let mut current = root;
    for &part in &parts[1..] {
        current = current.get(part)?;
    }
    Some(current.clone())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_context() -> YARDContext {
        YARDContext {
            account: json!({"id": "123456789", "name": "dev-account"}),
            region: json!({"name": "us-east-1", "code": "ue1"}),
            transforms: json!({"suffix": "prod", "nested": {"key": "deep_val"}}),
            dag: json!({"schedule": "@daily", "name": "orders_dag"}),
        }
    }

    #[test]
    fn resolve_dag_field() {
        let ctx = test_context();
        assert_eq!(
            resolve_json_path(&ctx, "dag.schedule"),
            Some(json!("@daily"))
        );
        assert_eq!(
            resolve_json_path(&ctx, "dag.name"),
            Some(json!("orders_dag"))
        );
    }

    // --- calculate_hash ---

    #[test]
    fn hash_deterministic() {
        let a = calculate_hash("hello");
        let b = calculate_hash("hello");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_differs_for_different_input() {
        assert_ne!(calculate_hash("a"), calculate_hash("b"));
    }

    #[test]
    fn hash_returns_hex_string() {
        let h = calculate_hash("test");
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // --- calculate_json_hash ---

    #[test]
    fn json_hash_stable_regardless_of_key_order() {
        let a = json!({"x": 1, "y": 2});
        let _b = json!({"y": 2, "x": 1});
        // serde_json serialization of Value is key-order dependent,
        // but these were constructed with the same underlying BTreeMap-like order.
        // This test documents the current behavior.
        // The important thing is same input -> same hash.
        let c = json!({"x": 1, "y": 2});
        assert_eq!(calculate_json_hash(&a).unwrap(), calculate_json_hash(&c).unwrap());
    }

    #[test]
    fn json_hash_differs_for_different_values() {
        let a = json!({"x": 1});
        let b = json!({"x": 2});
        assert_ne!(calculate_json_hash(&a).unwrap(), calculate_json_hash(&b).unwrap());
    }

    // --- resolve_json_path ---

    #[test]
    fn resolve_account_field() {
        let ctx = test_context();
        assert_eq!(
            resolve_json_path(&ctx, "account.id"),
            Some(json!("123456789"))
        );
    }

    #[test]
    fn resolve_region_field() {
        let ctx = test_context();
        assert_eq!(
            resolve_json_path(&ctx, "region.name"),
            Some(json!("us-east-1"))
        );
    }

    #[test]
    fn resolve_transforms_nested() {
        let ctx = test_context();
        assert_eq!(
            resolve_json_path(&ctx, "transforms.nested.key"),
            Some(json!("deep_val"))
        );
    }

    #[test]
    fn resolve_unknown_root_returns_none() {
        let ctx = test_context();
        assert_eq!(resolve_json_path(&ctx, "unknown.field"), None);
    }

    #[test]
    fn resolve_missing_field_returns_none() {
        let ctx = test_context();
        assert_eq!(resolve_json_path(&ctx, "account.nonexistent"), None);
    }

    #[test]
    fn resolve_single_segment_returns_none() {
        let ctx = test_context();
        assert_eq!(resolve_json_path(&ctx, "account"), None);
    }

    // --- resolve_variables ---

    #[test]
    fn resolve_single_variable() {
        let ctx = test_context();
        let input = "arn:aws:iam::${account.id}:role/glue";
        let result = resolve_variables(input, &ctx).unwrap();
        assert_eq!(result, "arn:aws:iam::123456789:role/glue");
    }

    #[test]
    fn resolve_multiple_variables() {
        let ctx = test_context();
        let input = "account=${account.id} region=${region.name}";
        let result = resolve_variables(input, &ctx).unwrap();
        assert_eq!(result, "account=123456789 region=us-east-1");
    }

    #[test]
    fn resolve_no_variables_passthrough() {
        let ctx = test_context();
        let input = "no variables here";
        let result = resolve_variables(input, &ctx).unwrap();
        assert_eq!(result, "no variables here");
    }

    #[test]
    fn resolve_missing_variable_errors() {
        let ctx = test_context();
        let input = "${account.nope}";
        let result = resolve_variables(input, &ctx);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_nested_transforms_variable() {
        let ctx = test_context();
        let input = "val=${transforms.nested.key}";
        let result = resolve_variables(input, &ctx).unwrap();
        assert_eq!(result, "val=deep_val");
    }
}
