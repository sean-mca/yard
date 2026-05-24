//! Hierarchical context-file resolution (account, region, transforms).
//!
//! All logic lives in [`yard_core::resolve`]; this module re-exports the
//! public helpers for any remaining in-crate callers and houses the
//! integration tests that exercise parent-directory traversal.

pub use yard_core::resolve::{find_and_parse_context, find_in_parent_folders, load_context};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_dir() -> PathBuf {
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("yard_ctx_{}_{id}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    // --- find_in_parent_folders ---

    #[test]
    fn finds_file_in_current_dir() {
        let root = make_temp_dir();
        fs::write(root.join("yard.yaml"), "project: test").unwrap();

        let result = find_in_parent_folders(&root, "yard.yaml");
        assert_eq!(result, Some(root.join("yard.yaml")));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn finds_file_in_parent_dir() {
        let root = make_temp_dir();
        let child = root.join("envs").join("dev");
        fs::create_dir_all(&child).unwrap();
        fs::write(root.join("yard.yaml"), "project: test").unwrap();

        let result = find_in_parent_folders(&child, "yard.yaml");
        assert_eq!(result, Some(root.join("yard.yaml")));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn returns_none_when_not_found() {
        let root = make_temp_dir();

        let result = find_in_parent_folders(&root, "nonexistent.yaml");
        assert!(result.is_none());

        let _ = fs::remove_dir_all(&root);
    }

    // --- find_and_parse_context ---

    #[test]
    fn parses_context_file() {
        let root = make_temp_dir();
        fs::write(root.join("account.yaml"), "id: '123'\nname: dev").unwrap();

        let result = find_and_parse_context(&root, "account.yaml", true).unwrap();
        assert_eq!(result["id"], serde_json::json!("123"));
        assert_eq!(result["name"], serde_json::json!("dev"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn finds_context_in_parent() {
        let root = make_temp_dir();
        let child = root.join("jobs").join("etl");
        fs::create_dir_all(&child).unwrap();
        fs::write(root.join("region.yaml"), "name: us-east-1").unwrap();

        let result = find_and_parse_context(&child, "region.yaml", true).unwrap();
        assert_eq!(result["name"], serde_json::json!("us-east-1"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn required_missing_context_errors() {
        let root = make_temp_dir();

        let result = find_and_parse_context(&root, "account.yaml", true);
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn optional_missing_context_returns_empty() {
        let root = make_temp_dir();

        let result = find_and_parse_context(&root, "transforms.yaml", false).unwrap();
        assert_eq!(result, serde_json::Value::Object(serde_json::Map::new()));

        let _ = fs::remove_dir_all(&root);
    }

    // --- load_context ---

    #[test]
    fn load_context_full() {
        let root = make_temp_dir();
        fs::write(root.join("account.yaml"), "id: '999'").unwrap();
        fs::write(root.join("region.yaml"), "name: eu-west-1").unwrap();
        fs::write(root.join("transforms.yaml"), "suffix: prod").unwrap();

        let ctx = load_context(&root).unwrap();
        assert_eq!(ctx.account["id"], serde_json::json!("999"));
        assert_eq!(ctx.region["name"], serde_json::json!("eu-west-1"));
        assert_eq!(ctx.transforms["suffix"], serde_json::json!("prod"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn load_context_without_transforms() {
        let root = make_temp_dir();
        fs::write(root.join("account.yaml"), "id: '111'").unwrap();
        fs::write(root.join("region.yaml"), "name: us-west-2").unwrap();

        let ctx = load_context(&root).unwrap();
        assert_eq!(ctx.account["id"], serde_json::json!("111"));
        assert_eq!(
            ctx.transforms,
            serde_json::Value::Object(serde_json::Map::new())
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn load_context_inherits_from_parent() {
        let root = make_temp_dir();
        let child = root.join("jobs").join("etl");
        fs::create_dir_all(&child).unwrap();

        // Place context at root, query from child
        fs::write(root.join("account.yaml"), "id: '555'").unwrap();
        fs::write(root.join("region.yaml"), "name: ap-southeast-1").unwrap();

        let ctx = load_context(&child).unwrap();
        assert_eq!(ctx.account["id"], serde_json::json!("555"));
        assert_eq!(ctx.region["name"], serde_json::json!("ap-southeast-1"));

        let _ = fs::remove_dir_all(&root);
    }
}
