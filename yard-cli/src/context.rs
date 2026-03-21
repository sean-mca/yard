use crate::utils;
use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use yaml_rust2::YamlLoader;
use yard_structs::YARDContext;

pub fn find_and_parse_context(start_path: &Path, filename: &str, required: bool) -> Result<Value> {
    let mut current = start_path.to_path_buf();

    loop {
        let target = current.join(filename);
        if target.exists() {
            let content = fs::read_to_string(&target)
                .with_context(|| format!("Failed to read {}", target.display()))?;

            // yaml_rust2 parsing
            let docs = YamlLoader::load_from_str(&content)
                .map_err(|e| anyhow!("YAML Scan Error in {}: {}", target.display(), e))?;

            let doc = docs
                .get(0)
                .ok_or_else(|| anyhow!("{} is empty", target.display()))?;

            return Ok(utils::yaml_to_json(doc));
        }

        if !current.pop() {
            break;
        }
    }

    if required {
        Err(anyhow!(
            "Required context file '{}' not found in {} or parents",
            filename,
            start_path.display()
        ))
    } else {
        Ok(Value::Object(serde_json::Map::new()))
    }
}

pub fn load_context(current_dir: &Path) -> Result<YARDContext> {
    let account = find_and_parse_context(current_dir, "account.yaml", true)?;
    let region = find_and_parse_context(current_dir, "region.yaml", true)?;
    let transforms = find_and_parse_context(current_dir, "transforms.yaml", false)?;

    Ok(YARDContext {
        account,
        region,
        transforms,
    })
}

pub fn find_in_parent_folders(start_path: &Path, filename: &str) -> Option<PathBuf> {
    let mut current = start_path.to_path_buf();
    loop {
        let target = current.join(filename);
        if target.exists() {
            return Some(target);
        }
        if !current.pop() {
            break;
        }
    }
    None
}
