use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DiffType {
    Create,
    Modify {
        changes: HashMap<String, (String, String)>,
    },
    Delete,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobDiff {
    pub name: String,
    pub diff_type: DiffType,
    pub old_hash: Option<String>,
    pub new_hash: Option<String>,
}
