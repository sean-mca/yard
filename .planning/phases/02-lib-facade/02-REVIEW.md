---
phase: 02-lib-facade
reviewed: 2026-04-18T12:00:00Z
depth: standard
files_reviewed: 7
files_reviewed_list:
  - yard-core/src/config_merge.rs
  - yard-core/src/dag_lifecycle.rs
  - yard-core/src/diff.rs
  - yard-core/src/lib.rs
  - yard-core/src/orchestrate.rs
  - yard-core/src/parsing.rs
  - yard-core/src/show.rs
findings:
  critical: 0
  warning: 3
  info: 2
  total: 5
status: issues_found
---

# Phase 02: Code Review Report

**Reviewed:** 2026-04-18T12:00:00Z
**Depth:** standard
**Files Reviewed:** 7
**Status:** issues_found

## Summary

The lib.rs facade extraction is well-executed. The 2114-line monolith has been cleanly decomposed into 6 focused modules with lib.rs reduced to a 31-line re-export facade. Module visibility, `crate::` import paths, and error handling with `.context()` are all correct. No `unwrap()` in production code, no `unsafe` blocks. The re-exports in lib.rs faithfully preserve the public API surface so downstream callers (yard-cli) are unaffected.

Three warnings relate to logic bugs in diff comparison functions and a duplicated test helper. Two informational items note a stale doc-comment and a minor inconsistency in `unwrap_or_default` usage for serialization.

## Warnings

### WR-01: compare_json only detects additions/modifications, not deletions

**File:** `yard-core/src/diff.rs:59-69`
**Issue:** `compare_json` iterates over `new_obj` keys and compares against `old_obj`, but never iterates over `old_obj` to find keys that were removed. If a user deletes a config key from their job definition, the change map will not reflect that deletion. This means `DiffType::Modify { changes }` will show an incomplete set of changes -- the hash-based diff still correctly detects the modification, but the human-readable `changes` map shown during `yard plan` will be misleading.
**Fix:**
```rust
fn compare_json(old: &Value, new: &Value) -> HashMap<String, (String, String)> {
    let mut changes = HashMap::new();
    if let (Value::Object(old_obj), Value::Object(new_obj)) = (old, new) {
        for (k, v) in new_obj {
            let old_val = old_obj.get(k).unwrap_or(&Value::Null);
            if old_val != v {
                changes.insert(k.clone(), (old_val.to_string(), v.to_string()));
            }
        }
        // Detect removed keys
        for k in old_obj.keys() {
            if !new_obj.contains_key(k) {
                changes.insert(k.clone(), (old_obj[k].to_string(), "null".to_string()));
            }
        }
    }
    changes
}
```

### WR-02: compare_dag_config has the same deletion-blindness

**File:** `yard-core/src/dag_lifecycle.rs:90-110`
**Issue:** Same issue as WR-01 -- `compare_dag_config` only iterates over the new config object keys, missing any keys that were removed from the old DAG deployment config. The structural diff shown to the user will not reflect removed fields.
**Fix:** Add the same reverse iteration as suggested in WR-01.

### WR-03: Duplicated `make_job` test helper across modules

**File:** `yard-core/src/dag_lifecycle.rs:462-503` and `yard-core/src/orchestrate.rs:480-521`
**Issue:** The `make_job` test helper function is identically duplicated in both `dag_lifecycle::tests` and `orchestrate::tests`. If the `JobDefinition` struct gains or loses a field, both copies must be updated in lockstep or tests will silently diverge or fail to compile. This is a maintenance risk introduced by the extraction -- the monolithic lib.rs had one copy.
**Fix:** Create a `#[cfg(test)]` helper module (e.g., `yard-core/src/test_helpers.rs`) with a `pub(crate)` `make_job` function, then import it from both test modules:
```rust
// In each test module:
use crate::test_helpers::make_job;
```

## Info

### IN-01: Stale doc-comment on parse_partition_by

**File:** `yard-core/src/parsing.rs:96`
**Issue:** The doc-comment says "Extract imports from a job config's imports array" but the function is `parse_partition_by`, which extracts `partition_by` fields -- not imports. This was likely a copy-paste artifact from before the extraction.
**Fix:** Change the doc-comment to:
```rust
/// Extract partition_by columns from a job config.
```

### IN-02: Orphaned doc-comment fragment on str_map_field

**File:** `yard-core/src/parsing.rs:157-158`
**Issue:** Lines 157-158 have two doc-comments stacked: `/// Helper to extract a string->string map field from JSON.` followed by `/// Helper to extract an order_by field: ...`. The first comment belongs to `str_map_field` (line 174) but is separated from it by the `order_by_field` function definition. It appears the order of functions was rearranged without moving the doc-comment.
**Fix:** Move `/// Helper to extract a string->string map field from JSON.` to directly above `fn str_map_field` on line 174, and keep `/// Helper to extract an order_by field...` above `fn order_by_field` on line 159.

---

_Reviewed: 2026-04-18T12:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
