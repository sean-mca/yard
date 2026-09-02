//! Per-environment plan orchestration for chatOps webhook events.

#![allow(dead_code)]

use std::path::Path;

use yard_structs::{DiffType, DiscoveredEnvironment, JobDiff};

pub const PLAN_COMMENT_MARKER: &str = "<!-- yard-plan-comment -->";

const GITHUB_COMMENT_MAX_LEN: usize = 65_536;
const COMMENT_TEMPLATE_OVERHEAD: usize = 200;
const TRUNCATION_NOTICE_LEN: usize = 200;

pub struct EnvPlanResult {
    pub env_name: String,
    pub diffs: Result<Vec<JobDiff>, String>,
}

/// Filter environments to only those affected by the changed files.
///
/// Cascade rules (D-03):
/// - Root `yard.yaml` change -> all environments
/// - `{env}/account.yaml` change -> that environment
/// - Any file under `{env}/` -> that environment
pub fn filter_affected_environments(
    environments: &[DiscoveredEnvironment],
    changed_files: &[String],
) -> Vec<DiscoveredEnvironment> {
    if changed_files.is_empty() {
        return Vec::new();
    }

    // Root yard.yaml change expands to ALL environments
    if changed_files.iter().any(|f| f == "yard.yaml") {
        return environments.to_vec();
    }

    let mut affected = Vec::new();
    for env in environments {
        let prefix = format!("{}/", env.name);
        if changed_files.iter().any(|f| f.starts_with(&prefix)) {
            affected.push(env.clone());
        }
    }
    affected
}

/// Plan a single environment directory (resolve + diff).
#[allow(dead_code)]
async fn plan_single_env(env_path: &Path) -> Result<Vec<JobDiff>, String> {
    let project = yard_core::resolve::resolve_project(env_path)
        .await
        .map_err(|e| format!("resolve failed: {e}"))?;
    let plugin_host_config = yard_core::plugin_host::PluginHostConfig {
        plugins_dir: env_path.join(".yard/plugins"),
        lock_file_path: Some(env_path.join("yard.lock")),
        ..Default::default()
    };
    yard_core::calculate_diff(&project.manifest, &project.current_state, &plugin_host_config)
        .await
        .map_err(|e| format!("diff failed: {e}"))
}

/// Run plans in parallel for each affected environment.
#[allow(dead_code)]
pub async fn run_per_env_plans(
    environments: Vec<DiscoveredEnvironment>,
    workdir: &Path,
    target_filter: Option<&str>,
) -> Vec<EnvPlanResult> {
    let mut join_set = tokio::task::JoinSet::new();

    for env in environments {
        let env_path = workdir.join(&env.name);
        let env_name = env.name.clone();
        let target = target_filter.map(|s| s.to_string());

        join_set.spawn(async move {
            let result = plan_single_env(&env_path).await;
            let result = match (result, target) {
                (Ok(diffs), Some(t)) => {
                    Ok(diffs.into_iter().filter(|d| d.name == t).collect())
                }
                (other, _) => other,
            };
            EnvPlanResult {
                env_name,
                diffs: result,
            }
        });
    }

    let mut results = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(env_result) => results.push(env_result),
            Err(e) => {
                tracing::error!(error = %e, "plan task panicked");
                results.push(EnvPlanResult {
                    env_name: "unknown".to_string(),
                    diffs: Err(format!("task panicked: {e}")),
                });
            }
        }
    }

    results.sort_by(|a, b| a.env_name.cmp(&b.env_name));
    results
}

/// Format per-environment plan results into a structured GitHub comment.
pub fn format_per_env_comment(
    results: &[EnvPlanResult],
    head_sha: &str,
    dashboard_url: Option<&str>,
    _plan_id: Option<&str>,
) -> String {
    let short_sha = &head_sha[..std::cmp::min(7, head_sha.len())];
    let mut output = format!("{PLAN_COMMENT_MARKER}\n### yard plan (SHA: `{short_sha}`)\n\n");

    let max_body = GITHUB_COMMENT_MAX_LEN
        .saturating_sub(COMMENT_TEMPLATE_OVERHEAD)
        .saturating_sub(TRUNCATION_NOTICE_LEN);

    let mut sorted: Vec<&EnvPlanResult> = results.iter().collect();
    sorted.sort_by(|a, b| a.env_name.cmp(&b.env_name));

    for result in sorted {
        let section = format_env_section(result);

        if output.len() + section.len() > max_body {
            let notice = match dashboard_url {
                Some(url) => format!(
                    "\n\n---\n**Output truncated.** [View full output]({url})\n"
                ),
                None => "\n\n---\n**Output truncated.**\n".to_string(),
            };
            output.push_str(&notice);
            return output;
        }

        output.push_str(&section);
    }

    output
}

fn format_env_section(result: &EnvPlanResult) -> String {
    match &result.diffs {
        Ok(diffs) if diffs.is_empty() => {
            format!("**{}**: No changes\n\n", result.env_name)
        }
        Ok(diffs) => {
            let mut section = format!(
                "<details><summary><strong>{}</strong>: {} change(s)</summary>\n\n```\n",
                result.env_name,
                diffs.len()
            );
            for diff in diffs {
                match &diff.diff_type {
                    DiffType::Create => {
                        section.push_str(&format!("  + Create job [{}]\n", diff.name));
                    }
                    DiffType::Modify { changes } => {
                        section.push_str(&format!("  ~ Modify job [{}]\n", diff.name));
                        for (key, (old, new)) in changes {
                            section.push_str(&format!("      {key} : {old} -> {new}\n"));
                        }
                    }
                    DiffType::Delete => {
                        section.push_str(&format!("  - Delete job [{}]\n", diff.name));
                    }
                    _ => {
                        section.push_str(&format!("  ? Changed job [{}]\n", diff.name));
                    }
                }
            }
            section.push_str("```\n\n</details>\n\n");
            section
        }
        Err(e) => {
            format!("**{}**: :x: Plan failed: {}\n\n", result.env_name, e)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_env(name: &str) -> DiscoveredEnvironment {
        DiscoveredEnvironment {
            name: name.to_string(),
            account_id: None,
            role_arn: None,
            regions: vec![],
        }
    }

    fn make_diff(name: &str, diff_type: DiffType) -> JobDiff {
        JobDiff {
            name: name.to_string(),
            diff_type,
            old_hash: None,
            new_hash: None,
        }
    }

    #[test]
    fn test_filter_no_changes() {
        let envs = vec![make_env("production")];
        let result = filter_affected_environments(&envs, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_filter_env_specific_change() {
        let envs = vec![make_env("production"), make_env("staging")];
        let changed = vec!["production/us-east-1/jobs/foo/config.yaml".to_string()];
        let result = filter_affected_environments(&envs, &changed);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "production");
    }

    #[test]
    fn test_filter_root_yaml_expands_all() {
        let envs = vec![make_env("production"), make_env("staging"), make_env("dev")];
        let changed = vec!["yard.yaml".to_string()];
        let result = filter_affected_environments(&envs, &changed);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_filter_account_yaml_expands_account() {
        let envs = vec![make_env("production"), make_env("staging")];
        let changed = vec!["production/account.yaml".to_string()];
        let result = filter_affected_environments(&envs, &changed);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "production");
    }

    #[test]
    fn test_format_no_changes_env() {
        let results = vec![EnvPlanResult {
            env_name: "dev".to_string(),
            diffs: Ok(vec![]),
        }];
        let output = format_per_env_comment(&results, "abc1234567", None, None);
        assert!(output.contains("No changes"));
    }

    #[test]
    fn test_format_with_diffs() {
        let results = vec![EnvPlanResult {
            env_name: "production".to_string(),
            diffs: Ok(vec![make_diff("etl-pipeline", DiffType::Create)]),
        }];
        let output = format_per_env_comment(&results, "abc1234567", None, None);
        assert!(output.contains("<details>"));
        assert!(output.contains("1 change(s)"));
        assert!(output.contains("+ Create job [etl-pipeline]"));
    }

    #[test]
    fn test_format_modify_diffs() {
        let mut changes = BTreeMap::new();
        changes.insert("timeout".to_string(), ("30".to_string(), "60".to_string()));
        let results = vec![EnvPlanResult {
            env_name: "staging".to_string(),
            diffs: Ok(vec![make_diff("job-a", DiffType::Modify { changes })]),
        }];
        let output = format_per_env_comment(&results, "abc1234567", None, None);
        assert!(output.contains("~ Modify job [job-a]"));
        assert!(output.contains("timeout : 30 -> 60"));
    }

    #[test]
    fn test_format_error_env() {
        let results = vec![EnvPlanResult {
            env_name: "broken".to_string(),
            diffs: Err("resolve failed: missing yard.yaml".to_string()),
        }];
        let output = format_per_env_comment(&results, "abc1234567", None, None);
        assert!(output.contains(":x: Plan failed"));
        assert!(output.contains("missing yard.yaml"));
    }

    #[test]
    fn test_format_marker_first_line() {
        let results = vec![EnvPlanResult {
            env_name: "dev".to_string(),
            diffs: Ok(vec![]),
        }];
        let output = format_per_env_comment(&results, "abc1234567", None, None);
        assert!(output.starts_with(PLAN_COMMENT_MARKER));
    }

    #[test]
    fn test_format_truncation_with_dashboard_url() {
        let big_diffs: Vec<JobDiff> = (0..5000)
            .map(|i| make_diff(&format!("job-{i}"), DiffType::Create))
            .collect();
        let results = vec![EnvPlanResult {
            env_name: "production".to_string(),
            diffs: Ok(big_diffs),
        }];
        let output = format_per_env_comment(
            &results,
            "abc1234567",
            Some("https://yard.example.com"),
            Some("plan-123"),
        );
        assert!(output.contains("Output truncated"));
        assert!(output.contains("https://yard.example.com"));
    }

    #[test]
    fn test_format_truncation_without_dashboard_url() {
        let big_diffs: Vec<JobDiff> = (0..5000)
            .map(|i| make_diff(&format!("job-{i}"), DiffType::Create))
            .collect();
        let results = vec![EnvPlanResult {
            env_name: "production".to_string(),
            diffs: Ok(big_diffs),
        }];
        let output = format_per_env_comment(&results, "abc1234567", None, None);
        assert!(output.contains("Output truncated"));
        assert!(!output.contains("View full output"));
    }

    #[tokio::test]
    async fn test_run_per_env_plans_empty() {
        let results = run_per_env_plans(vec![], Path::new("/tmp"), None).await;
        assert!(results.is_empty());
    }

    #[test]
    fn test_target_filter_removes_non_matching_diffs() {
        let diffs = vec![
            make_diff("job-a", DiffType::Create),
            make_diff("job-b", DiffType::Delete),
            make_diff("job-c", DiffType::Create),
        ];
        let filtered: Vec<JobDiff> = diffs
            .into_iter()
            .filter(|d| d.name == "job-b")
            .collect();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "job-b");
    }
}
