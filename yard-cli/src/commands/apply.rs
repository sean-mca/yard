use super::resolve_project;
use anyhow::{Context, Result};
use std::fs;
use yard_core::codegen;
use yard_structs::YardAction;

pub async fn execute(directory: Option<String>) -> Result<Option<YardAction>> {
    let project = resolve_project(directory).await?;
    let mut current_state = project.current_state;

    let diffs = yard_core::calculate_diff(&project.manifest, &current_state);

    if diffs.is_empty() {
        println!("No changes to apply.");
        return Ok(None);
    }

    println!("Applying changes for {}...", project.manifest.project);

    for diff in diffs {
        match diff.diff_type {
            yard_structs::DiffType::Create | yard_structs::DiffType::Modify { .. } => {
                let job_def = project
                    .manifest
                    .jobs
                    .get(&diff.name)
                    .context("Job definition missing during apply")?;

                let script_content = codegen::generate_python_script(&diff.name, job_def)
                    .context("Failed to generate Python script")?;

                let script_hash = yard_core::utils::calculate_hash(&script_content);

                let gen_dir = project.root_dir.join(".yard/generated");
                fs::create_dir_all(&gen_dir)?;
                let script_path = gen_dir.join(format!("{}.py", diff.name));
                fs::write(&script_path, &script_content)?;

                println!("  -> Generated script: .yard/generated/{}.py", diff.name);

                current_state.deployments.insert(
                    diff.name.clone(),
                    yard_structs::Deployment {
                        config_hash: script_hash,
                        config: job_def.config.clone(),
                        status: "generated".to_string(),
                        applied_at: chrono::Utc::now().to_rfc3339(),
                        resources: Vec::new(),
                        env: None,
                    },
                );
            }
            yard_structs::DiffType::Delete => {
                println!("  - Deleting job: {}", diff.name);
                current_state.deployments.remove(&diff.name);

                let script_path = project
                    .root_dir
                    .join(".yard/generated")
                    .join(format!("{}.py", diff.name));
                if script_path.exists() {
                    let _ = fs::remove_file(script_path);
                }
            }
            _ => {}
        }
    }

    // PERSIST UPDATED STATE
    let storage = yard_core::storage::get_storage(&project.manifest.state).await?;
    storage.write(&current_state).await?;
    println!("\nState updated successfully.");

    Ok(None)
}
