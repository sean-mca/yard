use anyhow::{Context, Result};
use std::path::PathBuf;

const STARTER_YAML: &str = "\
project: my-yard-project

state:
  type: local
  path: .yard/state

providers:
";

pub async fn execute(directory: Option<String>) -> Result<()> {
    let base_path = match directory {
        Some(d) => PathBuf::from(d),
        None => std::env::current_dir().context("Failed to get current directory")?,
    };

    tokio::fs::create_dir_all(&base_path)
        .await
        .with_context(|| format!("Failed to create directory {}", base_path.display()))?;

    let manifest_path = base_path.join("yard.yaml");
    if manifest_path.exists() {
        println!(
            "{} already exists, skipping scaffold.",
            manifest_path.display()
        );
    } else {
        tokio::fs::write(&manifest_path, STARTER_YAML)
            .await
            .with_context(|| format!("Failed to write {}", manifest_path.display()))?;
        println!("Created {}", manifest_path.display());
    }

    let project = yard_core::resolve::resolve_project(&base_path).await?;
    yard_core::init_state_backend(&project.manifest.state, Some(&project.manifest.aws)).await?;

    Ok(())
}
