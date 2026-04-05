pub mod commands;
pub mod context;
pub mod parser;
pub mod utils;

use anyhow::Result;
use clap::Parser;

pub async fn run() -> Result<()> {
    let cli = parser::Cli::parse();

    match cli.command {
        parser::Commands::Init { directory } => commands::init::execute(directory).await?,
        parser::Commands::Plan { directory } => commands::plan::execute(directory).await?,
        parser::Commands::Apply { directory, dry_run } => {
            commands::apply::execute(directory, dry_run).await?
        }
        parser::Commands::Validate { directory } => commands::validate::execute(directory).await?,
        parser::Commands::Destroy {
            job_name,
            directory,
            dry_run,
        } => commands::destroy::execute(job_name, directory, dry_run).await?,
        parser::Commands::ForceUnlock {
            job_name,
            directory,
        } => commands::force_unlock::execute(job_name, directory).await?,
    };

    Ok(())
}
