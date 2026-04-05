pub mod commands;
pub mod context;
pub mod parser;
pub mod utils;

use anyhow::Result;
use clap::Parser;

pub async fn run() -> Result<()> {
    let cli = parser::Cli::parse();

    let action = match cli.command {
        parser::Commands::Init { directory } => commands::init::execute(directory)?,
        parser::Commands::Plan { directory } => commands::plan::execute(directory).await?,
        parser::Commands::Apply { directory } => commands::apply::execute(directory).await?,
    };

    if let Some(work) = action {
        yard_core::dispatch(work).await?;
    }

    Ok(())
}
