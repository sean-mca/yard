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
        parser::Commands::Apply { directory } => commands::apply::execute(directory).await?,
        parser::Commands::Validate { directory } => commands::validate::execute(directory).await?,
    };

    Ok(())
}
