pub mod commands;
pub mod parser;

use anyhow::Result;
use clap::Parser;

pub fn run() -> Result<()> {
    let cli = parser::Cli::parse();

    let action = match cli.command {
        // Only 'directory' exists now, so this is clean
        parser::Commands::Init { directory } => commands::init::execute(directory)?,
    };

    if let Some(work) = action {
        yard_core::dispatch(work)?;
    }

    Ok(())
}
