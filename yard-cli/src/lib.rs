//! Thin CLI wrapper for the YARD engine.
//!
//! This crate parses command-line arguments via [`clap`], delegates all
//! business logic to [`yard_core`], and formats output for the terminal.
//! No domain logic lives here -- see [`yard_core`] for orchestration,
//! codegen, storage, and validation.
//!
//! # Modules
//!
//! - [`commands`] -- Per-subcommand handlers (`apply`, `plan`, `show`, etc.)
//! - [`context`] -- Re-exports of context-loading helpers from `yard_core`
//! - [`parser`] -- Clap argument definitions ([`parser::Cli`], [`parser::Commands`])
//! - [`utils`] -- Terminal color helpers and user-confirmation prompt

pub mod commands;
pub mod context;
pub mod parser;
pub mod utils;

use anyhow::Result;
use clap::Parser;

/// Parse CLI arguments and dispatch to the appropriate command handler.
///
/// Respects `--no-color` / `NO_COLOR` env var for plain output and
/// `--colorblind` for an accessible palette.
///
/// # Errors
///
/// Returns an error if the dispatched command fails (e.g. project
/// resolution, state access, provider operations, or I/O errors).
pub async fn run() -> Result<()> {
    let cli = parser::Cli::parse();

    // Respect --no-color flag and NO_COLOR env var (https://no-color.org)
    if cli.no_color || std::env::var("NO_COLOR").is_ok() {
        utils::disable_color();
    } else if cli.colorblind {
        utils::enable_colorblind_mode();
    }

    match cli.command {
        parser::Commands::Init { directory } => commands::init::execute(directory).await?,
        parser::Commands::Plan { directory, target } => {
            commands::plan::execute(directory, target).await?
        }
        parser::Commands::Apply {
            directory,
            dry_run,
            auto_approve,
            target,
        } => commands::apply::execute(directory, dry_run, auto_approve, target).await?,
        parser::Commands::Show {
            job_name,
            directory,
        } => commands::show::execute(job_name, directory).await?,
        parser::Commands::Validate { directory } => commands::validate::execute(directory).await?,
        parser::Commands::Destroy {
            job_name,
            directory,
            dry_run,
            auto_approve,
        } => commands::destroy::execute(job_name, directory, dry_run, auto_approve).await?,
        parser::Commands::ForceUnlock {
            job_name,
            directory,
        } => commands::force_unlock::execute(job_name, directory).await?,
        parser::Commands::List { target } => match target {
            parser::ListTarget::Targets { directory, json } => {
                commands::list::execute(directory, json).await?
            }
        },
    };

    Ok(())
}
