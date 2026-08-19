//! CLI argument definitions for YARD.
//!
//! Uses [`clap`] derive macros to define the top-level [`Cli`] struct,
//! the [`Commands`] enum of subcommands, and the [`ListTarget`] nested
//! subcommand. Doc comments on variants double as `--help` text.

use clap::{Parser, Subcommand};

/// Top-level CLI arguments parsed by [`clap`].
///
/// Global flags (`--no-color`, `--colorblind`) are propagated to all
/// subcommands.
#[derive(Parser)]
#[command(name = "yard")]
#[command(about = "YAML Architecture for Rapid Development", long_about = None)]
#[command(version)]
pub struct Cli {
    /// Disable colored output
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Use colorblind-friendly palette (cyan/blue/magenta instead of green/yellow/red)
    #[arg(long, global = true)]
    pub colorblind: bool,

    /// The subcommand to execute.
    #[command(subcommand)]
    pub command: Commands,
}

/// Available YARD subcommands.
///
/// Each variant maps to a handler in [`crate::commands`].
#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new YARD project
    Init {
        /// Project directory (defaults to current working directory).
        #[arg(index = 1)]
        directory: Option<String>,
    },

    /// Preview infrastructure changes without applying them
    Plan {
        /// Project directory (defaults to current working directory).
        #[arg(index = 1)]
        directory: Option<String>,

        /// Only plan a specific job
        #[arg(long, conflicts_with = "dir")]
        target: Option<String>,

        /// Scope to all jobs under a directory subtree
        #[arg(long, conflicts_with = "target")]
        dir: Option<String>,
    },

    /// Apply infrastructure changes (codegen + deploy)
    Apply {
        /// Project directory (defaults to current working directory).
        #[arg(index = 1)]
        directory: Option<String>,

        /// Skip provider deployment (codegen and state only)
        #[arg(long)]
        dry_run: bool,

        /// Skip confirmation prompt
        #[arg(long)]
        auto_approve: bool,

        /// Only apply a specific job
        #[arg(long, conflicts_with = "dir")]
        target: Option<String>,

        /// Scope to all jobs under a directory subtree
        #[arg(long, conflicts_with = "target")]
        dir: Option<String>,
    },

    /// Show the generated script for a job
    Show {
        /// The job name to show
        #[arg(index = 1)]
        job_name: String,

        /// Project directory (defaults to current working directory).
        #[arg(index = 2)]
        directory: Option<String>,
    },

    /// Validate all job configurations
    Validate {
        /// Project directory (defaults to current working directory).
        #[arg(index = 1)]
        directory: Option<String>,

        /// Only validate a specific job
        #[arg(long, conflicts_with = "dir")]
        target: Option<String>,

        /// Scope to all jobs under a directory subtree
        #[arg(long, conflicts_with = "target")]
        dir: Option<String>,
    },

    /// Destroy deployed jobs and remove state
    Destroy {
        /// Specific job to destroy (omit to destroy all)
        #[arg(index = 1, conflicts_with = "dir")]
        job_name: Option<String>,

        /// Project directory (defaults to current working directory).
        #[arg(index = 2)]
        directory: Option<String>,

        /// Skip provider teardown (state and local files only)
        #[arg(long)]
        dry_run: bool,

        /// Skip confirmation prompt
        #[arg(long)]
        auto_approve: bool,

        /// Scope destroy to all jobs under a directory subtree
        #[arg(long, conflicts_with = "job_name")]
        dir: Option<String>,
    },

    /// Force-unlock a locked job
    ForceUnlock {
        /// The job name to unlock
        #[arg(index = 1)]
        job_name: String,

        /// Project directory (defaults to current working directory).
        #[arg(index = 2)]
        directory: Option<String>,
    },

    /// List deployment targets (jobs + DAGs) as JSON for CI matrix builders
    List {
        /// The list sub-command to run.
        #[command(subcommand)]
        target: ListTarget,
    },
}

/// Sub-commands for `yard list`.
#[derive(Subcommand)]
pub enum ListTarget {
    /// Emit all deployment targets as a JSON array to stdout
    Targets {
        /// Project directory (defaults to current working directory)
        #[arg(index = 1)]
        directory: Option<String>,

        /// Accepted for forward-compatibility; JSON is the only output mode in v1.4
        #[arg(long)]
        json: bool,
    },
}
