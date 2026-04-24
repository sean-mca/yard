use clap::{Parser, Subcommand};

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

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new YARD project
    Init {
        #[arg(index = 1)]
        directory: Option<String>,
    },

    Plan {
        #[arg(index = 1)]
        directory: Option<String>,

        /// Only plan a specific job
        #[arg(long)]
        target: Option<String>,
    },

    Apply {
        #[arg(index = 1)]
        directory: Option<String>,

        /// Skip provider deployment (codegen and state only)
        #[arg(long)]
        dry_run: bool,

        /// Skip confirmation prompt
        #[arg(long)]
        auto_approve: bool,

        /// Only apply a specific job
        #[arg(long)]
        target: Option<String>,
    },

    /// Show the generated script for a job
    Show {
        /// The job name to show
        #[arg(index = 1)]
        job_name: String,

        #[arg(index = 2)]
        directory: Option<String>,
    },

    /// Validate all job configurations
    Validate {
        #[arg(index = 1)]
        directory: Option<String>,
    },

    /// Destroy deployed jobs and remove state
    Destroy {
        /// Specific job to destroy (omit to destroy all)
        #[arg(index = 1)]
        job_name: Option<String>,

        #[arg(index = 2)]
        directory: Option<String>,

        /// Skip provider teardown (state and local files only)
        #[arg(long)]
        dry_run: bool,

        /// Skip confirmation prompt
        #[arg(long)]
        auto_approve: bool,
    },

    /// Force-unlock a locked job
    ForceUnlock {
        /// The job name to unlock
        #[arg(index = 1)]
        job_name: String,

        #[arg(index = 2)]
        directory: Option<String>,
    },

    /// List deployment targets (jobs + DAGs) as JSON for CI matrix builders
    List {
        #[command(subcommand)]
        target: ListTarget,
    },
}

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
