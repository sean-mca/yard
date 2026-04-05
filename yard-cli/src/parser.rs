use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "yard")]
#[command(about = "YAML Architecture for Rapid Development", long_about = None)]
#[command(version)]
pub struct Cli {
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
    },

    Apply {
        #[arg(index = 1)]
        directory: Option<String>,
    },
}
