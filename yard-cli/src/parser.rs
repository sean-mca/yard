use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "yard")]
#[command(about = "YAML Architecture for Rapid Development", long_about = None)]
#[command(version)] // Pulls version from Cargo.toml
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Sets the level of verbosity
    #[arg(short, long, global = true, default_value_t = 0, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new YARD project
    Init {
        #[arg(index = 1)]
        directory: Option<String>,
    },
}
