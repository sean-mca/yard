//! YARD CLI entry point.
//!
//! Delegates to [`yard::run`] and prints any top-level error to stderr
//! before exiting with a non-zero status code.

#![warn(clippy::unwrap_used, clippy::expect_used)]

#[tokio::main]
async fn main() {
    if let Err(e) = yard::run().await {
        eprintln!("YARD Error: {e:#}");
        std::process::exit(1);
    }
}
