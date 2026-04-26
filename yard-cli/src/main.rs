// src/main.rs
#![warn(clippy::unwrap_used, clippy::expect_used)]

#[tokio::main]
async fn main() {
    if let Err(e) = yard::run().await {
        eprintln!("YARD Error: {e:#}");
        std::process::exit(1);
    }
}
