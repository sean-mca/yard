// src/main.rs
#[tokio::main]
async fn main() {
    if let Err(e) = yard::run().await {
        eprintln!("YARD Error: {:?}", e);
        std::process::exit(1);
    }
}
