// src/main.rs
fn main() {
    if let Err(e) = yard::run() {
        eprintln!("YARD Error: {:?}", e);
        std::process::exit(1);
    }
}
