pub mod components;
pub mod dashboard;
pub mod drift;
pub mod fetch;
pub mod jobs;
pub mod login;
pub mod metrics;
pub mod settings;
pub mod sheet;
pub mod sidebar;

// Phase 7 additions
pub mod connection_indicator;
#[cfg(target_arch = "wasm32")]
pub mod connection;

/// API base URL. Set YARD_API_BASE at compile time for non-default setups.
/// Defaults to "http://127.0.0.1:3001" for local dev with dx serve.
/// In production (single-port), set to "" for relative URLs.
pub fn api_base() -> &'static str {
    option_env!("YARD_API_BASE").unwrap_or("http://127.0.0.1:3001")
}
