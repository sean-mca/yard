pub mod components;
pub mod dashboard;
pub mod drift;
pub mod environments;
pub mod fetch;
pub mod jobs;
pub mod login;
pub mod metrics;
pub mod search;
pub mod settings;
pub mod skeleton;
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

/// Percent-encode a string for safe inclusion in a URL query parameter or
/// path segment. Encodes all characters except unreserved characters
/// (A-Z, a-z, 0-9, `-`, `_`, `.`, `~`) per RFC 3986 section 2.3.
///
/// This is a stdlib-only implementation that avoids pulling in a percent-
/// encoding crate for a simple task (per CLAUDE.md "prefer stdlib").
pub fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 3);
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(char::from(HEX_CHARS[(byte >> 4) as usize]));
                out.push(char::from(HEX_CHARS[(byte & 0x0F) as usize]));
            }
        }
    }
    out
}

const HEX_CHARS: [u8; 16] = *b"0123456789ABCDEF";
