pub fn calculate_hash(config: &serde_json::Value) -> String {
    let serialized = serde_json::to_string(config).unwrap_or_default();
    blake3::hash(serialized.as_bytes()).to_hex().to_string()
}
