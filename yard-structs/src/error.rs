use serde::{Deserialize, Serialize};

/// A single validation error found during config or DAG validation.
///
/// Carries the field path that failed validation and a human-readable
/// message describing the constraint that was violated. Displayed as
/// `"{field}: {message}"`.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ValidationError {
    /// Dot-separated path to the invalid field (e.g. `"sources[0].path"`).
    pub field: String,
    /// Human-readable description of the validation failure.
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.message)
    }
}
