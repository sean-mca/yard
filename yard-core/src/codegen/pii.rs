//! PII masking codegen: renders the `EntityDetector.detect()` block for
//! AWS Glue jobs that need sensitive-data redaction.
//!
//! The rendered Python block converts the sink DataFrame to a
//! `DynamicFrame`, calls `EntityDetector.detect()` with fine-grained
//! `detectionParameters` (REDACT action, `"****"` mask), converts back
//! to a DataFrame, and drops the `DetectedEntities` metadata column.
//! All intermediate variables use the `_yard_pii_` prefix to avoid
//! collisions with user code (GEN-06).

use std::fmt::Write;

/// Render the PII masking block for the given entity types.
///
/// Emits a DynamicFrame conversion sandwich around an
/// `EntityDetector.detect()` call, then drops the metadata column.
/// Uses `_yard_pii_` prefixed variables to avoid collisions (GEN-06).
///
/// Returns the Python code block as an indented string. Cannot fail
/// because validation (Phase 60) has already rejected invalid input
/// (D-05).
#[must_use]
pub(super) fn render_pii(mask_pii: &[String], source_var: &str) -> String {
    let mut out = String::with_capacity(256);

    // DynamicFrame conversion (GEN-02)
    let _ = writeln!(
        out,
        "    _yard_pii_dyf = DynamicFrame.fromDF({source_var}, glueContext, \"_yard_pii\")"
    );

    // EntityDetector.detect() call with detectionParameters (GEN-01)
    let _ = writeln!(out, "    _yard_pii_dyf = EntityDetector.detect(");
    let _ = writeln!(out, "        _yard_pii_dyf,");
    let _ = writeln!(out, "        {{");
    for (i, entity) in mask_pii.iter().enumerate() {
        let comma = if i + 1 < mask_pii.len() { "," } else { "" };
        let _ = writeln!(
            out,
            "            \"{entity}\": [{{\"action\": \"REDACT\", \"actionOptions\": {{\"redactText\": \"****\"}}}}]{comma}"
        );
    }
    let _ = writeln!(out, "        }},");
    let _ = writeln!(out, "        \"DetectedEntities\"");
    let _ = writeln!(out, "    )");

    // Convert back to DataFrame and drop metadata column (GEN-02, GEN-03)
    let _ = writeln!(out, "    {source_var} = _yard_pii_dyf.toDF()");
    let _ = write!(
        out,
        "    {source_var} = {source_var}.drop(\"DetectedEntities\")"
    );

    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn render_pii_single_entity() {
        let output = render_pii(&["USA_SSN".to_string()], "df_events");

        assert!(output.contains("DynamicFrame.fromDF(df_events"));
        assert!(output.contains("EntityDetector.detect("));
        assert!(output.contains("\"USA_SSN\""));
        assert!(output.contains("REDACT"));
        assert!(output.contains("****"));
        assert!(output.contains("_yard_pii_dyf"));
        assert!(output.contains(".toDF()"));
        assert!(output.contains(".drop(\"DetectedEntities\")"));
    }

    #[test]
    fn render_pii_multiple_entities() {
        let output = render_pii(
            &[
                "USA_SSN".to_string(),
                "CREDIT_CARD".to_string(),
                "EMAIL".to_string(),
            ],
            "df_events",
        );

        // All three entity names appear in the detectionParameters dict
        assert!(output.contains("\"USA_SSN\""));
        assert!(output.contains("\"CREDIT_CARD\""));
        assert!(output.contains("\"EMAIL\""));

        // Trailing commas on first two entries but not the last
        assert!(output.contains("\"USA_SSN\": [{\"action\": \"REDACT\", \"actionOptions\": {\"redactText\": \"****\"}}],"));
        assert!(output.contains("\"CREDIT_CARD\": [{\"action\": \"REDACT\", \"actionOptions\": {\"redactText\": \"****\"}}],"));
        // Last entry has no trailing comma
        let email_line = output
            .lines()
            .find(|l| l.contains("\"EMAIL\""))
            .expect("EMAIL line should exist");
        assert!(
            !email_line.ends_with(','),
            "last entity entry should not have trailing comma"
        );
    }

    #[test]
    fn render_pii_uses_yard_prefix() {
        let output = render_pii(&["USA_SSN".to_string()], "df_events");

        // All intermediate variables start with _yard_pii_ (GEN-06)
        assert!(output.contains("_yard_pii_dyf"));
        assert!(output.contains("_yard_pii\""));

        // No bare "dyf" or "pii_frame" variables
        let has_bare_dyf = output.lines().any(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("dyf ")
                || trimmed.starts_with("dyf=")
                || trimmed.contains(" dyf ")
        });
        assert!(!has_bare_dyf, "should not have bare 'dyf' variable");

        let has_pii_frame = output.contains("pii_frame");
        assert!(!has_pii_frame, "should not have 'pii_frame' variable");
    }

    #[test]
    fn render_pii_source_var_propagates() {
        let output = render_pii(&["USA_SSN".to_string()], "df_enriched");

        assert!(output.contains("DynamicFrame.fromDF(df_enriched"));
        assert!(output.contains("df_enriched = _yard_pii_dyf.toDF()"));
        assert!(output.contains("df_enriched = df_enriched.drop("));
    }

    #[test]
    fn render_pii_indentation() {
        let output = render_pii(&["USA_SSN".to_string()], "df_events");

        // Every non-empty line starts with at least 4 spaces (inside
        // run() function body context)
        for line in output.lines() {
            if !line.is_empty() {
                assert!(
                    line.starts_with("    "),
                    "line should start with 4 spaces: {line:?}"
                );
            }
        }
    }
}
