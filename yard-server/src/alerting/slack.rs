//! Slack Incoming Webhook delivery for drift alerts.
//!
//! `post_slack_alert` is a free async function (not a trait method — see
//! CONTEXT.md D-13). Single attempt, no retry — the call site logs and drops
//! failures (D-12). `alert_last_sent_at` is persisted by the caller only on
//! success (plan 08-05).
//!
//! `build_slack_payload` is split out so the Slack Blocks JSON shape is
//! unit-testable without any HTTP client.

use std::time::Duration;

use serde_json::Value;

use crate::types::DriftData;

/// Maximum number of drifted job names to enumerate in the Slack section block.
/// Beyond this we append a "… and N more" footer to keep the message compact.
const JOB_LIST_CAP: usize = 20;

/// Request timeout for the outbound Slack POST (D-14).
const SLACK_TIMEOUT_SECS: u64 = 10;

/// POST a Slack Blocks notification to `webhook_url`. Single attempt, no retry.
/// Failure handling is the caller's responsibility — on `Err`, the caller
/// logs via `tracing::warn!` and does NOT update `alert_last_sent_at`
/// (see CONTEXT.md D-12).
#[allow(dead_code)] // Called by plan 08-05 drift_poll_loop alert block.
pub async fn post_slack_alert(
    webhook_url: &str,
    drift: &DriftData,
    threshold: u32,
) -> Result<(), reqwest::Error> {
    let payload = build_slack_payload(drift, threshold);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(SLACK_TIMEOUT_SECS))
        .build()?;
    let resp = client
        .post(webhook_url)
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?;
    resp.error_for_status()?;
    Ok(())
}

/// Build the Slack Blocks payload. Pure-ish — the only impurity is
/// `chrono::Utc::now()` for the context timestamp. Separated from
/// `post_slack_alert` so payload shape tests don't need a live webhook.
#[allow(dead_code)] // Called by post_slack_alert above.
pub fn build_slack_payload(drift: &DriftData, threshold: u32) -> Value {
    let job_lines: Vec<String> = drift
        .items
        .iter()
        .take(JOB_LIST_CAP)
        .map(|i| format!("• {}", i.name))
        .collect();

    let mut section_text = format!(
        "*{} jobs drifted* (threshold: {})\n{}",
        drift.drifted,
        threshold,
        job_lines.join("\n"),
    );
    if drift.items.len() > JOB_LIST_CAP {
        let remaining = drift.items.len() - JOB_LIST_CAP;
        section_text.push_str(&format!("\n… and {remaining} more"));
    }

    serde_json::json!({
        "blocks": [
            {
                "type": "header",
                "text": { "type": "plain_text", "text": ":warning: yard drift alert" }
            },
            {
                "type": "section",
                "text": { "type": "mrkdwn", "text": section_text }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": format!("Detected {}", chrono::Utc::now().to_rfc3339())
                    }
                ]
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DriftItem, DriftType};

    fn drift(count: usize) -> DriftData {
        let items = (0..count)
            .map(|i| DriftItem {
                name: format!("job-{i}"),
                environment: "dev".into(),
                region: "us-east-1".into(),
                drift_type: DriftType::Modified,
                fields_changed: vec![],
                old_config: None,
                new_config: None,
            })
            .collect();
        DriftData {
            items,
            in_sync: 0,
            drifted: count as u32,
        }
    }

    #[test]
    fn payload_has_three_blocks() {
        let payload = build_slack_payload(&drift(3), 1);
        let blocks = payload["blocks"].as_array().expect("blocks is array");
        assert_eq!(blocks.len(), 3);
    }

    #[test]
    fn payload_header_block_has_expected_text() {
        let payload = build_slack_payload(&drift(3), 1);
        assert_eq!(payload["blocks"][0]["type"], "header");
        assert_eq!(payload["blocks"][0]["text"]["type"], "plain_text");
        assert_eq!(
            payload["blocks"][0]["text"]["text"],
            ":warning: yard drift alert"
        );
    }

    #[test]
    fn payload_section_block_contains_count_threshold_and_job_names() {
        let payload = build_slack_payload(&drift(3), 2);
        let section_text = payload["blocks"][1]["text"]["text"]
            .as_str()
            .expect("section text is str");
        assert!(
            section_text.contains("*3 jobs drifted*"),
            "expected count header, got: {section_text}"
        );
        assert!(
            section_text.contains("(threshold: 2)"),
            "expected threshold, got: {section_text}"
        );
        assert!(section_text.contains("• job-0"));
        assert!(section_text.contains("• job-1"));
        assert!(section_text.contains("• job-2"));
    }

    #[test]
    fn payload_context_block_has_timestamp() {
        let payload = build_slack_payload(&drift(1), 1);
        assert_eq!(payload["blocks"][2]["type"], "context");
        let ctx_text = payload["blocks"][2]["elements"][0]["text"]
            .as_str()
            .expect("context text is str");
        assert!(
            ctx_text.starts_with("Detected "),
            "expected 'Detected <timestamp>', got: {ctx_text}"
        );
        assert!(
            ctx_text.contains('T'),
            "expected RFC3339 'T' separator, got: {ctx_text}"
        );
    }

    #[test]
    fn payload_truncates_long_job_lists_with_ellipsis_footer() {
        let payload = build_slack_payload(&drift(25), 1);
        let section_text = payload["blocks"][1]["text"]["text"]
            .as_str()
            .expect("section text is str");
        assert!(
            section_text.contains("… and 5 more"),
            "expected '… and 5 more' footer, got: {section_text}"
        );
        assert!(
            !section_text.contains("job-24"),
            "truncation failed — job-24 should not be present"
        );
        assert!(
            section_text.contains("job-19"),
            "expected job-19 at cap boundary, got: {section_text}"
        );
    }

    #[test]
    fn payload_does_not_append_ellipsis_when_list_fits() {
        let payload = build_slack_payload(&drift(20), 1);
        let section_text = payload["blocks"][1]["text"]["text"]
            .as_str()
            .expect("section text is str");
        assert!(
            !section_text.contains("… and"),
            "unexpected truncation footer at cap, got: {section_text}"
        );
    }

    #[tokio::test]
    async fn post_slack_alert_returns_err_on_invalid_url() {
        let d = drift(1);
        let result = post_slack_alert("not-a-url", &d, 1).await;
        assert!(result.is_err(), "expected Err on invalid URL, got Ok");
    }
}
