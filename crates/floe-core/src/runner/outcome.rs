//! Run-outcome helpers for connector-owned execution adapters.
//!
//! Connectors (Airflow, Dagster, Kubernetes, …) use these types and helpers
//! to map their execution results onto Floe's normalized [`RunStatus`] before
//! building a [`RunOutcome`] or writing a summary report.

use crate::report::RunStatus;

// ---------------------------------------------------------------------------
// ConnectorRunStatus
// ---------------------------------------------------------------------------

/// The terminal state of a connector-managed run as observed by the connector.
///
/// Connectors use this enum to represent their own job/task states before
/// mapping them to the Floe-normalized [`RunStatus`].  The mapping is
/// deliberately coarse: fine-grained outcomes (e.g. `Rejected`, `Aborted`)
/// can only be inferred from the in-pod [`parse_run_status_from_logs`] helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorRunStatus {
    /// The job/task completed without infrastructure error.
    ///
    /// The actual logical outcome (success, rejected, …) must be read from
    /// the pod/task logs via [`parse_run_status_from_logs`].
    Succeeded,
    /// The job/task failed at the infrastructure level (non-zero exit,
    /// pod OOM, etc.) before or without emitting a `run_finished` event.
    Failed,
    /// The connector's timeout elapsed before the job/task reached a
    /// terminal state.
    Timeout,
}

impl ConnectorRunStatus {
    /// Map to a Floe [`RunStatus`] suitable for a summary report.
    ///
    /// Use [`parse_run_status_from_logs`] first to obtain a finer-grained
    /// status from pod logs; only fall back to this mapping when logs are
    /// unavailable or parsing fails.
    pub fn to_run_status(self) -> RunStatus {
        match self {
            ConnectorRunStatus::Succeeded => RunStatus::Success,
            ConnectorRunStatus::Failed | ConnectorRunStatus::Timeout => RunStatus::Failed,
        }
    }

    /// Conventional exit code for use in summary reports.
    pub fn exit_code(self) -> i32 {
        match self {
            ConnectorRunStatus::Succeeded => 0,
            ConnectorRunStatus::Failed | ConnectorRunStatus::Timeout => 1,
        }
    }
}

// ---------------------------------------------------------------------------
// parse_run_status_from_logs
// ---------------------------------------------------------------------------

/// Parse a [`RunStatus`] from structured log output produced by
/// `floe run --log-format json`.
///
/// Scans `logs` line-by-line for a JSON object with
/// `"event": "run_finished"` and extracts the `"status"` field.
/// Returns `None` if no such event is found or if the status string
/// is not a recognized value.
///
/// # Example
/// ```
/// use floe_core::runner::parse_run_status_from_logs;
/// use floe_core::report::RunStatus;
///
/// let logs = r#"{"schema":"floe/v0/log","level":"info","event":"run_finished","run_id":"r1","status":"success","exit_code":0,"files":1,"rows":10,"accepted":10,"rejected":0,"warnings":0,"errors":0,"summary_uri":null,"ts_ms":0}"#;
/// assert_eq!(parse_run_status_from_logs(logs), Some(RunStatus::Success));
/// ```
pub fn parse_run_status_from_logs(logs: &str) -> Option<RunStatus> {
    for line in logs.lines() {
        let Ok(obj) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if obj.get("event").and_then(|v| v.as_str()) != Some("run_finished") {
            continue;
        }
        let Some(status_str) = obj.get("status").and_then(|v| v.as_str()) else {
            continue;
        };
        match status_str {
            "success" => return Some(RunStatus::Success),
            "success_with_warnings" => return Some(RunStatus::SuccessWithWarnings),
            "rejected" => return Some(RunStatus::Rejected),
            "aborted" => return Some(RunStatus::Aborted),
            "failed" => return Some(RunStatus::Failed),
            _ => continue,
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::RunStatus;

    // ---- ConnectorRunStatus ------------------------------------------------

    #[test]
    fn succeeded_maps_to_run_status_success() {
        assert_eq!(
            ConnectorRunStatus::Succeeded.to_run_status(),
            RunStatus::Success
        );
    }

    #[test]
    fn failed_maps_to_run_status_failed() {
        assert_eq!(
            ConnectorRunStatus::Failed.to_run_status(),
            RunStatus::Failed
        );
    }

    #[test]
    fn timeout_maps_to_run_status_failed() {
        assert_eq!(
            ConnectorRunStatus::Timeout.to_run_status(),
            RunStatus::Failed
        );
    }

    #[test]
    fn succeeded_exit_code_is_zero() {
        assert_eq!(ConnectorRunStatus::Succeeded.exit_code(), 0);
    }

    #[test]
    fn failed_exit_code_is_one() {
        assert_eq!(ConnectorRunStatus::Failed.exit_code(), 1);
    }

    #[test]
    fn timeout_exit_code_is_one() {
        assert_eq!(ConnectorRunStatus::Timeout.exit_code(), 1);
    }

    // ---- parse_run_status_from_logs ----------------------------------------

    fn make_run_finished_log(status: &str) -> String {
        format!(
            r#"{{"schema":"floe/v0/log","level":"info","event":"run_finished","run_id":"x","status":"{status}","exit_code":0,"files":1,"rows":10,"accepted":8,"rejected":2,"warnings":0,"errors":0,"summary_uri":null,"ts_ms":0}}"#
        )
    }

    #[test]
    fn parses_success_status() {
        assert_eq!(
            parse_run_status_from_logs(&make_run_finished_log("success")),
            Some(RunStatus::Success)
        );
    }

    #[test]
    fn parses_success_with_warnings_status() {
        assert_eq!(
            parse_run_status_from_logs(&make_run_finished_log("success_with_warnings")),
            Some(RunStatus::SuccessWithWarnings)
        );
    }

    #[test]
    fn parses_rejected_status() {
        assert_eq!(
            parse_run_status_from_logs(&make_run_finished_log("rejected")),
            Some(RunStatus::Rejected)
        );
    }

    #[test]
    fn parses_aborted_status() {
        assert_eq!(
            parse_run_status_from_logs(&make_run_finished_log("aborted")),
            Some(RunStatus::Aborted)
        );
    }

    #[test]
    fn parses_failed_status() {
        assert_eq!(
            parse_run_status_from_logs(&make_run_finished_log("failed")),
            Some(RunStatus::Failed)
        );
    }

    #[test]
    fn returns_none_for_empty_logs() {
        assert_eq!(parse_run_status_from_logs(""), None);
    }

    #[test]
    fn returns_none_for_non_json_logs() {
        assert_eq!(
            parse_run_status_from_logs("some plain text\nno json here"),
            None
        );
    }

    #[test]
    fn ignores_non_run_finished_events() {
        let noise = r#"{"schema":"floe/v0/log","level":"info","event":"entity_finished","name":"foo","status":"rejected","ts_ms":0}"#;
        assert_eq!(parse_run_status_from_logs(noise), None);
    }

    #[test]
    fn ignores_unknown_status_values() {
        assert_eq!(
            parse_run_status_from_logs(&make_run_finished_log("unknown_future_status")),
            None
        );
    }

    #[test]
    fn malformed_run_finished_missing_status_followed_by_valid_returns_valid() {
        // A run_finished record with no "status" field must be skipped, not
        // treated as a terminal None that hides the valid record below it.
        let malformed = r#"{"schema":"floe/v0/log","event":"run_finished","run_id":"x","ts_ms":0}"#;
        let valid = make_run_finished_log("rejected");
        let logs = format!("{malformed}\n{valid}");
        assert_eq!(
            parse_run_status_from_logs(&logs),
            Some(RunStatus::Rejected),
            "valid run_finished after malformed one must be found"
        );
    }

    #[test]
    fn malformed_run_finished_non_string_status_followed_by_valid_returns_valid() {
        // "status" present but not a string (e.g. a number) — still malformed.
        let malformed = r#"{"schema":"floe/v0/log","event":"run_finished","status":42,"ts_ms":0}"#;
        let valid = make_run_finished_log("success");
        let logs = format!("{malformed}\n{valid}");
        assert_eq!(
            parse_run_status_from_logs(&logs),
            Some(RunStatus::Success),
            "valid run_finished after non-string status must be found"
        );
    }

    #[test]
    fn only_malformed_run_finished_records_returns_none() {
        let malformed1 =
            r#"{"schema":"floe/v0/log","event":"run_finished","run_id":"x","ts_ms":0}"#;
        let malformed2 =
            r#"{"schema":"floe/v0/log","event":"run_finished","status":null,"ts_ms":0}"#;
        let logs = format!("{malformed1}\n{malformed2}");
        assert_eq!(
            parse_run_status_from_logs(&logs),
            None,
            "no valid run_finished => None"
        );
    }

    #[test]
    fn finds_event_among_mixed_log_lines() {
        let logs = format!(
            "not json\n{}\n{}",
            r#"{"event":"other","status":"success"}"#,
            make_run_finished_log("rejected")
        );
        assert_eq!(parse_run_status_from_logs(&logs), Some(RunStatus::Rejected));
    }
}
