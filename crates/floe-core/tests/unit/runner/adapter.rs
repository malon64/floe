use floe_core::report::RunStatus;
use floe_core::{parse_run_status_from_logs, ConnectorRunStatus};

// ---------------------------------------------------------------------------
// ConnectorRunStatus — outcome/status metadata helpers
// ---------------------------------------------------------------------------

#[test]
fn connector_run_status_succeeded_maps_to_success() {
    assert_eq!(
        ConnectorRunStatus::Succeeded.to_run_status(),
        RunStatus::Success
    );
}

#[test]
fn connector_run_status_failed_maps_to_failed() {
    assert_eq!(
        ConnectorRunStatus::Failed.to_run_status(),
        RunStatus::Failed
    );
}

#[test]
fn connector_run_status_timeout_maps_to_failed() {
    assert_eq!(
        ConnectorRunStatus::Timeout.to_run_status(),
        RunStatus::Failed
    );
}

#[test]
fn connector_run_status_exit_codes() {
    assert_eq!(ConnectorRunStatus::Succeeded.exit_code(), 0);
    assert_eq!(ConnectorRunStatus::Failed.exit_code(), 1);
    assert_eq!(ConnectorRunStatus::Timeout.exit_code(), 1);
}

// ---------------------------------------------------------------------------
// parse_run_status_from_logs — connector log-parsing helper
// ---------------------------------------------------------------------------

fn run_finished_log(status: &str) -> String {
    format!(
        r#"{{"schema":"floe/v0/log","level":"info","event":"run_finished","run_id":"r1","status":"{status}","exit_code":0,"files":1,"rows":10,"accepted":10,"rejected":0,"warnings":0,"errors":0,"summary_uri":null,"ts_ms":0}}"#
    )
}

#[test]
fn parse_run_status_success() {
    assert_eq!(
        parse_run_status_from_logs(&run_finished_log("success")),
        Some(RunStatus::Success)
    );
}

#[test]
fn parse_run_status_rejected() {
    assert_eq!(
        parse_run_status_from_logs(&run_finished_log("rejected")),
        Some(RunStatus::Rejected)
    );
}

#[test]
fn parse_run_status_aborted() {
    assert_eq!(
        parse_run_status_from_logs(&run_finished_log("aborted")),
        Some(RunStatus::Aborted)
    );
}

#[test]
fn parse_run_status_failed() {
    assert_eq!(
        parse_run_status_from_logs(&run_finished_log("failed")),
        Some(RunStatus::Failed)
    );
}

#[test]
fn parse_run_status_returns_none_when_no_run_finished_event() {
    assert_eq!(parse_run_status_from_logs("plain text\nnot json"), None);
}

#[test]
fn parse_run_status_ignores_other_events() {
    let noise =
        r#"{"schema":"floe/v0/log","event":"entity_finished","status":"success","ts_ms":0}"#;
    assert_eq!(parse_run_status_from_logs(noise), None);
}
