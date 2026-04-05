use floe_core::report::RunStatus;
use floe_core::{select_runner, ConnectorRunStatus, RunnerKind, RunnerMeta};

// ---------------------------------------------------------------------------
// RunnerKind
// ---------------------------------------------------------------------------

#[test]
fn default_runner_kind_is_local() {
    assert_eq!(RunnerKind::default(), RunnerKind::Local);
}

#[test]
fn runner_kind_from_str_local() {
    let kind = RunnerKind::from_profile_str("local").expect("parse local");
    assert_eq!(kind, RunnerKind::Local);
}

#[test]
fn runner_kind_from_str_kubernetes_is_recognized() {
    // kubernetes is a valid profile runner type even though the adapter
    // lives in the connector layer, not in floe-core.
    let kind = RunnerKind::from_profile_str("kubernetes").expect("parse kubernetes");
    assert_eq!(kind, RunnerKind::Kubernetes);
}

#[test]
fn runner_kind_from_str_unknown_fails() {
    let err = RunnerKind::from_profile_str("databricks").unwrap_err();
    assert!(
        err.to_string().contains("databricks"),
        "error should name the unknown runner; got: {err}"
    );
}

#[test]
fn runner_kind_from_str_empty_fails() {
    let err = RunnerKind::from_profile_str("").unwrap_err();
    assert!(err.to_string().contains("unknown"), "got: {err}");
}

#[test]
fn runner_kind_as_str_roundtrips_local() {
    assert_eq!(RunnerKind::Local.as_str(), "local");
}

#[test]
fn runner_kind_as_str_roundtrips_kubernetes() {
    assert_eq!(RunnerKind::Kubernetes.as_str(), "kubernetes");
}

// ---------------------------------------------------------------------------
// Profile runner type → RunnerKind mapping
// ---------------------------------------------------------------------------

#[test]
fn profile_runner_type_maps_to_local() {
    let kind = RunnerKind::from_profile_str("local").expect("profile runner type");
    assert_eq!(kind, RunnerKind::Local);
}

#[test]
fn profile_runner_type_maps_to_kubernetes() {
    // Profile parsing must accept "kubernetes" without error so that
    // connector-side profiles are valid at the schema layer.
    let kind = RunnerKind::from_profile_str("kubernetes").expect("profile runner type");
    assert_eq!(kind, RunnerKind::Kubernetes);
}

// ---------------------------------------------------------------------------
// select_runner / RunnerAdapter
// ---------------------------------------------------------------------------

#[test]
fn select_runner_local_returns_adapter_with_local_meta() {
    let adapter = select_runner(RunnerKind::Local).expect("local adapter");
    let meta: RunnerMeta = adapter.meta(std::path::Path::new("/tmp/config.yml"));
    assert_eq!(meta.kind, RunnerKind::Local);
    assert!(
        meta.config_path.contains("config.yml"),
        "meta should echo the config path; got: {}",
        meta.config_path
    );
}

#[test]
fn select_runner_default_produces_local_adapter() {
    let adapter = select_runner(RunnerKind::default()).expect("default adapter");
    let meta = adapter.meta(std::path::Path::new("/any/path.yml"));
    assert_eq!(meta.kind, RunnerKind::Local);
}

#[test]
fn select_runner_kubernetes_returns_actionable_error() {
    // The Kubernetes adapter is connector-owned; floe-core must not panic
    // but return an informative error so the caller can surface it.
    let err = select_runner(RunnerKind::Kubernetes)
        .err()
        .expect("kubernetes must return an error");
    let msg = err.to_string();
    assert!(
        msg.contains("kubernetes") || msg.contains("connector"),
        "error should mention kubernetes or connector layer; got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// RunnerMeta scaffold
// ---------------------------------------------------------------------------

#[test]
fn runner_meta_fields_accessible() {
    let meta = RunnerMeta {
        kind: RunnerKind::Local,
        config_path: "/data/config.yml".to_string(),
    };
    assert_eq!(meta.kind, RunnerKind::Local);
    assert_eq!(meta.config_path, "/data/config.yml");
}

// ---------------------------------------------------------------------------
// Regression: existing run() path is unaffected
// ---------------------------------------------------------------------------

#[test]
fn default_run_path_uses_local_runner() {
    let default_kind = RunnerKind::default();
    assert_eq!(
        default_kind,
        RunnerKind::Local,
        "run() must use LocalRunnerAdapter by default"
    );
}

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
        floe_core::parse_run_status_from_logs(&run_finished_log("success")),
        Some(RunStatus::Success)
    );
}

#[test]
fn parse_run_status_rejected() {
    assert_eq!(
        floe_core::parse_run_status_from_logs(&run_finished_log("rejected")),
        Some(RunStatus::Rejected)
    );
}

#[test]
fn parse_run_status_aborted() {
    assert_eq!(
        floe_core::parse_run_status_from_logs(&run_finished_log("aborted")),
        Some(RunStatus::Aborted)
    );
}

#[test]
fn parse_run_status_failed() {
    assert_eq!(
        floe_core::parse_run_status_from_logs(&run_finished_log("failed")),
        Some(RunStatus::Failed)
    );
}

#[test]
fn parse_run_status_returns_none_when_no_run_finished_event() {
    assert_eq!(
        floe_core::parse_run_status_from_logs("plain text\nnot json"),
        None
    );
}

#[test]
fn parse_run_status_ignores_other_events() {
    let noise =
        r#"{"schema":"floe/v0/log","event":"entity_finished","status":"success","ts_ms":0}"#;
    assert_eq!(floe_core::parse_run_status_from_logs(noise), None);
}
