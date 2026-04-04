use std::path::Path;

use floe_core::{
    kubernetes_runner_meta, select_kubernetes_runner, K8sJobPhase, KubernetesConfig,
    KubernetesRunStatus, KubernetesRunnerAdapter, RunOptions, RunnerAdapter, RunnerKind,
};

// ---------------------------------------------------------------------------
// KubernetesConfig validation (public API)
// ---------------------------------------------------------------------------

fn valid_config() -> KubernetesConfig {
    KubernetesConfig {
        namespace: "default".to_string(),
        image: "my-registry/floe:latest".to_string(),
        job_name_prefix: "floe-run".to_string(),
        timeout_secs: 300,
        poll_interval_secs: 1,
        service_account: None,
        dashboard_url_template: None,
    }
}

#[test]
fn valid_config_passes_validation() {
    valid_config().validate().expect("valid config should pass");
}

#[test]
fn empty_namespace_fails() {
    let mut cfg = valid_config();
    cfg.namespace = "  ".to_string();
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("namespace"), "got: {err}");
}

#[test]
fn empty_image_fails() {
    let mut cfg = valid_config();
    cfg.image = String::new();
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("image"), "got: {err}");
}

#[test]
fn empty_job_name_prefix_fails() {
    let mut cfg = valid_config();
    cfg.job_name_prefix = String::new();
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("job_name_prefix"), "got: {err}");
}

#[test]
fn zero_timeout_fails() {
    let mut cfg = valid_config();
    cfg.timeout_secs = 0;
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("timeout_secs"), "got: {err}");
}

#[test]
fn zero_poll_interval_fails() {
    let mut cfg = valid_config();
    cfg.poll_interval_secs = 0;
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("poll_interval_secs"), "got: {err}");
}

#[test]
fn invalid_config_prevents_adapter_construction() {
    let mut cfg = valid_config();
    cfg.image = String::new();
    let err = select_kubernetes_runner(cfg)
        .err()
        .expect("should fail with invalid config");
    assert!(err.to_string().contains("image"), "got: {err}");
}

// ---------------------------------------------------------------------------
// Status mapping
// ---------------------------------------------------------------------------

#[test]
fn succeeded_phase_maps_to_succeeded() {
    assert_eq!(
        KubernetesRunStatus::from_phase(&K8sJobPhase::Succeeded),
        KubernetesRunStatus::Succeeded
    );
}

#[test]
fn failed_phase_maps_to_failed() {
    assert_eq!(
        KubernetesRunStatus::from_phase(&K8sJobPhase::Failed),
        KubernetesRunStatus::Failed
    );
}

#[test]
fn unknown_phase_maps_to_failed() {
    assert_eq!(
        KubernetesRunStatus::from_phase(&K8sJobPhase::Unknown("weird".to_string())),
        KubernetesRunStatus::Failed
    );
}

#[test]
fn kubernetes_run_status_as_str() {
    assert_eq!(KubernetesRunStatus::Succeeded.as_str(), "succeeded");
    assert_eq!(KubernetesRunStatus::Failed.as_str(), "failed");
    assert_eq!(KubernetesRunStatus::Timeout.as_str(), "timeout");
}

#[test]
fn succeeded_status_maps_to_run_status_success() {
    use floe_core::report::RunStatus;
    assert_eq!(
        KubernetesRunStatus::Succeeded.to_run_status(),
        RunStatus::Success
    );
    assert_eq!(
        KubernetesRunStatus::Failed.to_run_status(),
        RunStatus::Failed
    );
    assert_eq!(
        KubernetesRunStatus::Timeout.to_run_status(),
        RunStatus::Failed
    );
}

// ---------------------------------------------------------------------------
// Adapter construction and meta
// ---------------------------------------------------------------------------

#[test]
fn kubernetes_adapter_meta_has_kubernetes_kind() {
    let adapter = KubernetesRunnerAdapter::new(valid_config()).expect("construct adapter");
    let meta = adapter.meta(Path::new("/cfg/config.yml"));
    assert_eq!(meta.kind, RunnerKind::Kubernetes);
    assert!(meta.config_path.contains("config.yml"));
    let b = meta.backend.as_ref().expect("backend meta present");
    assert_eq!(b.backend_type, "kubernetes");
}

#[test]
fn select_kubernetes_runner_constructs_valid_adapter() {
    let adapter = select_kubernetes_runner(valid_config()).expect("construct");
    let meta = adapter.meta(Path::new("/cfg/config.yml"));
    assert_eq!(meta.kind, RunnerKind::Kubernetes);
}

// ---------------------------------------------------------------------------
// Dashboard URL template (via kubernetes_runner_meta helper)
// ---------------------------------------------------------------------------

#[test]
fn dashboard_url_template_is_filled_by_runner_meta_helper() {
    let cfg = KubernetesConfig {
        dashboard_url_template: Some("https://k8s.local/jobs/{job}".to_string()),
        ..valid_config()
    };
    let meta = kubernetes_runner_meta(
        Path::new("/cfg/config.yml"),
        &cfg,
        "floe-run-abc",
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:05:00Z",
        &KubernetesRunStatus::Succeeded,
    );
    assert_eq!(
        meta.backend.as_ref().unwrap().url.as_deref(),
        Some("https://k8s.local/jobs/floe-run-abc")
    );
}

#[test]
fn runner_meta_helper_populates_all_fields() {
    let meta = kubernetes_runner_meta(
        Path::new("/cfg/config.yml"),
        &valid_config(),
        "floe-run-xyz",
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:10:00Z",
        &KubernetesRunStatus::Succeeded,
    );
    let b = meta.backend.as_ref().unwrap();
    assert_eq!(b.backend_type, "kubernetes");
    assert_eq!(b.backend_run_id.as_deref(), Some("floe-run-xyz"));
    assert_eq!(b.submitted_at.as_deref(), Some("2026-01-01T00:00:00Z"));
    assert_eq!(b.final_status.as_deref(), Some("succeeded"));
}

// ---------------------------------------------------------------------------
// Timeout status assertions (status mapping, no real cluster needed)
// ---------------------------------------------------------------------------

#[test]
fn timeout_run_status_maps_to_failed() {
    use floe_core::report::RunStatus;
    assert_eq!(
        KubernetesRunStatus::Timeout.to_run_status(),
        RunStatus::Failed
    );
}

#[test]
fn timeout_status_as_str_is_timeout() {
    assert_eq!(KubernetesRunStatus::Timeout.as_str(), "timeout");
}

// ---------------------------------------------------------------------------
// RunOptions types compose correctly with adapter
// ---------------------------------------------------------------------------

#[test]
fn run_options_dry_run_accepted() {
    let _opts = RunOptions {
        run_id: Some("test-run".to_string()),
        entities: vec!["customers".to_string()],
        dry_run: true,
    };
    let _adapter = KubernetesRunnerAdapter::new(valid_config()).expect("construct");
}
