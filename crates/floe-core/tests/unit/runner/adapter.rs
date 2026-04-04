use floe_core::{select_runner, BackendMeta, RunnerKind, RunnerMeta};

// ---------------------------------------------------------------------------
// RunnerKind — local
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
fn runner_kind_from_str_unknown_fails() {
    // "databricks" is not yet a supported runner.
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
fn runner_kind_as_str_local_roundtrips() {
    assert_eq!(RunnerKind::Local.as_str(), "local");
}

// ---------------------------------------------------------------------------
// RunnerKind — kubernetes
// ---------------------------------------------------------------------------

#[test]
fn runner_kind_from_str_kubernetes() {
    let kind = RunnerKind::from_profile_str("kubernetes").expect("parse kubernetes");
    assert_eq!(kind, RunnerKind::Kubernetes);
}

#[test]
fn runner_kind_as_str_kubernetes_roundtrips() {
    assert_eq!(RunnerKind::Kubernetes.as_str(), "kubernetes");
}

#[test]
fn profile_runner_type_maps_to_kubernetes() {
    let kind = RunnerKind::from_profile_str("kubernetes").expect("profile runner type");
    assert_eq!(kind, RunnerKind::Kubernetes);
}

// ---------------------------------------------------------------------------
// Profile runner type → RunnerKind mapping (local)
// ---------------------------------------------------------------------------

#[test]
fn profile_runner_type_maps_to_local() {
    let kind = RunnerKind::from_profile_str("local").expect("profile runner type");
    assert_eq!(kind, RunnerKind::Local);
}

// ---------------------------------------------------------------------------
// select_runner / RunnerAdapter — local
// ---------------------------------------------------------------------------

#[test]
fn select_runner_local_returns_adapter_with_local_meta() {
    let adapter = select_runner(RunnerKind::Local);
    let meta: RunnerMeta = adapter.meta(std::path::Path::new("/tmp/config.yml"));
    assert_eq!(meta.kind, RunnerKind::Local);
    assert!(
        meta.config_path.contains("config.yml"),
        "meta should echo the config path; got: {}",
        meta.config_path
    );
    assert!(
        meta.backend.is_none(),
        "local runner should have no backend meta"
    );
}

#[test]
fn select_runner_default_produces_local_adapter() {
    let adapter = select_runner(RunnerKind::default());
    let meta = adapter.meta(std::path::Path::new("/any/path.yml"));
    assert_eq!(meta.kind, RunnerKind::Local);
}

// ---------------------------------------------------------------------------
// RunnerMeta + BackendMeta scaffold
// ---------------------------------------------------------------------------

#[test]
fn runner_meta_local_no_backend() {
    let meta = RunnerMeta {
        kind: RunnerKind::Local,
        config_path: "/data/config.yml".to_string(),
        backend: None,
    };
    assert_eq!(meta.kind, RunnerKind::Local);
    assert_eq!(meta.config_path, "/data/config.yml");
    assert!(meta.backend.is_none());
}

#[test]
fn runner_meta_with_backend_fields_accessible() {
    let meta = RunnerMeta {
        kind: RunnerKind::Kubernetes,
        config_path: "/data/config.yml".to_string(),
        backend: Some(BackendMeta {
            backend_type: "kubernetes".to_string(),
            backend_run_id: Some("floe-run-abc".to_string()),
            submitted_at: Some("2026-01-01T00:00:00Z".to_string()),
            finished_at: Some("2026-01-01T00:05:00Z".to_string()),
            final_status: Some("succeeded".to_string()),
            url: Some("https://k8s.internal/jobs/floe-run-abc".to_string()),
        }),
    };
    let b = meta.backend.as_ref().unwrap();
    assert_eq!(b.backend_type, "kubernetes");
    assert_eq!(b.backend_run_id.as_deref(), Some("floe-run-abc"));
    assert_eq!(b.final_status.as_deref(), Some("succeeded"));
    assert!(b.url.is_some());
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
