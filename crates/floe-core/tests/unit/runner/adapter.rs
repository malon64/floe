use floe_core::{select_runner, RunnerKind, RunnerMeta};

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
fn runner_kind_from_str_unknown_fails() {
    let err = RunnerKind::from_profile_str("kubernetes").unwrap_err();
    assert!(
        err.to_string().contains("kubernetes"),
        "error should name the unknown runner; got: {err}"
    );
}

#[test]
fn runner_kind_from_str_empty_fails() {
    let err = RunnerKind::from_profile_str("").unwrap_err();
    assert!(err.to_string().contains("unknown"), "got: {err}");
}

#[test]
fn runner_kind_as_str_roundtrips() {
    assert_eq!(RunnerKind::Local.as_str(), "local");
}

// ---------------------------------------------------------------------------
// Profile runner type → RunnerKind mapping
// ---------------------------------------------------------------------------

#[test]
fn profile_runner_type_maps_to_local() {
    // Simulates: execution.runner.type = "local" in a profile.
    let kind = RunnerKind::from_profile_str("local").expect("profile runner type");
    assert_eq!(kind, RunnerKind::Local);
}

// ---------------------------------------------------------------------------
// select_runner / RunnerAdapter
// ---------------------------------------------------------------------------

#[test]
fn select_runner_local_returns_adapter_with_local_meta() {
    let adapter = select_runner(RunnerKind::Local);
    // Verify the adapter produces correct metadata.
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
    let adapter = select_runner(RunnerKind::default());
    let meta = adapter.meta(std::path::Path::new("/any/path.yml"));
    assert_eq!(meta.kind, RunnerKind::Local);
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

/// Verify that run() still routes through the local adapter unchanged by
/// confirming RunnerKind::default() is Local (the only adapter today).
/// A full end-to-end regression lives in the integration tests.
#[test]
fn default_run_path_uses_local_runner() {
    // The only observable property we can check without a real config is
    // that the default kind is Local, which is what run() uses.
    let default_kind = RunnerKind::default();
    assert_eq!(
        default_kind,
        RunnerKind::Local,
        "run() must use LocalRunnerAdapter by default"
    );
}
