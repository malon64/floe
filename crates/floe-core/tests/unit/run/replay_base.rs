use floe_core::config::ConfigBase;
use floe_core::manifest_replay_config_base_for_tests as manifest_replay_config_base;
use std::path::{Path, PathBuf};

#[test]
fn remote_manifest_replay_uses_recorded_work_root_not_manifest_bucket() {
    // A manifest loaded from a remote URI must NOT resolve its paths against its own
    // bucket (issue #443). With runtime_env=image/work_root=/work, the replay base is a
    // local root at /work with no remote_base.
    let remote = ConfigBase::remote_from_uri(
        PathBuf::from("/tmp/manifest"),
        "s3://openlakeforge-ops/floe/manifests/x.manifest.json",
    )
    .expect("remote base");
    let json = r#"{"runtime_env":"image","work_root":"/work"}"#;
    let base = manifest_replay_config_base(&remote, json);
    assert!(
        base.remote_base().is_none(),
        "remote manifest bucket must not become the resolution base"
    );
    assert_eq!(base.local_dir(), Path::new("/work"));
}

#[test]
fn remote_manifest_replay_never_keeps_remote_base_even_without_hints() {
    let remote = ConfigBase::remote_from_uri(
        PathBuf::from("/tmp/manifest"),
        "s3://bucket/x.manifest.json",
    )
    .expect("remote base");
    let base = manifest_replay_config_base(&remote, "{}");
    assert!(base.remote_base().is_none());
}

#[test]
fn local_manifest_replay_base_is_unchanged() {
    let local = ConfigBase::local_from_path(Path::new("/data/manifests/x.json"));
    let base = manifest_replay_config_base(&local, "{}");
    assert!(base.remote_base().is_none());
    assert_eq!(base.local_dir(), Path::new("/data/manifests"));
}
