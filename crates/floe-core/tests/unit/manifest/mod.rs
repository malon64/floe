use floe_core::{build_common_manifest_json, load_config, resolve_config_location};
use serde_json::Value;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn manifest_uses_local_uri_for_local_config() {
    let config_path = repo_root().join("example/config.yml");
    let expected_config_uri = format!(
        "local://{}",
        std::fs::canonicalize(&config_path)
            .expect("canonicalize config path")
            .display()
    );

    let config_location = resolve_config_location(
        config_path
            .to_str()
            .expect("example/config.yml path should be valid UTF-8"),
    )
    .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload =
        build_common_manifest_json(&config_location, &config, &[]).expect("build common manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest is valid json");

    assert_eq!(value["schema"], "floe.manifest.v1");
    assert_eq!(value["config_uri"], expected_config_uri);
}

#[test]
fn manifest_id_is_stable_for_same_config() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(
        config_path
            .to_str()
            .expect("example/config.yml path should be valid UTF-8"),
    )
    .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let first = build_common_manifest_json(&config_location, &config, &[]).expect("manifest");
    let second = build_common_manifest_json(&config_location, &config, &[]).expect("manifest");

    let first_value: Value = serde_json::from_str(&first).expect("valid json");
    let second_value: Value = serde_json::from_str(&second).expect("valid json");

    let first_id = first_value["manifest_id"]
        .as_str()
        .expect("manifest_id should be string");
    let second_id = second_value["manifest_id"]
        .as_str()
        .expect("manifest_id should be string");

    assert!(first_id.starts_with("mfv1-"));
    assert_eq!(first_id.len(), 21);
    assert_eq!(first_id, second_id);
}
