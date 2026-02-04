use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;

const PLAN_SCHEMA: &str = "floe.plan.v1";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn validate_output_json_is_single_json_object() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["validate", "-c"])
        .arg(&config_path)
        .args(["--output", "json"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should be a single JSON object");
    assert_eq!(value["schema"], PLAN_SCHEMA);
    assert_eq!(value["valid"], true);
    assert!(value["plan"].is_object());
}

#[test]
fn validate_default_output_is_human_text() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["validate", "-c"])
        .arg(&config_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Config valid:"))
        .stdout(predicate::str::contains("Next: floe run"));
}

#[test]
fn validate_invalid_config_json_sets_valid_false_and_exit_1() {
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/invalid_config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["validate", "-c"])
        .arg(&fixture_path)
        .args(["--output", "json"])
        .assert()
        .failure();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("stdout should be JSON even when invalid");
    assert_eq!(value["schema"], PLAN_SCHEMA);
    assert_eq!(value["valid"], false);
    assert!(value["errors"].is_array());
    assert!(!value["errors"].as_array().unwrap().is_empty());
    assert!(value["plan"].is_null());
}
