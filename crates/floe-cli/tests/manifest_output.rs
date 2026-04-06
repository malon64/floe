use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn manifest_generate_common_to_file() {
    let config_path = repo_root().join("example/config.yml");
    let expected_config_uri = format!(
        "local://{}",
        std::fs::canonicalize(&config_path)
            .expect("canonicalize config path")
            .display()
    );
    let tmp = tempdir().expect("create temp dir");
    let output_path = tmp.path().join("manifest.airflow.json");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--output"])
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Manifest written:"));

    let payload = fs::read_to_string(&output_path).expect("manifest file exists");
    let value: Value = serde_json::from_str(&payload).expect("manifest should be valid json");

    assert_eq!(value["schema"], "floe.manifest.v1");
    assert_eq!(value["config_uri"], expected_config_uri);
    assert!(value["manifest_id"].as_str().is_some());
    assert!(value["execution"].is_object());
    assert!(value["runners"].is_object());

    let entities = value["entities"].as_array().expect("entities array");
    let names: Vec<_> = entities
        .iter()
        .map(|entity| entity["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["customer", "orders"]);

    let first_asset_key = entities[0]["asset_key"].as_array().unwrap();
    assert!(!first_asset_key.is_empty());
}

#[test]
fn manifest_generate_common_to_stdout() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    assert_eq!(value["schema"], "floe.manifest.v1");
}

#[test]
fn manifest_generate_with_entity_filter() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--entities", "orders", "--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    let entities = value["entities"].as_array().expect("entities array");
    assert_eq!(entities.len(), 1);
    assert_eq!(entities[0]["name"], "orders");
}

#[test]
fn manifest_generate_invalid_config_fails() {
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/invalid_config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["manifest", "generate", "-c"])
        .arg(&fixture_path)
        .args(["--output", "-"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error:"));
}

#[test]
fn manifest_generate_with_local_profile_has_local_runner() {
    let config_path = repo_root().join("example/config.yml");
    let profile_path = repo_root().join("example/profiles/dev.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .arg("--profile")
        .arg(&profile_path)
        .args(["--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    assert_eq!(value["runners"]["default"], "local");
    assert_eq!(
        value["runners"]["definitions"]["local"]["type"],
        "local_process"
    );
}

#[test]
fn manifest_generate_with_kubernetes_profile_has_kubernetes_runner() {
    let config_path = repo_root().join("example/config.yml");
    let tmp = tempdir().expect("create temp dir");
    let profile_path = tmp.path().join("k8s.yml");

    let mut f = fs::File::create(&profile_path).expect("create profile file");
    writeln!(
        f,
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata:\n  name: prod-k8s\nexecution:\n  runner:\n    type: kubernetes_job"
    )
    .expect("write profile");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .arg("--profile")
        .arg(&profile_path)
        .args(["--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    assert_eq!(value["runners"]["default"], "default");
    assert_eq!(
        value["runners"]["definitions"]["default"]["type"],
        "kubernetes_job"
    );
}

#[test]
fn manifest_generate_kubernetes_profile_serializes_k8_runner_fields() {
    let config_path = repo_root().join("example/config.yml");
    let tmp = tempdir().expect("create temp dir");
    let profile_path = tmp.path().join("k8s-fields.yml");

    let mut f = fs::File::create(&profile_path).expect("create profile file");
    writeln!(
        f,
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata:\n  name: prod-k8s\nexecution:\n  runner:\n    type: kubernetes_job\n    command: floe\n    args:\n      - run\n      - -c\n      - /config/config.yml\n    timeout_seconds: 3600\n    ttl_seconds_after_finished: 600\n    poll_interval_seconds: 15\n    secrets:\n      - floe-db\n      - floe-warehouse"
    )
    .expect("write profile");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .arg("--profile")
        .arg(&profile_path)
        .args(["--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(runner["type"], "kubernetes_job");
    assert_eq!(runner["command"], "floe");
    assert_eq!(
        runner["args"],
        serde_json::json!(["run", "-c", "/config/config.yml"])
    );
    assert_eq!(runner["timeout_seconds"], 3600);
    assert_eq!(runner["ttl_seconds_after_finished"], 600);
    assert_eq!(runner["poll_interval_seconds"], 15);
    assert_eq!(
        runner["secrets"],
        serde_json::json!(["floe-db", "floe-warehouse"])
    );
}

#[test]
fn manifest_generate_with_missing_profile_fails() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .arg("--profile")
        .arg("/nonexistent/path/profile.yml")
        .args(["--output", "-"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error:"));
}
