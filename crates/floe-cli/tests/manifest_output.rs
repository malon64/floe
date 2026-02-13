use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn manifest_generate_airflow_to_file() {
    let config_path = repo_root().join("example/config.yml");
    let expected_config_uri = std::fs::canonicalize(&config_path)
        .expect("canonicalize config path")
        .display()
        .to_string();
    let tmp = tempdir().expect("create temp dir");
    let output_path = tmp.path().join("manifest.airflow.json");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--target", "airflow", "--output"])
        .arg(&output_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Manifest written:"));

    let payload = fs::read_to_string(&output_path).expect("manifest file exists");
    let value: Value = serde_json::from_str(&payload).expect("manifest should be valid json");

    assert_eq!(value["schema"], "floe.airflow.manifest.v1");
    assert_eq!(value["config_uri"], expected_config_uri);

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
fn manifest_generate_dagster_to_stdout() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--target", "dagster", "--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    assert_eq!(value["schema"], "floe.dagster.manifest.v1");
}

#[test]
fn manifest_generate_with_entity_filter() {
    let config_path = repo_root().join("example/config.yml");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args([
            "--target",
            "airflow",
            "--entities",
            "orders",
            "--output",
            "-",
        ])
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
        .args(["--target", "airflow", "--output", "-"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error:"));
}
