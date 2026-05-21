use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::tempdir;

fn write_minimal_config(dir: &std::path::Path, policy: &str) -> PathBuf {
    let config_path = dir.join("config.yml");
    fs::write(
        &config_path,
        format!(
            r#"version: "0.1"
entities:
  - name: orders
    source:
      format: csv
      path: ./in/orders
      options:
        header: true
        separator: ","
    sink:
      write_mode: overwrite
      accepted:
        format: parquet
        path: ./out/orders/
      rejected:
        format: csv
        path: ./out/rejected/orders/
    policy:
      severity: {policy}
    schema:
      columns:
        - name: order_id
          type: string
          nullable: false
"#
        ),
    )
    .expect("write config");
    config_path
}

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
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata:\n  name: prod-k8s\nexecution:\n  runner:\n    type: kubernetes_job\n    image: my-registry/floe:0.4.0\n    namespace: data-platform"
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
        "apiVersion: floe/v1\nkind: EnvironmentProfile\nmetadata:\n  name: prod-k8s\nexecution:\n  runner:\n    type: kubernetes_job\n    image: my-registry/floe:latest\n    namespace: floe-prod\n    command: floe\n    args:\n      - run\n      - -c\n      - /config/config.yml\n    timeout_seconds: 3600\n    ttl_seconds_after_finished: 600\n    poll_interval_seconds: 15\n    secrets:\n      - name: DB_PASSWORD\n        secret_name: floe-db-secret\n        key: password"
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
    assert_eq!(runner["image"], "my-registry/floe:latest");
    assert_eq!(runner["namespace"], "floe-prod");
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
        serde_json::json!([{
            "name": "DB_PASSWORD",
            "secret_name": "floe-db-secret",
            "key": "password"
        }])
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

#[test]
fn manifest_generate_applies_profile_variables_to_config() {
    let tmp = tempdir().expect("create temp dir");
    let config_path = tmp.path().join("config.yml");
    let profile_path = tmp.path().join("profile.yml");

    fs::write(
        &config_path,
        r#"version: "0.1"
report:
  path: "{{REPORT_DIR}}"
entities:
  - name: customers
    source:
      format: csv
      path: "{{INCOMING_DIR}}/customers.csv"
    sink:
      accepted:
        format: parquet
        path: "{{OUTPUT_DIR}}/customers/"
    policy:
      severity: warn
    schema:
      columns:
        - name: customer_id
          type: string
          nullable: false
"#,
    )
    .expect("write config");
    fs::write(
        &profile_path,
        r#"apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
variables:
  REPORT_DIR: ./reports-dev
  INCOMING_DIR: ./in-dev
  OUTPUT_DIR: ./out-dev
"#,
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
    assert!(value["report_base_uri"]
        .as_str()
        .expect("report base uri")
        .contains("reports-dev"));
    assert!(value["entities"][0]["source"]["path"]
        .as_str()
        .expect("source path")
        .contains("in-dev"));
    assert!(value["entities"][0]["sinks"]["accepted"]["path"]
        .as_str()
        .expect("accepted path")
        .contains("out-dev"));
}

#[test]
fn manifest_entity_includes_policy_severity_and_write_mode() {
    let tmp = tempdir().expect("create temp dir");
    let config_path = write_minimal_config(tmp.path(), "reject");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    let entity = &value["entities"][0];
    assert_eq!(entity["policy_severity"], "reject");
    assert_eq!(entity["write_mode"], "overwrite");
    assert_eq!(entity["incremental_mode"], "none");
}

#[test]
fn manifest_entity_includes_schema_columns() {
    let tmp = tempdir().expect("create temp dir");
    let config_path = write_minimal_config(tmp.path(), "warn");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--output", "-"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let value: Value = serde_json::from_str(stdout.trim()).expect("stdout should be json");
    let schema = &value["entities"][0]["schema"];
    let columns = schema["columns"].as_array().expect("columns array");
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "order_id");
    assert_eq!(columns[0]["column_type"], "string");
    assert_eq!(columns[0]["nullable"], false);
}

#[test]
fn manifest_includes_storages_when_profile_has_storages() {
    let tmp = tempdir().expect("create temp dir");
    let config_path = write_minimal_config(tmp.path(), "warn");
    let profile_path = tmp.path().join("profile.yml");

    fs::write(
        &profile_path,
        r#"apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: test-s3
storages:
  default: my-s3
  definitions:
    - name: my-s3
      type: s3
      bucket: my-bucket
      region: us-east-1
"#,
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
    assert!(value["storages"].is_object(), "storages should be embedded");
    assert_eq!(value["storages"]["default"], "my-s3");
}

#[test]
fn manifest_execution_uses_manifest_uri_arg() {
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
    let base_args = value["execution"]["base_args"]
        .as_array()
        .expect("base_args");
    let args_str: Vec<&str> = base_args.iter().map(|v| v.as_str().unwrap()).collect();
    assert!(
        args_str.contains(&"--manifest"),
        "base_args should contain --manifest"
    );
    assert!(
        args_str.contains(&"{manifest_uri}"),
        "base_args should contain {{manifest_uri}} token"
    );
}

#[test]
fn run_with_manifest_file_executes_entity() {
    let tmp = tempdir().expect("create temp dir");
    let config_path = write_minimal_config(tmp.path(), "warn");
    let manifest_path = tmp.path().join("manifest.json");
    let input_dir = tmp.path().join("in/orders");
    fs::create_dir_all(&input_dir).expect("create input dir");
    fs::write(input_dir.join("orders.csv"), "order_id\n001\n002\n").expect("write csv");

    // Generate the manifest first.
    Command::new(assert_cmd::cargo::cargo_bin!("floe"))
        .args(["manifest", "generate", "-c"])
        .arg(&config_path)
        .args(["--output"])
        .arg(&manifest_path)
        .assert()
        .success();

    // Patch the manifest to point paths to the temp dir.
    let manifest_str = fs::read_to_string(&manifest_path).expect("read manifest");
    let mut manifest: Value = serde_json::from_str(&manifest_str).expect("parse manifest");
    let entity = &mut manifest["entities"][0];
    entity["source"]["path"] = Value::String(input_dir.display().to_string());
    entity["source"]["uri"] = Value::String(input_dir.display().to_string());
    entity["sinks"]["accepted"]["path"] =
        Value::String(tmp.path().join("out/orders").display().to_string());
    entity["sinks"]["accepted"]["uri"] =
        Value::String(tmp.path().join("out/orders").display().to_string());
    entity["sinks"]["rejected"]["path"] =
        Value::String(tmp.path().join("out/rejected").display().to_string());
    entity["sinks"]["rejected"]["uri"] =
        Value::String(tmp.path().join("out/rejected").display().to_string());
    fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .expect("rewrite manifest");

    Command::new(assert_cmd::cargo::cargo_bin!("floe"))
        .args(["run", "--manifest"])
        .arg(&manifest_path)
        .args(["--entities", "orders"])
        .assert()
        .success();
}
