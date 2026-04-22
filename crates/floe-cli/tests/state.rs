use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

fn write_config(dir: &tempfile::TempDir) -> std::path::PathBuf {
    let source_dir = dir.path().join("incoming");
    fs::create_dir_all(&source_dir).expect("mkdir source");
    let config_path = dir.path().join("config.yml");
    fs::write(
        &config_path,
        format!(
            r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{}"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
            source_dir.display(),
            dir.path().join("out").display()
        ),
    )
    .expect("write config");
    config_path
}

#[test]
fn state_inspect_prints_state_summary_and_json() {
    let dir = tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let state_path = dir.path().join("incoming/.floe/state/sales/state.json");
    fs::create_dir_all(state_path.parent().expect("parent")).expect("mkdir state parent");
    fs::write(
        &state_path,
        r#"{
  "schema": "floe.state.file-ingest.v1",
  "entity": "sales",
  "updated_at": "2026-04-22T09:00:00Z",
  "files": {
    "local:///tmp/incoming/sales.csv": {
      "processed_at": "2026-04-22T08:59:00Z",
      "size": 42,
      "mtime": "2026-04-22T08:00:00Z"
    }
  }
}"#,
    )
    .expect("write state");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["state", "inspect", "-c"])
        .arg(&config_path)
        .args(["--entity", "sales"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Entity: sales"))
        .stdout(predicate::str::contains("Incremental mode: file"))
        .stdout(predicate::str::contains("State exists: yes"))
        .stdout(predicate::str::contains("Tracked files: 1"))
        .stdout(predicate::str::contains("\"entity\": \"sales\""));
}

#[test]
fn state_reset_requires_explicit_yes_flag() {
    let dir = tempdir().expect("tempdir");
    let config_path = write_config(&dir);

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["state", "reset", "-c"])
        .arg(&config_path)
        .args(["--entity", "sales"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("rerun with --yes to confirm"));
}

#[test]
fn state_reset_removes_existing_file() {
    let dir = tempdir().expect("tempdir");
    let config_path = write_config(&dir);
    let state_path = dir.path().join("incoming/.floe/state/sales/state.json");
    fs::create_dir_all(state_path.parent().expect("parent")).expect("mkdir state parent");
    fs::write(
        &state_path,
        r#"{"schema":"floe.state.file-ingest.v1","entity":"sales","updated_at":null,"files":{}}"#,
    )
    .expect("write state");

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd.args(["state", "reset", "-c"])
        .arg(&config_path)
        .args(["--entity", "sales", "--yes"])
        .assert()
        .success()
        .stdout(predicate::str::contains("State reset: removed state file"));

    assert!(!state_path.exists());
}
