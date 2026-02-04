use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;

#[path = "../src/logging.rs"]
mod logging;

use floe_core::RunEvent;

fn write_fixture(temp_dir: &tempfile::TempDir) -> std::path::PathBuf {
    let root = temp_dir.path();
    fs::create_dir_all(root.join("in")).expect("create input dir");
    fs::create_dir_all(root.join("out")).expect("create out dir");

    fs::write(
        root.join("in/customers.csv"),
        "customer_id,created_at\nc1,2026-01-01T00:00:00Z\nc2,2026-01-02T00:00:00Z\n",
    )
    .expect("write csv");

    let config = r#"
version: "0.2"
report:
  path: "report"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "parquet"
        path: "out/accepted/customer"
      rejected:
        format: "csv"
        path: "out/rejected/customer"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
        - name: "created_at"
          type: "datetime"
          nullable: true
"#;

    let config_path = root.join("config.yml");
    fs::write(&config_path, config).expect("write config");
    config_path
}

#[test]
fn event_json_has_schema_and_level() {
    let event = RunEvent::RunStarted {
        run_id: "run-123".to_string(),
        config: "/tmp/config.yml".to_string(),
        report_base: Some("/tmp/report".to_string()),
        ts_ms: 1,
    };
    let line = logging::format_event_json(&event).expect("json");
    let text = logging::format_event_text(&event);
    assert!(text.contains("run_started"));
    let value: serde_json::Value = serde_json::from_str(&line).expect("valid json");
    assert_eq!(value["schema"], logging::LOG_SCHEMA);
    assert!(value["level"].is_string());
    assert_eq!(value["event"], "run_started");
}

#[test]
fn log_format_json_stdout_is_ndjson_only_and_summary_is_stderr() {
    let temp_dir = tempfile::TempDir::new().expect("tempdir");
    let config_path = write_fixture(&temp_dir);

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .current_dir(temp_dir.path())
        .args(["run", "-c"])
        .arg(&config_path)
        .args(["--log-format", "json"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);

    assert!(stderr.contains("Run summary:"), "stderr was: {stderr}");

    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "stdout was empty");

    let mut last_event = None;
    for line in &lines {
        let value: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|err| panic!("non-json line on stdout: {line:?} ({err})"));
        assert_eq!(value["schema"], logging::LOG_SCHEMA);
        assert!(value["level"].is_string());
        last_event = value["event"].as_str().map(|s| s.to_string());
    }

    assert_eq!(last_event.as_deref(), Some("run_finished"));
}

#[test]
fn default_mode_prints_human_summary() {
    let temp_dir = tempfile::TempDir::new().expect("tempdir");
    let config_path = write_fixture(&temp_dir);

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    cmd
        .current_dir(temp_dir.path())
        .args(["run", "-c"])
        .arg(&config_path)
        .assert()
        .success()
        .stdout(predicate::str::contains("Totals:"))
        .stdout(predicate::str::contains("Run summary:"));
}

#[test]
fn quiet_json_still_emits_run_finished() {
    let temp_dir = tempfile::TempDir::new().expect("tempdir");
    let config_path = write_fixture(&temp_dir);

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("floe"));
    let assert = cmd
        .current_dir(temp_dir.path())
        .args(["run", "-c"])
        .arg(&config_path)
        .args(["--quiet", "--log-format", "json"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "stdout was empty");
    let last: serde_json::Value = serde_json::from_str(lines[lines.len() - 1]).expect("json");
    assert_eq!(last["event"], "run_finished");
}
