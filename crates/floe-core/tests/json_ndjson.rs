use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::report::{FileStatus, MismatchAction};
use floe_core::{run, RunOptions};
use polars::prelude::{ParquetReader, SerReader};

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_ndjson(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write ndjson");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

fn run_config(path: &Path) -> floe_core::RunOutcome {
    run(
        path,
        RunOptions {
            run_id: Some("test-run".to_string()),
            entities: Vec::new(),
        },
    )
    .expect("run config")
}

#[test]
fn ndjson_missing_columns_fill_nulls() {
    let root = temp_dir("floe-ndjson-missing");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_ndjson(&input_dir, "input.json", "{\"id\":\"1\"}\n{\"id\":\"2\"}\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "json"
      path: "{input_dir}"
      options:
        ndjson: true
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      mismatch:
        missing_columns: "fill_nulls"
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Success);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::FilledNulls);

    let output_path = accepted_dir.join("input.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let name_col = df.column("name").expect("missing name column");
    assert_eq!(name_col.null_count(), df.height());
}

#[test]
fn ndjson_extra_columns_ignore() {
    let root = temp_dir("floe-ndjson-extra");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_ndjson(
        &input_dir,
        "input.json",
        "{\"id\":\"1\",\"extra\":\"skip\"}\n",
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "json"
      path: "{input_dir}"
      options:
        ndjson: true
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      mismatch:
        extra_columns: "ignore"
      columns:
        - name: "id"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Success);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::IgnoredExtras);

    let output_path = accepted_dir.join("input.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    assert!(df.column("extra").is_err());
}

#[test]
fn ndjson_invalid_line_rejects_file() {
    let root = temp_dir("floe-ndjson-invalid");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_ndjson(&input_dir, "input.json", "{\"id\":\"1\"}\n{bad}\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "json"
      path: "{input_dir}"
      options:
        ndjson: true
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Rejected);
    let issue = file.mismatch.error.as_ref().expect("expected json error");
    assert_eq!(issue.rule, "json_parse_error");
    assert!(issue.message.contains("entity.name=customer"));
}

#[test]
fn ndjson_nested_values_reject_file() {
    let root = temp_dir("floe-ndjson-nested");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_ndjson(
        &input_dir,
        "input.json",
        "{\"id\":\"1\",\"meta\":{\"a\":1}}\n",
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "json"
      path: "{input_dir}"
      options:
        ndjson: true
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Rejected);
    let issue = file.mismatch.error.as_ref().expect("expected json error");
    assert_eq!(issue.rule, "json_unsupported_value");
    assert!(issue.message.contains("entity.name=customer"));
}
