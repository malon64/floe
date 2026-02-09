use std::fs;
use std::path::{Path, PathBuf};

use floe_core::report::FileStatus;
use floe_core::{run, RunOptions};
use polars::prelude::{ParquetReader, SerReader};

fn write_json(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write json");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut parquet_files = Vec::new();
    if dir.exists() {
        for entry in fs::read_dir(dir).expect("read accepted dir") {
            let entry = entry.expect("read entry");
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                parquet_files.push(entry.path());
            }
        }
    }
    parquet_files
}

#[test]
fn json_selector_strict_rejects_non_scalar() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_json(
        &input_dir,
        "data.json",
        r#"[{"id":"1","user":{"name":"Ada"}}]"#,
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
      cast_mode: "strict"
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
        - name: "user_payload"
          source: "user"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-json-strict".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let file_report = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file_report.status, FileStatus::Rejected);
    assert_eq!(
        file_report
            .mismatch
            .error
            .as_ref()
            .expect("mismatch error")
            .rule,
        "json_selector_non_scalar"
    );
}

#[test]
fn json_selector_coerce_stringifies_non_scalar() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_json(
        &input_dir,
        "data.json",
        r#"[{"id":"1","user":{"name":"Ada"}}]"#,
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
      cast_mode: "coerce"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "user_payload"
          source: "user"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-json-coerce".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let file_report = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file_report.status, FileStatus::Success);

    let files = parquet_files(&accepted_dir);
    assert_eq!(files.len(), 1);
    let file = fs::File::open(&files[0]).expect("open parquet");
    let df = ParquetReader::new(file).finish().expect("read parquet");
    let value = df
        .column("user_payload")
        .expect("user_payload column")
        .as_materialized_series()
        .str()
        .expect("string column")
        .get(0)
        .expect("value");
    assert_eq!(value, "{\"name\":\"Ada\"}");
}

#[test]
fn json_selector_mismatch_uses_top_level_only() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_json(
        &input_dir,
        "data.json",
        r#"[{"id":"1","user":{"name":"Ada"},"extra":"x"}]"#,
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
      cast_mode: "strict"
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
      mismatch:
        extra_columns: "reject_file"
      columns:
        - name: "id"
          type: "string"
        - name: "user_name"
          source: "user.name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-json-mismatch".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let file_report = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file_report.status, FileStatus::Rejected);
    assert_eq!(file_report.mismatch.missing_columns, Vec::<String>::new());
    assert_eq!(
        file_report.mismatch.extra_columns,
        vec!["extra".to_string(), "user".to_string()]
    );
}
