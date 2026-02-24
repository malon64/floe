use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{run, RunOptions};

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

#[test]
fn local_run_writes_iceberg_table_and_report_fields() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_iceberg");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n2,bob\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "append"
      accepted:
        format: "iceberg"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-iceberg".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    assert!(accepted_dir.join("metadata").exists());
    assert!(accepted_dir.join("data").exists());

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "iceberg");
    assert_eq!(report.accepted_output.path, report.sink.accepted.path);
    assert_eq!(
        report.accepted_output.table_root_uri.as_deref(),
        Some(report.sink.accepted.path.as_str())
    );
    assert_eq!(report.accepted_output.write_mode.as_deref(), Some("append"));
    assert_eq!(report.accepted_output.files_written, 1);
    assert_eq!(report.accepted_output.parts_written, 1);
    assert!(report.accepted_output.snapshot_id.is_some());
}
