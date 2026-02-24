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
fn local_delta_run_respects_partition_by_and_reports_metrics() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "orders.csv", "id;country\n1;us\n2;ca\n3;us\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "append"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
        partition_by: ["country"]
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "country"
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
            run_id: Some("it-delta-partitioned".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let entry_names = fs::read_dir(&accepted_dir)
        .expect("read delta table dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(entry_names.iter().any(|name| name == "_delta_log"));
    assert!(entry_names.iter().any(|name| name == "country=us"));
    assert!(entry_names.iter().any(|name| name == "country=ca"));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "delta");
    assert!(report.accepted_output.files_written > 0);
    assert_eq!(report.accepted_output.parts_written, 1);
    assert!(report.accepted_output.total_bytes_written.is_some());
    assert!(report.accepted_output.avg_file_size_mb.is_some());
    assert!(report.accepted_output.small_files_count.is_some());
}

#[test]
fn local_delta_run_without_partitioning_preserves_existing_behavior() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/orders_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "orders.csv", "id;country\n1;us\n2;ca\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "country"
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
            run_id: Some("it-delta-unpartitioned".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let entry_names = fs::read_dir(&accepted_dir)
        .expect("read delta table dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    assert!(entry_names.iter().any(|name| name == "_delta_log"));
    assert!(!entry_names.iter().any(|name| name.starts_with("country=")));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "delta");
    assert!(report.accepted_output.files_written > 0);
}
