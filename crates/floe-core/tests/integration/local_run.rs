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
fn local_run_writes_outputs_and_report() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id;name\n1;alice\n2;bob\n");

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
        - name: "name"
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
            profile: None,
            run_id: Some("it-run".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    assert_eq!(outcome.run_id, "it-run");

    let mut parquet_files = Vec::new();
    if accepted_dir.exists() {
        for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
            let entry = entry.expect("read entry");
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                parquet_files.push(entry.path());
            }
        }
    }
    assert!(
        !parquet_files.is_empty(),
        "expected parquet output in {}",
        accepted_dir.display()
    );

    let report_path = report_dir.join("run_it-run/customer/run.json");
    assert!(
        report_path.exists(),
        "expected report at {}",
        report_path.display()
    );

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.sink.accepted.format, "parquet");
    assert_eq!(
        report.accepted_output.files_written.expect("files_written") as usize,
        parquet_files.len()
    );
    assert_eq!(
        report.accepted_output.parts_written as usize,
        parquet_files.len()
    );
    assert!(report.accepted_output.total_bytes_written.is_some());
    assert!(report.accepted_output.avg_file_size_mb.is_some());
    assert!(report.accepted_output.small_files_count.is_some());
}

#[test]
fn local_run_parquet_metrics_track_chunked_output_files() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    let mut csv = String::from("id;name\n");
    for i in 0..200 {
        csv.push_str(&format!("{i};user_{i}\n"));
    }
    write_csv(&input_dir, "data.csv", &csv);

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
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
        options:
          max_size_per_file: 256
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = root.join("out/rejected/customer").display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("it-run-parquet-metrics".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let parquet_files = fs::read_dir(&accepted_dir)
        .expect("read accepted dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .count();
    assert!(parquet_files > 1, "expected chunked parquet output");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(
        report.accepted_output.files_written.expect("files_written") as usize,
        parquet_files
    );
    assert_eq!(report.accepted_output.parts_written as usize, parquet_files);
    let total_bytes = report
        .accepted_output
        .total_bytes_written
        .expect("total_bytes_written");
    assert!(total_bytes > 0);
    assert!(
        report
            .accepted_output
            .avg_file_size_mb
            .expect("avg_file_size_mb")
            > 0.0
    );
    assert!(report.accepted_output.small_files_count.is_some());
}
