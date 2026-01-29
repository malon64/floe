use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

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
fn accepted_output_is_entity_level() {
    let root = temp_dir("floe-entity-accepted");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "a.csv", "id;name\n1;alice\n2;bob\n");
    write_csv(&input_dir, "b.csv", "id;name\n3;carol\n4;dave\n");

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
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.accepted_total, 4);
    assert_eq!(report.accepted_output.parts_written, 1);
    assert_eq!(
        report.accepted_output.part_files,
        vec!["part-00000.parquet".to_string()]
    );

    let mut part_files = Vec::new();
    for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
        let entry = entry.expect("read accepted entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            part_files.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    part_files.sort();
    assert_eq!(part_files, vec!["part-00000.parquet".to_string()]);
    assert!(!accepted_dir.join("a.parquet").exists());
    assert!(!accepted_dir.join("b.parquet").exists());

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open accepted parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read accepted parquet");
    assert_eq!(df.height(), 4);
}

#[test]
fn accepted_output_splits_into_multiple_parts() {
    let root = temp_dir("floe-entity-accepted-multipart");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "a.csv", "id;name\n1;alice\n2;bob\n3;carol\n");
    write_csv(&input_dir, "b.csv", "id;name\n4;dave\n5;erin\n6;frank\n");

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
          max_size_per_file: 1
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
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.accepted_total, 6);
    assert_eq!(report.accepted_output.parts_written, 6);

    let mut part_files = Vec::new();
    for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
        let entry = entry.expect("read accepted entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            part_files.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    part_files.sort();
    assert_eq!(
        part_files,
        vec![
            "part-00000.parquet".to_string(),
            "part-00001.parquet".to_string(),
            "part-00002.parquet".to_string(),
            "part-00003.parquet".to_string(),
            "part-00004.parquet".to_string(),
            "part-00005.parquet".to_string()
        ]
    );

    let mut total_rows = 0usize;
    for file_name in part_files {
        let output_path = accepted_dir.join(&file_name);
        let file = std::fs::File::open(&output_path).expect("open accepted parquet");
        let df = ParquetReader::new(file)
            .finish()
            .expect("read accepted parquet");
        total_rows += df.height();
    }
    assert_eq!(total_rows, 6);
}
