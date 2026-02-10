use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::io::write::parts;
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
            dry_run: false,
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

#[test]
fn accepted_output_overwrite_clears_previous_parts() {
    let root = temp_dir("floe-entity-accepted-overwrite");
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
      write_mode: "overwrite"
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

    let first = run_config(&config_path);
    assert_eq!(
        first.entity_outcomes[0]
            .report
            .accepted_output
            .parts_written,
        6
    );
    assert!(accepted_dir.join("part-00005.parquet").exists());

    write_csv(&input_dir, "a.csv", "id;name\n10;zara\n");
    fs::remove_file(input_dir.join("b.csv")).expect("remove second input");

    let second = run_config(&config_path);
    assert_eq!(
        second.entity_outcomes[0]
            .report
            .accepted_output
            .parts_written,
        1
    );
    assert!(!accepted_dir.join("part-00001.parquet").exists());

    let mut part_files = Vec::new();
    for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
        let entry = entry.expect("read accepted entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            part_files.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    part_files.sort();
    assert_eq!(part_files, vec!["part-00000.parquet".to_string()]);

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open accepted parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read accepted parquet");
    assert_eq!(df.height(), 1);
}

#[test]
fn accepted_output_append_adds_new_parts_across_runs() {
    let root = temp_dir("floe-entity-accepted-append");
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
      write_mode: "append"
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

    let first = run_config(&config_path);
    assert_eq!(first.entity_outcomes[0].report.results.accepted_total, 6);
    assert_eq!(
        first.entity_outcomes[0]
            .report
            .accepted_output
            .parts_written,
        6
    );

    let mut first_part_files = Vec::new();
    for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
        let entry = entry.expect("read accepted entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            first_part_files.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    first_part_files.sort();
    assert_eq!(first_part_files.len(), 6);
    assert!(first_part_files
        .iter()
        .all(|file| parts::is_part_filename(file, "parquet")));

    write_csv(&input_dir, "a.csv", "id;name\n7;zoe\n");
    fs::remove_file(input_dir.join("b.csv")).expect("remove second input");

    let second = run_config(&config_path);
    assert_eq!(second.entity_outcomes[0].report.results.accepted_total, 1);
    assert_eq!(
        second.entity_outcomes[0]
            .report
            .accepted_output
            .parts_written,
        1
    );
    assert_eq!(
        second.entity_outcomes[0]
            .report
            .accepted_output
            .part_files
            .len(),
        1
    );
    assert!(second.entity_outcomes[0]
        .report
        .accepted_output
        .part_files
        .iter()
        .all(|file| parts::is_part_filename(file, "parquet")));

    let mut part_files = Vec::new();
    for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
        let entry = entry.expect("read accepted entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            part_files.push(entry.file_name().to_string_lossy().to_string());
        }
    }
    part_files.sort();
    assert_eq!(part_files.len(), 7);
    assert!(part_files
        .iter()
        .all(|file| parts::is_part_filename(file, "parquet")));
    let new_parts = part_files
        .iter()
        .filter(|file| !first_part_files.contains(file))
        .count();
    assert_eq!(new_parts, 1);

    let mut total_rows = 0usize;
    for file_name in part_files {
        let output_path = accepted_dir.join(&file_name);
        let file = std::fs::File::open(&output_path).expect("open accepted parquet");
        let df = ParquetReader::new(file)
            .finish()
            .expect("read accepted parquet");
        total_rows += df.height();
    }
    assert_eq!(total_rows, 7);
}

#[test]
fn accepted_output_append_rejects_duplicates_across_runs() {
    let root = temp_dir("floe-entity-accepted-append-unique");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "a.csv", "id;name\n1;alice\n2;bob\n");

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
          unique: true
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let first = run_config(&config_path);
    assert_eq!(first.entity_outcomes[0].report.results.accepted_total, 2);
    assert_eq!(first.entity_outcomes[0].report.results.rejected_total, 0);
    assert_eq!(
        first.entity_outcomes[0]
            .report
            .accepted_output
            .part_files
            .len(),
        1
    );
    assert!(first.entity_outcomes[0]
        .report
        .accepted_output
        .part_files
        .iter()
        .all(|file| parts::is_part_filename(file, "parquet")));

    write_csv(&input_dir, "a.csv", "id;name\n2;bob\n3;carol\n");

    let second = run_config(&config_path);
    assert_eq!(second.entity_outcomes[0].report.results.accepted_total, 1);
    assert_eq!(second.entity_outcomes[0].report.results.rejected_total, 1);
    assert_eq!(
        second.entity_outcomes[0]
            .report
            .accepted_output
            .part_files
            .len(),
        1
    );
    assert!(second.entity_outcomes[0]
        .report
        .accepted_output
        .part_files
        .iter()
        .all(|file| parts::is_part_filename(file, "parquet")));
    let rejected_path = second.entity_outcomes[0]
        .report
        .files
        .first()
        .and_then(|file| file.output.rejected_path.as_ref())
        .expect("missing rejected output path");
    let rejected_path = rejected_path.trim_start_matches("local://");
    assert!(Path::new(rejected_path).exists());
}
