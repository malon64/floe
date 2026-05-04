use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::io::write::parts::is_part_filename;
use floe_core::report::{FileStatus, MismatchAction, RuleName};
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
            profile: None,
            run_id: Some("test-run".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config")
}

#[test]
fn unique_across_files_rejects_duplicates() {
    let root = temp_dir("floe-unique-entity");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "a.csv", "id;name\n1;alice\n2;bob\n");
    write_csv(&input_dir, "b.csv", "id;name\n2;carol\n3;dave\n");

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

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    let file_a = report
        .files
        .iter()
        .find(|file| file.input_file.ends_with("a.csv"))
        .expect("file a");
    let file_b = report
        .files
        .iter()
        .find(|file| file.input_file.ends_with("b.csv"))
        .expect("file b");
    assert_eq!(file_a.status, FileStatus::Success);
    assert_eq!(file_b.status, FileStatus::Rejected);
    assert!(file_b
        .validation
        .rules
        .iter()
        .any(|rule| rule.rule == RuleName::Unique));

    let accepted_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&accepted_path).expect("open accepted parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read accepted parquet");
    assert_eq!(df.height(), 3);

    let rejected_path = rejected_dir.join("part-00000.csv");
    let rejected_contents = fs::read_to_string(&rejected_path).expect("read rejected csv");
    assert!(rejected_contents.contains("__floe_errors"));
    assert!(rejected_contents.contains("unique"));
    let rejected_path_str = rejected_path.display().to_string();
    assert_eq!(
        file_b.output.rejected_path.as_deref(),
        Some(rejected_path_str.as_str())
    );
}

#[test]
fn mismatch_rejects_before_row_checks() {
    let root = temp_dir("floe-mismatch-precheck");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "id\n1\n");

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
      severity: "reject"
    schema:
      mismatch:
        missing_columns: "reject_file"
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
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Rejected);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::RejectedFile);

    let rejected_path = rejected_dir.join("input.csv");
    let rejected_contents = fs::read_to_string(&rejected_path).expect("read rejected csv");
    assert!(!rejected_contents.contains("__floe_errors"));
}

#[test]
fn rejected_overwrite_keeps_parts_across_files_in_run() {
    let root = temp_dir("floe-rejected-overwrite");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "a.csv", "id;name\n;alice\n");
    write_csv(&input_dir, "b.csv", "id;name\n;bob\n");

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
      write_mode: "overwrite"
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
          nullable: false
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.rejected_total, 2);

    let mut rejected_files = fs::read_dir(&rejected_dir)
        .expect("read rejected dir")
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("csv"))
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    rejected_files.sort();
    assert_eq!(rejected_files.len(), 2);
    assert!(rejected_files
        .iter()
        .all(|name| is_part_filename(name, "csv")));
    assert_ne!(rejected_files[0], rejected_files[1]);
}
