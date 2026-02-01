use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::report::FileStatus;
use floe_core::{run, RunOptions};
use polars::prelude::{DataFrame, NamedFrom, ParquetReader, ParquetWriter, SerReader, Series};

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

fn write_parquet(dir: &Path, name: &str, mut df: DataFrame) -> PathBuf {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet");
    ParquetWriter::new(file)
        .finish(&mut df)
        .expect("write parquet");
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
fn parquet_directory_reads_multiple_files() {
    let root = temp_dir("floe-parquet-multi");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let df_a = DataFrame::new(vec![
        Series::new("id".into(), ["1"]).into(),
        Series::new("name".into(), ["alice"]).into(),
    ])
    .expect("df a");
    let df_b = DataFrame::new(vec![
        Series::new("id".into(), ["2"]).into(),
        Series::new("name".into(), ["bob"]).into(),
    ])
    .expect("df b");
    write_parquet(&input_dir, "a.parquet", df_a);
    write_parquet(&input_dir, "b.parquet", df_b);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "parquet"
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
    assert_eq!(report.files.len(), 2);
    let output_path = accepted_dir.join("part-00000.parquet");
    assert!(output_path.exists());
    let file = std::fs::File::open(&output_path).expect("open accepted parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read accepted parquet");
    assert_eq!(df.height(), 2);
}

#[test]
fn parquet_missing_columns_fill_nulls() {
    let root = temp_dir("floe-parquet-missing");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let df = DataFrame::new(vec![Series::new("id".into(), ["1"]).into()]).expect("df");
    write_parquet(&input_dir, "input.parquet", df);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "parquet"
      path: "{input_dir}"
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

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let name_col = df.column("name").expect("missing name column");
    assert_eq!(name_col.null_count(), df.height());
}

#[test]
fn parquet_extra_columns_ignore() {
    let root = temp_dir("floe-parquet-extra");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let df = DataFrame::new(vec![
        Series::new("id".into(), ["1"]).into(),
        Series::new("extra".into(), ["skip"]).into(),
    ])
    .expect("df");
    write_parquet(&input_dir, "input.parquet", df);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "parquet"
      path: "{input_dir}"
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

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    assert!(df.column("extra").is_err());
}

#[test]
fn parquet_cast_errors_reject_rows() {
    let root = temp_dir("floe-parquet-cast");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let df = DataFrame::new(vec![
        Series::new("id".into(), ["1", "2"]).into(),
        Series::new("amount".into(), ["1", "bad"]).into(),
    ])
    .expect("df");
    write_parquet(&input_dir, "input.parquet", df);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "payments"
    source:
      format: "parquet"
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
        - name: "amount"
          type: "number"
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
    assert_eq!(file.accepted_count, 1);
    assert_eq!(file.rejected_count, 1);
}
