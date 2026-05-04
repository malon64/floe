use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use floe_core::report::FileStatus;
use floe_core::{run, RunOptions};
use orc_rust::arrow_writer::ArrowWriterBuilder;
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

fn make_batch(ids: &[&str], names: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids = Arc::new(StringArray::from(ids.to_vec()));
    let names = Arc::new(StringArray::from(names.to_vec()));
    RecordBatch::try_new(schema, vec![ids, names]).expect("record batch")
}

fn make_payments_batch(ids: &[&str], amounts: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let ids = Arc::new(StringArray::from(ids.to_vec()));
    let amounts = Arc::new(StringArray::from(amounts.to_vec()));
    RecordBatch::try_new(schema, vec![ids, amounts]).expect("record batch")
}

fn write_orc(dir: &Path, name: &str, batch: RecordBatch) -> PathBuf {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create orc");
    let mut writer = ArrowWriterBuilder::new(file, batch.schema())
        .try_build()
        .expect("orc writer");
    writer.write(&batch).expect("write orc");
    writer.close().expect("close orc");
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
fn orc_directory_reads_multiple_files() {
    let root = temp_dir("floe-orc-multi");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let batch_a = make_batch(&["1"], &["alice"]);
    let batch_b = make_batch(&["2"], &["bob"]);
    write_orc(&input_dir, "a.orc", batch_a);
    write_orc(&input_dir, "b.orc", batch_b);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "orc"
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
fn orc_cast_errors_reject_rows() {
    let root = temp_dir("floe-orc-cast");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let batch = make_payments_batch(&["1", "2"], &["1", "bad"]);
    write_orc(&input_dir, "input.orc", batch);

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "payments"
    source:
      format: "orc"
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
