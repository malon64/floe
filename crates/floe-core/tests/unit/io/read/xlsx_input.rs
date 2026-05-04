use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::report::{FileStatus, MismatchAction};
use floe_core::{run, RunOptions};
use polars::prelude::{ParquetReader, SerReader};
use rust_xlsxwriter::Workbook;

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

fn write_xlsx(dir: &Path, name: &str, build: impl FnOnce(&mut Workbook)) -> PathBuf {
    let path = dir.join(name);
    let mut workbook = Workbook::new();
    build(&mut workbook);
    workbook.save(&path).expect("write xlsx");
    path
}

#[test]
fn xlsx_reads_named_sheet_with_offsets() {
    let root = temp_dir("floe-xlsx-happy");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    write_xlsx(&input_dir, "input.xlsx", |workbook| {
        {
            let worksheet = workbook.add_worksheet();
            worksheet.set_name("Ignore").expect("set sheet name");
            worksheet.write_string(0, 0, "skip").expect("write cell");
        }
        {
            let worksheet = workbook.add_worksheet();
            worksheet.set_name("Data").expect("set sheet name");
            worksheet
                .write_string(0, 0, "metadata")
                .expect("write cell");
            worksheet.write_string(1, 0, "id").expect("header");
            worksheet.write_string(1, 1, "name").expect("header");
            worksheet.write_string(2, 0, "1").expect("row");
            worksheet.write_string(2, 1, "alice").expect("row");
            worksheet.write_string(3, 0, "2").expect("row");
            worksheet.write_string(3, 1, "bob").expect("row");
        }
    });

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customers"
    source:
      format: "xlsx"
      path: "{input_dir}"
      options:
        sheet: "Data"
        header_row: 2
        data_row: 3
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
    assert_eq!(report.files.len(), 1);

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open accepted parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read accepted parquet");
    assert_eq!(df.height(), 2);
    assert!(df.column("id").is_ok());
    assert!(df.column("name").is_ok());
}

#[test]
fn xlsx_schema_mismatch_rejects_file() {
    let root = temp_dir("floe-xlsx-mismatch");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    write_xlsx(&input_dir, "input.xlsx", |workbook| {
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("Data").expect("set sheet name");
        worksheet.write_string(0, 0, "id").expect("header");
        worksheet.write_string(1, 0, "1").expect("row");
    });

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customers"
    source:
      format: "xlsx"
      path: "{input_dir}"
      options:
        sheet: "Data"
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
}

#[test]
fn xlsx_cast_errors_reject_rows() {
    let root = temp_dir("floe-xlsx-cast");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    write_xlsx(&input_dir, "input.xlsx", |workbook| {
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("Data").expect("set sheet name");
        worksheet.write_string(0, 0, "id").expect("header");
        worksheet.write_string(0, 1, "amount").expect("header");
        worksheet.write_string(1, 0, "1").expect("row");
        worksheet.write_string(1, 1, "1").expect("row");
        worksheet.write_string(2, 0, "2").expect("row");
        worksheet.write_string(2, 1, "bad").expect("row");
    });

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "payments"
    source:
      format: "xlsx"
      path: "{input_dir}"
      options:
        sheet: "Data"
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
