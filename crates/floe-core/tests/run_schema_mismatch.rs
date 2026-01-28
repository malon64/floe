use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::report::{FileStatus, MismatchAction, RunStatus};
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

fn mismatch_block(missing: Option<&str>, extra: Option<&str>) -> String {
    if missing.is_none() && extra.is_none() {
        return String::new();
    }
    let mut out = String::from("      mismatch:\n");
    if let Some(value) = missing {
        out.push_str(&format!("        missing_columns: \"{}\"\n", value));
    }
    if let Some(value) = extra {
        out.push_str(&format!("        extra_columns: \"{}\"\n", value));
    }
    out
}

fn columns_block(columns: &[(&str, &str)]) -> String {
    let mut out = String::from("      columns:\n");
    for (name, ty) in columns {
        out.push_str(&format!("        - name: \"{}\"\n", name));
        out.push_str(&format!("          type: \"{}\"\n", ty));
    }
    out
}

fn config_yaml(
    input_dir: &Path,
    accepted_dir: &Path,
    rejected_dir: Option<&Path>,
    report_dir: &Path,
    severity: &str,
    mismatch: String,
    columns: String,
) -> String {
    let rejected_block = match rejected_dir {
        Some(dir) => format!(
            "      rejected:\n        format: \"csv\"\n        path: \"{}\"\n",
            dir.display()
        ),
        None => String::new(),
    };
    format!(
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
{rejected_block}    policy:
      severity: "{severity}"
    schema:
{mismatch}{columns}"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_block = rejected_block,
        severity = severity,
        mismatch = mismatch,
        columns = columns
    )
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
fn missing_columns_fill_nulls_accepts() {
    let root = temp_dir("floe-mismatch-missing-fill");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "id\n1\n");

    let mismatch = mismatch_block(Some("fill_nulls"), None);
    let columns = columns_block(&[("id", "string"), ("name", "string")]);
    let yaml = config_yaml(
        &input_dir,
        &accepted_dir,
        None,
        &report_dir,
        "warn",
        mismatch,
        columns,
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    let file = &report.files[0];
    assert_eq!(file.status, FileStatus::Success);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::FilledNulls);

    let output_path = accepted_dir.join("input.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let name_col = df.column("name").expect("missing name column");
    assert_eq!(name_col.null_count(), df.height());
}

#[test]
fn missing_columns_reject_file_rejects() {
    let root = temp_dir("floe-mismatch-missing-reject");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "id\n1\n");

    let mismatch = mismatch_block(Some("reject_file"), None);
    let columns = columns_block(&[("id", "string"), ("name", "string")]);
    let yaml = config_yaml(
        &input_dir,
        &accepted_dir,
        Some(&rejected_dir),
        &report_dir,
        "reject",
        mismatch,
        columns,
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Rejected);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::RejectedFile);
    let message = file
        .mismatch
        .error
        .as_ref()
        .expect("expected mismatch error")
        .message
        .clone();
    assert!(message.contains("entity.name=customer"));
}

#[test]
fn headerless_csv_uses_declared_names() {
    let root = temp_dir("floe-mismatch-headerless");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "1;alice\n2;bob\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
      options:
        header: false
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
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Success);

    let output_path = accepted_dir.join("input.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let id = df.column("id").expect("missing id column");
    let name = df.column("name").expect("missing name column");
    assert_eq!(id.null_count(), 0);
    assert_eq!(name.null_count(), 0);
}

#[test]
fn extra_columns_ignore_accepts() {
    let root = temp_dir("floe-mismatch-extra-ignore");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "id;extra\n1;skip\n");

    let mismatch = mismatch_block(None, Some("ignore"));
    let columns = columns_block(&[("id", "string")]);
    let yaml = config_yaml(
        &input_dir,
        &accepted_dir,
        None,
        &report_dir,
        "warn",
        mismatch,
        columns,
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let file = &outcome.entity_outcomes[0].report.files[0];
    assert_eq!(file.status, FileStatus::Success);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::IgnoredExtras);

    let output_path = accepted_dir.join("input.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    assert!(df.column("extra").is_err());
}

#[test]
fn extra_columns_reject_file_aborts() {
    let root = temp_dir("floe-mismatch-extra-abort");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "id;extra\n1;skip\n");

    let mismatch = mismatch_block(None, Some("reject_file"));
    let columns = columns_block(&[("id", "string")]);
    let yaml = config_yaml(
        &input_dir,
        &accepted_dir,
        Some(&rejected_dir),
        &report_dir,
        "abort",
        mismatch,
        columns,
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(outcome.summary.run.status, RunStatus::Aborted);
    assert_eq!(outcome.summary.run.exit_code, 2);
    let file = &report.files[0];
    assert_eq!(file.status, FileStatus::Aborted);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::Aborted);
    let message = file
        .mismatch
        .error
        .as_ref()
        .expect("expected mismatch error")
        .message
        .clone();
    assert!(message.contains("entity.name=customer"));
}

#[test]
fn reject_file_requested_but_warn_continues_with_warning() {
    let root = temp_dir("floe-mismatch-warn-override");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "input.csv", "id\n1\n");

    let mismatch = mismatch_block(Some("reject_file"), None);
    let columns = columns_block(&[("id", "string"), ("name", "string")]);
    let yaml = config_yaml(
        &input_dir,
        &accepted_dir,
        None,
        &report_dir,
        "warn",
        mismatch,
        columns,
    );
    let config_path = write_config(&root, &yaml);

    let outcome = run_config(&config_path);
    let report = &outcome.entity_outcomes[0].report;
    let file = &report.files[0];
    assert_eq!(file.status, FileStatus::Success);
    assert_eq!(file.mismatch.mismatch_action, MismatchAction::FilledNulls);
    let warning = file
        .mismatch
        .warning
        .as_ref()
        .expect("expected mismatch warning");
    assert!(warning.contains("entity.name=customer"));
    assert!(report.results.warnings_total > 0);
}
