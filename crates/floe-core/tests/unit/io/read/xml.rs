use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::report::{FileStatus, MismatchAction};
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

fn write_xml(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write xml");
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
fn xml_valid_succeeds() {
    let root = temp_dir("floe-xml-valid");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_xml(
        &input_dir,
        "input.xml",
        r#"<rows><row><id>1</id><name>Ada</name></row><row><id>2</id><name>Bob</name></row></rows>"#,
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "xml"
      path: "{input_dir}"
      options:
        row_tag: "row"
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
}

#[test]
fn xml_missing_elements_become_nulls() {
    let root = temp_dir("floe-xml-missing");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_xml(
        &input_dir,
        "input.xml",
        r#"<rows><row><id>1</id><name>Ada</name></row><row><id>2</id></row></rows>"#,
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "xml"
      path: "{input_dir}"
      options:
        row_tag: "row"
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

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let name_col = df.column("name").expect("missing name column");
    assert_eq!(name_col.null_count(), 1);
}

#[test]
fn xml_attribute_only_rows_with_attribute_selectors_succeed() {
    let root = temp_dir("floe-xml-attributes");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_xml(
        &input_dir,
        "input.xml",
        r#"<rows><row id="1" name="Ada"/><row id="2" name="Bob"/></rows>"#,
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "xml"
      path: "{input_dir}"
      options:
        row_tag: "row"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          source: "@id"
          type: "string"
        - name: "name"
          source: "@name"
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
    assert_eq!(df.height(), 2);
}

#[test]
fn xml_schema_mismatch_rejects() {
    let root = temp_dir("floe-xml-mismatch");
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted");
    let rejected_dir = root.join("out/rejected");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");
    write_xml(
        &input_dir,
        "input.xml",
        r#"<rows><row><id>1</id></row></rows>"#,
    );

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "xml"
      path: "{input_dir}"
      options:
        row_tag: "row"
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
