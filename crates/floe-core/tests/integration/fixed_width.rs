use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{run, RunOptions};
use polars::prelude::{ParquetReader, SerReader};

fn write_fixed(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write fixed-width");
    path
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    fs::write(&path, contents).expect("write config");
    path
}

#[test]
fn fixed_width_parses_rows() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_fixed(&input_dir, "data.txt", "0001Alice \n0002Bob   \n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "fixed"
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
          width: 4
        - name: "name"
          type: "string"
          width: 6
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("fixed-width-parse".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let id_col = df.column("id").expect("missing id column");
    let name_col = df.column("name").expect("missing name column");
    let id_values = id_col.as_materialized_series().str().expect("id utf8");
    let name_values = name_col.as_materialized_series().str().expect("name utf8");
    assert_eq!(id_values.get(0), Some("0001"));
    assert_eq!(id_values.get(1), Some("0002"));
    assert_eq!(name_values.get(0), Some("Alice"));
    assert_eq!(name_values.get(1), Some("Bob"));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.accepted_total, 2);
}

#[test]
fn fixed_width_cast_errors_reject_rows() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_fixed(&input_dir, "data.txt", "000110\nABCD20\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "fixed"
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
          type: "integer"
          width: 4
        - name: "qty"
          type: "integer"
          width: 2
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
            run_id: Some("fixed-width-cast".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let report = &outcome.entity_outcomes[0].report;
    let file_report = &report.files[0];
    assert_eq!(file_report.accepted_count, 1);
    assert_eq!(file_report.rejected_count, 1);
}

#[test]
fn fixed_width_handles_unicode_characters() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_fixed(&input_dir, "data.txt", "0001José東京\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "fixed"
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
          width: 4
        - name: "name"
          type: "string"
          width: 4
        - name: "city"
          type: "string"
          width: 2
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("fixed-width-unicode".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let output_path = accepted_dir.join("part-00000.parquet");
    let file = std::fs::File::open(&output_path).expect("open output parquet");
    let df = ParquetReader::new(file)
        .finish()
        .expect("read output parquet");
    let name_col = df.column("name").expect("missing name column");
    let city_col = df.column("city").expect("missing city column");
    let name_values = name_col.as_materialized_series().str().expect("name utf8");
    let city_values = city_col.as_materialized_series().str().expect("city utf8");
    assert_eq!(name_values.get(0), Some("José"));
    assert_eq!(city_values.get(0), Some("東京"));

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.accepted_total, 1);
}
