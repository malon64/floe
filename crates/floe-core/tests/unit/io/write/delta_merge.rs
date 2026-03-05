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
fn merge_scd1_warn_rejects_duplicate_source_keys_before_merge() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch.csv",
        "id;country;name\n1;fr;alice\n1;fr;alice-dup\n",
    );

    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id", "country"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("unit-delta-merge-dup".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge_scd1 should reject duplicate merge-key rows before merge in warn mode");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.rows_total, 2);
    assert_eq!(report.results.accepted_total, 1);
    assert_eq!(report.results.rejected_total, 1);
    assert_eq!(report.accepted_output.target_rows_after, Some(1));
}

#[test]
fn merge_scd1_reports_inserted_and_updated_counts() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer_delta");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;name\n1;fr;alice\n2;ca;bob\n",
    );

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
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "{accepted_dir}"
    policy:
      severity: "warn"
    schema:
      primary_key: ["id", "country"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    run(
        &config_path,
        RunOptions {
            run_id: Some("unit-delta-merge-init".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("initial merge run");

    fs::remove_file(input_dir.join("batch1.csv")).expect("remove first file");
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;name\n1;fr;alice-updated\n3;us;carol\n",
    );

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("unit-delta-merge-upsert".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("merge upsert run");

    let accepted = &outcome.entity_outcomes[0].report.accepted_output;
    assert_eq!(accepted.write_mode.as_deref(), Some("merge_scd1"));
    assert_eq!(accepted.inserted_count, Some(1));
    assert_eq!(accepted.updated_count, Some(1));
    assert_eq!(accepted.closed_count, None);
    assert_eq!(accepted.unchanged_count, None);
    assert_eq!(accepted.target_rows_before, Some(2));
    assert_eq!(accepted.target_rows_after, Some(3));
}
