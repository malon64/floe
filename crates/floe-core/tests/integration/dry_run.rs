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
fn dry_run_resolves_inputs_and_lists() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id;name\n1;alice\n2;bob\n");
    write_csv(&input_dir, "more.csv", "id;name\n3;cara\n");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}/*.csv"
    sink:
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
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
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-dry".to_string()),
            entities: Vec::new(),
            dry_run: true,
        },
    )
    .expect("dry run config");

    let previews = outcome.dry_run_previews.expect("dry run previews");
    assert_eq!(previews.len(), 1);
    let preview = &previews[0];
    assert_eq!(preview.name, "customer");
    assert_eq!(preview.scanned_files.len(), 2);
    assert!(preview
        .scanned_files
        .iter()
        .any(|file| file.contains("data.csv")));
    assert!(preview
        .scanned_files
        .iter()
        .any(|file| file.contains("more.csv")));
}

#[test]
fn dry_run_missing_inputs_errors() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("missing");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");

    let yaml = format!(
        r#"version: "0.1"
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
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        input_dir = input_dir.display(),
        accepted_dir = accepted_dir.display(),
        rejected_dir = rejected_dir.display(),
    );
    let config_path = write_config(root, &yaml);

    let err = run(
        &config_path,
        RunOptions {
            run_id: Some("it-dry-missing".to_string()),
            entities: Vec::new(),
            dry_run: true,
        },
    )
    .expect_err("dry run should fail");
    let message = err.to_string();
    assert!(
        message.contains("no input files matched"),
        "unexpected error: {message}"
    );
}
