use std::fs;
use std::path::{Path, PathBuf};

use floe_core::{run, RunOptions};

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write config");
}

#[test]
fn local_run_outputs_use_normalized_paths() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    let input_dir = root.join("input");
    let out_dir = root.join("out/accepted/customer");
    let report_dir = root.join("reports/base");
    fs::create_dir_all(&cfg_dir).expect("cfg dir");
    fs::create_dir_all(&input_dir).expect("input dir");
    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n");

    let config_path = cfg_dir.join("config.yml");
    let yaml = r#"version: "0.1"
report:
  path: "../report/../reports/./base"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "../in/../input/./data.csv"
    sink:
      accepted:
        format: "parquet"
        path: "../out/../out/accepted/./customer"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
"#
    .to_string();
    write_config(&config_path, &yaml);

    let outcome = run(
        &config_path,
        RunOptions { profile: None,
            run_id: Some("path-norm-it".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let expected_report_base = root.join("reports/base").display().to_string();
    assert_eq!(
        outcome.report_base_path.as_deref(),
        Some(expected_report_base.as_str())
    );
    assert_eq!(outcome.summary.report.path, expected_report_base);
    assert!(!outcome.summary.report.path.contains("/./"));
    assert!(!outcome.summary.report.path.contains("/../"));
    assert!(!outcome.summary.report.report_file.contains("/./"));
    assert!(!outcome.summary.report.report_file.contains("/../"));

    let entity_report = &outcome.entity_outcomes[0].report;
    assert!(!entity_report.accepted_output.path.contains("/./"));
    assert!(!entity_report.accepted_output.path.contains("/../"));
    assert!(!entity_report.files[0].input_file.contains("/./"));
    assert!(!entity_report.files[0].input_file.contains("/../"));

    let expected_report_file = root.join("reports/base/run_path-norm-it/customer/run.json");
    assert!(expected_report_file.exists());
    assert!(report_dir.exists());
    assert!(out_dir.exists());
}
