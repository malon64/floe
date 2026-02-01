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
fn local_run_writes_outputs_and_report() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id;name\n1;alice\n2;bob\n");

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
            run_id: Some("it-run".to_string()),
            entities: Vec::new(),
        },
    )
    .expect("run config");

    assert_eq!(outcome.run_id, "it-run");

    let mut parquet_files = Vec::new();
    if accepted_dir.exists() {
        for entry in fs::read_dir(&accepted_dir).expect("read accepted dir") {
            let entry = entry.expect("read entry");
            if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                parquet_files.push(entry.path());
            }
        }
    }
    assert!(
        !parquet_files.is_empty(),
        "expected parquet output in {}",
        accepted_dir.display()
    );

    let report_path = report_dir.join("run_it-run/customer/run.json");
    assert!(
        report_path.exists(),
        "expected report at {}",
        report_path.display()
    );
}
