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
fn composite_unique_checks_across_files_and_reports_samples() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let accepted_dir = root.join("out/accepted/customer");
    let rejected_dir = root.join("out/rejected/customer");
    let report_dir = root.join("report");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(
        &input_dir,
        "batch1.csv",
        "id;country;email\n1;fr;alice@demo\n2;;bob@demo\n3;us;carol@demo\n",
    );
    write_csv(
        &input_dir,
        "batch2.csv",
        "id;country;email\n1;fr;alice_new@demo\n2;;bob_new@demo\n4;us;carol@demo\n",
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
      accepted:
        format: "parquet"
        path: "{accepted_dir}"
      rejected:
        format: "csv"
        path: "{rejected_dir}"
    policy:
      severity: "reject"
    schema:
      primary_key: ["id", "country"]
      unique_keys:
        - ["id", "country"]
        - ["email"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "email"
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
        RunOptions { profile: None,
            run_id: Some("it-composite-unique".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("run config");

    let report = &outcome.entity_outcomes[0].report;
    assert_eq!(report.results.rows_total, 6);
    assert_eq!(report.results.accepted_total, 2);
    assert_eq!(report.results.rejected_total, 4);

    assert_eq!(report.unique_constraints.len(), 2);
    let tuple_constraint = report
        .unique_constraints
        .iter()
        .find(|constraint| constraint.columns == vec!["id".to_string(), "country".to_string()])
        .expect("tuple unique constraint");
    assert_eq!(tuple_constraint.duplicates_count, 1);
    assert!(!tuple_constraint.samples.is_empty());

    let email_constraint = report
        .unique_constraints
        .iter()
        .find(|constraint| constraint.columns == vec!["email".to_string()])
        .expect("email unique constraint");
    assert_eq!(email_constraint.duplicates_count, 1);
    assert!(!email_constraint.samples.is_empty());
}
