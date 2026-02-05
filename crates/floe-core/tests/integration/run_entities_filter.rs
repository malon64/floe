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
fn run_respects_selected_entities_filter() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();

    let employees_in = root.join("in/hr");
    let orders_in = root.join("in/sales");
    fs::create_dir_all(&employees_in).expect("create employees input dir");
    fs::create_dir_all(&orders_in).expect("create orders input dir");

    write_csv(&employees_in, "employees.csv", "id;name\n1;alice\n");
    write_csv(&orders_in, "orders.csv", "id;amount\n1;10\n");

    let employees_accepted = root.join("out/accepted/employees");
    let employees_rejected = root.join("out/rejected/employees");
    let orders_accepted = root.join("out/accepted/orders");
    let orders_rejected = root.join("out/rejected/orders");
    let report_dir = root.join("report");

    let yaml = format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
entities:
  - name: "employees"
    source:
      format: "csv"
      path: "{employees_in}"
    sink:
      accepted:
        format: "parquet"
        path: "{employees_accepted}"
      rejected:
        format: "csv"
        path: "{employees_rejected}"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "id"
          type: "string"
        - name: "name"
          type: "string"
  - name: "orders"
    source:
      format: "csv"
      path: "{orders_in}"
    sink:
      accepted:
        format: "parquet"
        path: "{orders_accepted}"
      rejected:
        format: "csv"
        path: "{orders_rejected}"
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
        employees_in = employees_in.display(),
        employees_accepted = employees_accepted.display(),
        employees_rejected = employees_rejected.display(),
        orders_in = orders_in.display(),
        orders_accepted = orders_accepted.display(),
        orders_rejected = orders_rejected.display(),
    );
    let config_path = write_config(root, &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            run_id: Some("it-run".to_string()),
            entities: vec!["employees".to_string()],
        },
    )
    .expect("run config");

    assert_eq!(outcome.run_id, "it-run");

    let employees_report_path = report_dir.join("run_it-run/employees/run.json");
    assert!(
        employees_report_path.exists(),
        "expected employees report at {}",
        employees_report_path.display()
    );

    let orders_report_path = report_dir.join("run_it-run/orders/run.json");
    assert!(
        !orders_report_path.exists(),
        "did not expect orders report at {}",
        orders_report_path.display()
    );

    assert!(
        employees_accepted.exists(),
        "expected employees accepted outputs under {}",
        employees_accepted.display()
    );
    assert!(
        !orders_accepted.exists(),
        "did not expect orders accepted outputs under {}",
        orders_accepted.display()
    );

    let summary_path = report_dir.join("run_it-run/run.summary.json");
    let summary_json: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&summary_path).expect("read summary"))
            .expect("parse summary json");
    let entities = summary_json
        .get("entities")
        .and_then(|v| v.as_array())
        .expect("entities array in summary");
    assert_eq!(entities.len(), 1);
    assert_eq!(
        entities[0].get("name").and_then(|v| v.as_str()),
        Some("employees")
    );
}
