use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::load_config;

fn write_temp_config(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("floe-config-{nanos}.yml"));
    fs::write(&path, contents).expect("write temp config");
    path
}

#[test]
fn parse_config_loads_report_and_defaults() {
    let yaml = r#"
version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.report.as_ref().unwrap().path, "/tmp/reports");
    assert_eq!(config.entities.len(), 1);
    let entity = &config.entities[0];
    let options = entity.source.options.as_ref().expect("options");
    assert_eq!(options.header, Some(true));
    assert_eq!(options.separator.as_deref(), Some(";"));
    assert_eq!(options.encoding.as_deref(), Some("UTF8"));
}
