use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::{validate, ValidateOptions};

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
fn gcs_definition_requires_bucket() {
    let yaml = r#"
version: "0.1"
storages:
  default: gcs_raw
  definitions:
    - name: gcs_raw
      type: gcs
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "parquet"
        path: "out/customers"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
          nullable: false
"#;
    let path = write_temp_config(yaml);
    let err = validate(&path, ValidateOptions::default()).expect_err("should fail");
    let message = err.to_string();
    assert!(message.contains("requires bucket for type gcs"));
}

#[test]
fn gcs_storage_reference_is_not_supported_yet() {
    let yaml = r#"
version: "0.1"
storages:
  default: gcs_raw
  definitions:
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
      storage: gcs_raw
    sink:
      accepted:
        format: "parquet"
        path: "out/customers"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
          nullable: false
"#;
    let path = write_temp_config(yaml);
    let err = validate(&path, ValidateOptions::default()).expect_err("should fail");
    let message = err.to_string();
    assert!(message.contains("entity.name=customer"));
    assert!(message.contains("source.storage=gcs_raw"));
    assert!(message.contains("gcs"));
    assert!(message.contains("not implemented yet"));
}
