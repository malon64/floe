use floe_core::{validate, ValidateOptions};

use super::super::common::write_temp_config;

#[test]
fn duckdb_local_target_validates() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        path: "out/warehouse.duckdb"
        duckdb:
          table: customers
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    validate(&path, ValidateOptions::default()).expect("local duckdb target should validate");
}

#[test]
fn duckdb_motherduck_target_validates() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        duckdb:
          connection: "md:analytics"
          table: customers
          token: "${MOTHERDUCK_TOKEN}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    validate(&path, ValidateOptions::default()).expect("motherduck duckdb target should validate");
}

#[test]
fn duckdb_requires_duckdb_block() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        path: "out/warehouse.duckdb"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let err = validate(&path, ValidateOptions::default()).expect_err("missing duckdb block");
    assert!(err
        .to_string()
        .contains("requires a sink.accepted.duckdb block"));
}

#[test]
fn duckdb_object_store_path_rejected_with_motherduck_hint() {
    let yaml = r#"
version: "0.1"
storages:
  default: s3_out
  definitions:
    - name: s3_out
      type: s3
      bucket: my-bucket
      region: eu-west-1
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
      storage: s3_out
    sink:
      accepted:
        format: "duckdb"
        path: "warehouse.duckdb"
        storage: s3_out
        duckdb:
          table: customers
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let err = validate(&path, ValidateOptions::default()).expect_err("object-store path rejected");
    let message = err.to_string();
    assert!(message.contains("MotherDuck"), "message was: {message}");
}

#[test]
fn duckdb_motherduck_with_storage_rejected() {
    let yaml = r#"
version: "0.1"
storages:
  default: local_out
  definitions:
    - name: local_out
      type: local
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
      storage: local_out
    sink:
      accepted:
        format: "duckdb"
        storage: local_out
        duckdb:
          connection: "md:analytics"
          table: customers
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let err =
        validate(&path, ValidateOptions::default()).expect_err("motherduck + storage rejected");
    assert!(err
        .to_string()
        .contains("sink.accepted.storage must be unset for a MotherDuck"));
}

#[test]
fn duckdb_non_motherduck_connection_rejected() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "duckdb"
        duckdb:
          connection: "s3://bucket/db.duckdb"
          table: customers
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let err = validate(&path, ValidateOptions::default()).expect_err("non-md connection rejected");
    assert!(err
        .to_string()
        .contains("only MotherDuck connection strings"));
}

#[test]
fn duckdb_merge_mode_without_primary_key_rejected() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "duckdb"
        path: "out/warehouse.duckdb"
        duckdb:
          table: customers
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let err =
        validate(&path, ValidateOptions::default()).expect_err("merge mode requires primary_key");
    assert!(err.to_string().contains("requires schema.primary_key"));
}
