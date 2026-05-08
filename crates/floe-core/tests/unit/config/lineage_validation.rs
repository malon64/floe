use floe_core::{validate, ValidateOptions};

use super::super::common::write_temp_config;

fn assert_validation_error(contents: &str, expected_parts: &[&str]) {
    let path = write_temp_config(contents);
    let err = validate(&path, ValidateOptions::default()).expect_err("expected error");
    let message = err.to_string();
    for part in expected_parts {
        assert!(
            message.contains(part),
            "expected error to contain {part:?}, got: {message}"
        );
    }
}

#[test]
fn lineage_url_required() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  namespace: "my-ns"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    assert_validation_error(config, &["url"]);
}

#[test]
fn lineage_namespace_required() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  url: "http://marquez:5000"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    assert_validation_error(config, &["namespace"]);
}

#[test]
fn lineage_valid_config_parses() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
lineage:
  url: "http://marquez:5000"
  namespace: "my-namespace"
  timeout_secs: 10
  producer: "https://github.com/myorg/floe"
entities:
  - name: "orders"
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
        - name: "id"
          type: "string"
"#;
    let path = write_temp_config(config);
    validate(&path, ValidateOptions::default()).expect("valid lineage config");
}
