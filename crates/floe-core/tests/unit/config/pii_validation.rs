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

fn assert_validation_ok(contents: &str) {
    let path = write_temp_config(contents);
    validate(&path, ValidateOptions::default()).expect("expected config to be valid");
}

fn base_config_with_pii(pii_yaml: &str) -> String {
    format!(
        r#"version: "0.1"
report:
  path: "/tmp/reports"
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
        - name: "order_id"
          type: "string"
        - name: "credit_card"
          type: "string"
        - name: "email"
          type: "string"
        - name: "amount"
          type: "float64"
    pii:
{pii_yaml}"#
    )
}

#[test]
fn pii_unknown_column_errors() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "nonexistent_col"
          strategy: "hash""#,
    );
    assert_validation_error(&config, &["nonexistent_col", "unknown schema column"]);
}

#[test]
fn pii_tokenize_strategy_errors() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "tokenize""#,
    );
    assert_validation_error(&config, &["tokenize", "not yet supported"]);
}

#[test]
fn pii_mask_requires_pattern() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "mask""#,
    );
    assert_validation_error(&config, &["mask_pattern"]);
}

#[test]
fn pii_mask_invalid_pattern_no_tokens_errors() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "mask"
          mask_pattern: "****""#,
    );
    assert_validation_error(&config, &["must contain at least one token"]);
}

#[test]
fn pii_drop_on_primary_key_errors() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
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
      primary_key:
        - "order_id"
      columns:
        - name: "order_id"
          type: "string"
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "order_id"
          strategy: "drop""#;
    assert_validation_error(config, &["drop", "primary_key"]);
}

#[test]
fn pii_duplicate_column_names_errors() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "hash"
        - name: "credit_card"
          strategy: "nullify""#,
    );
    assert_validation_error(&config, &["credit_card", "duplicated"]);
}

#[test]
fn pii_valid_config_parses() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "mask"
          mask_pattern: "****{last4}"
        - name: "email"
          strategy: "hash"
        - name: "order_id"
          strategy: "nullify""#,
    );
    assert_validation_ok(&config);
}

#[test]
fn pii_redact_strategy_valid() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "redact"
          redact_value: "[PII]""#,
    );
    assert_validation_ok(&config);
}

#[test]
fn pii_abort_severity_errors() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
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
      severity: "abort"
    schema:
      columns:
        - name: "order_id"
          type: "string"
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "credit_card"
          strategy: "hash""#;
    assert_validation_error(config, &["abort", "DataFrame processing"]);
}

#[test]
fn pii_drop_on_iceberg_errors() {
    let config = r#"version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "warehouse/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "order_id"
          type: "string"
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "credit_card"
          strategy: "drop""#;
    assert_validation_error(config, &["drop", "iceberg", "declared schema"]);
}
