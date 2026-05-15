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

#[test]
fn pii_nullify_on_required_column_errors() {
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
      columns:
        - name: "order_id"
          type: "string"
          nullable: false
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "order_id"
          strategy: "nullify""#;
    assert_validation_error(config, &["nullify", "nullable=false"]);
}

#[test]
fn pii_merge_key_masking_errors() {
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
        format: "delta"
        path: "/tmp/out"
      write_mode: "merge_scd1"
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
          strategy: "hash""#;
    assert_validation_error(config, &["primary_key", "merge_scd1", "merge predicate"]);
}

#[test]
fn pii_nullify_on_unique_keys_group_errors() {
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
      unique_keys:
        - ["email"]
      columns:
        - name: "order_id"
          type: "string"
        - name: "email"
          type: "string"
    pii:
      columns:
        - name: "email"
          strategy: "nullify""#;
    assert_validation_error(config, &["Nullify", "unique-key", "uniqueness"]);
}

#[test]
fn pii_redact_on_column_unique_errors() {
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
      columns:
        - name: "order_id"
          type: "string"
          unique: true
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "order_id"
          strategy: "redact""#;
    assert_validation_error(config, &["Redact", "unique-key", "uniqueness"]);
}

#[test]
fn pii_hash_on_unique_key_errors() {
    // hash on a unique-key column leaks raw values into the run report via
    // UniqueTracker samples (recorded before masking runs).
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
      unique_keys:
        - ["email"]
      columns:
        - name: "order_id"
          type: "string"
        - name: "email"
          type: "string"
    pii:
      columns:
        - name: "email"
          strategy: "hash""#;
    assert_validation_error(config, &["hash", "unique-key", "run report"]);
}

#[test]
fn pii_hash_on_non_unique_key_is_valid() {
    // hash is fine on columns that are not part of any uniqueness constraint.
    let config = base_config_with_pii(
        r#"      columns:
        - name: "credit_card"
          strategy: "hash""#,
    );
    assert_validation_ok(&config);
}

#[test]
fn pii_mismatch_missing_reject_file_errors() {
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
      mismatch:
        missing_columns: "reject_file"
      columns:
        - name: "order_id"
          type: "string"
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "credit_card"
          strategy: "hash""#;
    assert_validation_error(config, &["missing_columns", "reject_file", "PII masking"]);
}

#[test]
fn pii_mismatch_extra_reject_file_errors() {
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
      mismatch:
        extra_columns: "reject_file"
      columns:
        - name: "order_id"
          type: "string"
        - name: "credit_card"
          type: "string"
    pii:
      columns:
        - name: "credit_card"
          strategy: "hash""#;
    assert_validation_error(config, &["extra_columns", "reject_file", "PII masking"]);
}

#[test]
fn pii_hash_on_non_string_column_errors() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "amount"
          strategy: "hash""#,
    );
    assert_validation_error(&config, &["Hash", "string columns", "float64"]);
}

#[test]
fn pii_redact_on_non_string_column_errors() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "amount"
          strategy: "redact""#,
    );
    assert_validation_error(&config, &["Redact", "string columns", "float64"]);
}

#[test]
fn pii_nullify_on_non_string_column_is_valid() {
    let config = base_config_with_pii(
        r#"      columns:
        - name: "amount"
          strategy: "nullify""#,
    );
    assert_validation_ok(&config);
}

#[test]
fn pii_hash_on_unique_key_also_errors_in_append_mode() {
    // Confirm the rejection applies regardless of write_mode.
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
      write_mode: "append"
    policy:
      severity: "warn"
    schema:
      unique_keys:
        - ["email"]
      columns:
        - name: "order_id"
          type: "string"
        - name: "email"
          type: "string"
    pii:
      columns:
        - name: "email"
          strategy: "hash""#;
    assert_validation_error(config, &["hash", "unique-key", "run report"]);
}

#[test]
fn pii_legacy_unique_ignored_when_unique_keys_set() {
    // columns[].unique=true is ignored at runtime when schema.unique_keys is
    // present; validate_pii must not reject PII on such columns.
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
      unique_keys:
        - ["email"]
      columns:
        - name: "order_id"
          type: "string"
          unique: true
        - name: "email"
          type: "string"
    pii:
      columns:
        - name: "order_id"
          strategy: "nullify""#;
    assert_validation_ok(config);
}

#[test]
fn pii_with_archive_sink_errors() {
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
      archive:
        path: "/tmp/archive"
    incremental_mode: "archive"
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
          strategy: "hash""#;
    assert_validation_error(config, &["sink.archive", "unmasked", "pii"]);
}
