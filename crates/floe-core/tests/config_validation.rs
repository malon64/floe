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
    path.push(format!("floe-config-validation-{nanos}.yml"));
    fs::write(&path, contents).expect("write temp config");
    path
}

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

fn base_entity(name: &str) -> String {
    format!(
        r#"  - name: "{name}"
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
"#
    )
}

fn base_config(entities_yaml: &str) -> String {
    format!(
        r#"version: "0.1"
report:
  path: "/tmp/reports"
entities:
{entities_yaml}"#
    )
}

#[test]
fn missing_report_path_errors() {
    let yaml = format!(
        r#"version: "0.1"
report: {{}}
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_error(&yaml, &["report.path"]);
}

#[test]
fn empty_entities_errors() {
    let yaml = r#"version: "0.1"
report:
  path: "/tmp/reports"
entities: []
"#;
    assert_validation_error(yaml, &["entities", "empty"]);
}

#[test]
fn entity_missing_name_errors() {
    let yaml = r#"version: "0.1"
report:
  path: "/tmp/reports"
entities:
  - source:
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
"#;
    assert_validation_error(yaml, &["entity.name", "entities[0]"]);
}

#[test]
fn invalid_source_format_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "xml"
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
"#;
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["entity.name=customer", "source.format", "xml"]);
}

#[test]
fn invalid_cast_mode_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      cast_mode: "unsafe"
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
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "source.cast_mode", "unsafe"],
    );
}

#[test]
fn invalid_normalize_strategy_errors() {
    let entity = r#"  - name: "customer"
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
      normalize_columns:
        enabled: true
        strategy: "odd_case"
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "schema.normalize_columns.strategy",
            "odd_case",
        ],
    );
}

#[test]
fn invalid_column_type_errors() {
    let entity = r#"  - name: "customer"
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
          type: "uuid"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "schema.columns[0].type",
            "uuid",
            "customer_id",
        ],
    );
}

#[test]
fn invalid_policy_severity_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
    policy:
      severity: "block"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["entity.name=customer", "policy.severity", "block"]);
}

#[test]
fn duplicate_entity_names_error() {
    let entities = format!("{}{}", base_entity("customer"), base_entity("customer"));
    let yaml = base_config(&entities);
    assert_validation_error(&yaml, &["entity.name=customer", "duplicated"]);
}

#[test]
fn missing_sink_accepted_path_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(&yaml, &["entity.name=customer", "sink.accepted.path"]);
}

#[test]
fn rejected_format_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "parquet"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "sink.rejected.format", "parquet"],
    );
}

#[test]
fn unknown_root_field_errors() {
    let yaml = r#"version: "0.1"
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
extra_root: "oops"
"#;
    assert_validation_error(yaml, &["unknown field root.extra_root"]);
}

#[test]
fn unknown_entity_field_errors() {
    let entity = r#"  - name: "customer"
    tune: "fast"
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
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "unknown field entity.tune"],
    );
}

#[test]
fn unknown_nested_field_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      options:
        header: true
        sep: ";"
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
"#;
    let yaml = base_config(entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "unknown field source.options.sep"],
    );
}
