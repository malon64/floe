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

fn assert_validation_ok(contents: &str) {
    let path = write_temp_config(contents);
    validate(&path, ValidateOptions::default()).expect("expected config to be valid");
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
      rejected:
        format: "csv"
        path: "/tmp/rejected"
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

fn config_with_storages(storages_yaml: &str, entities_yaml: &str) -> String {
    format!(
        r#"version: "0.1"
storages:
{storages_yaml}
report:
  path: "/tmp/reports"
entities:
{entities_yaml}"#
    )
}

#[test]
fn missing_report_path_defaults() {
    let yaml = format!(
        r#"version: "0.1"
report: {{}}
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_ok(&yaml);
}

#[test]
fn report_section_optional() {
    let yaml = format!(
        r#"version: "0.1"
entities:
{}"#,
        base_entity("customer")
    );
    assert_validation_ok(&yaml);
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
fn json_source_defaults_to_array_mode() {
    let entity = r#"  - name: "customer"
    source:
      format: "json"
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
    assert_validation_ok(&yaml);
}

#[test]
fn json_source_rejects_unknown_mode() {
    let entity = r#"  - name: "customer"
    source:
      format: "json"
      path: "/tmp/input"
      options:
        json_mode: "lines"
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
        &[
            "entity.name=customer",
            "source.options.json_mode=lines",
            "array",
            "ndjson",
        ],
    );
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
fn iceberg_accepted_format_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
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
        &[
            "entity.name=customer",
            "sink.accepted.format=iceberg",
            "not supported yet",
        ],
    );
}

#[test]
fn parquet_sink_options_are_valid() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          compression: "gzip"
          row_group_size: 1000
          max_size_per_file: 268435456
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = base_config(entity);
    assert_validation_ok(&yaml);
}

#[test]
fn parquet_sink_invalid_compression_errors() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          compression: "fast"
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
        &[
            "entity.name=customer",
            "sink.accepted.options.compression",
            "fast",
        ],
    );
}

#[test]
fn parquet_sink_row_group_size_must_be_positive() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          row_group_size: 0
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
        &[
            "entity.name=customer",
            "sink.accepted.options.row_group_size",
        ],
    );
}

#[test]
fn parquet_sink_max_size_per_file_must_be_positive() {
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        options:
          max_size_per_file: 0
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
        &[
            "entity.name=customer",
            "sink.accepted.options.max_size_per_file",
        ],
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

#[test]
fn metadata_allows_extra_fields() {
    let yaml = r#"version: "0.1"
metadata:
  project: "demo"
  owner: "data"
  extra_key: "free-form"
report:
  path: "/tmp/reports"
entities:
  - name: "customer"
    metadata:
      domain: "sales"
      extra_note: "ok"
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
    assert_validation_ok(yaml);
}

#[test]
fn storages_optional_defaults_to_local() {
    let yaml = base_config(&base_entity("customer"));
    assert_validation_ok(&yaml);
}

#[test]
fn storages_local_defined_and_used() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
"#;
    let yaml = config_with_storages(storages, &base_entity("customer"));
    assert_validation_ok(&yaml);
}

#[test]
fn source_storage_override_works() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "local_alt"
      type: "local"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "local_alt"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn sink_accepted_storage_override_works() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "local_alt"
      type: "local"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
        storage: "local_alt"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn missing_storage_reference_errors() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "missing"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_error(
        &yaml,
        &["entity.name=customer", "source.storage", "missing"],
    );
}

#[test]
fn s3_storage_reference_ok() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "s3_raw"
      type: "s3"
      bucket: "demo"
      region: "us-east-1"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
      storage: "s3_raw"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_ok(&yaml);
}

#[test]
fn parquet_source_rejects_s3_storage() {
    let storages = r#"  default: "local"
  definitions:
    - name: "local"
      type: "local"
    - name: "s3_raw"
      type: "s3"
      bucket: "demo"
      region: "us-east-1"
"#;
    let entity = r#"  - name: "customer"
    source:
      format: "parquet"
      path: "/tmp/input"
      storage: "s3_raw"
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
    let yaml = config_with_storages(storages, entity);
    assert_validation_error(
        &yaml,
        &[
            "entity.name=customer",
            "source.format=parquet",
            "local storage",
        ],
    );
}
