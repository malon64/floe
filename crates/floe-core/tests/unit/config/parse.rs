use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config::WriteMode;
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
      rejected:
        format: "csv"
        path: "/tmp/rejected"
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
    assert_eq!(entity.source.cast_mode.as_deref(), Some("strict"));
    let options = entity.source.options.as_ref().expect("options");
    assert_eq!(options.header, Some(true));
    assert_eq!(options.separator.as_deref(), Some(";"));
    assert_eq!(options.encoding.as_deref(), Some("UTF8"));
    assert_eq!(entity.sink.write_mode, WriteMode::Overwrite);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::Overwrite);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::Overwrite
    );
    assert_eq!(entity.sink.resolved_write_mode(), WriteMode::Overwrite);
}

#[test]
fn parse_config_supports_sink_level_append_write_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "append"
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.sink.write_mode, WriteMode::Append);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::Append);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::Append
    );
    assert_eq!(entity.sink.resolved_write_mode(), WriteMode::Append);
}

#[test]
fn parse_config_defaults_column_source_to_name() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
    source:
      format: "json"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "user_first_name"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let column = &config.entities[0].schema.columns[0];
    assert_eq!(column.name, "user_first_name");
    assert_eq!(column.source_or_name(), "user_first_name");
    assert!(column.source.is_none());
}

#[test]
fn parse_config_preserves_column_source() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
    source:
      format: "json"
      path: "/tmp/input"
    sink:
      accepted:
        format: "parquet"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "user_first_name"
          source: "user.names[0]"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let column = &config.entities[0].schema.columns[0];
    assert_eq!(column.name, "user_first_name");
    assert_eq!(column.source.as_deref(), Some("user.names[0]"));
    assert_eq!(column.source_or_name(), "user.names[0]");
}

#[test]
fn parse_config_supports_sink_partitioning_and_file_size_knobs() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "events_delta"
    source:
      format: "csv"
      path: "/tmp/input.csv"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out_delta"
        partition_by: ["event_date", "country"]
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "event_date"
          type: "date"
        - name: "country"
          type: "string"
  - name: "events_iceberg"
    source:
      format: "csv"
      path: "/tmp/input.csv"
    sink:
      accepted:
        format: "iceberg"
        path: "/tmp/out_iceberg"
        options:
          max_size_per_file: 268435456
        partition_spec:
          - column: "event_date"
            transform: "day"
          - column: "country"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "event_date"
          type: "date"
        - name: "country"
          type: "string"
"#;

    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    let delta = &config.entities[0];
    assert_eq!(
        delta
            .sink
            .accepted
            .partition_by
            .as_ref()
            .expect("partition_by"),
        &vec!["event_date".to_string(), "country".to_string()]
    );
    assert!(delta.sink.accepted.partition_spec.is_none());

    let iceberg = &config.entities[1];
    assert_eq!(
        iceberg
            .sink
            .accepted
            .options
            .as_ref()
            .expect("options")
            .max_size_per_file,
        Some(268435456)
    );
    let spec = iceberg
        .sink
        .accepted
        .partition_spec
        .as_ref()
        .expect("partition_spec");
    assert_eq!(spec.len(), 2);
    assert_eq!(spec[0].column, "event_date");
    assert_eq!(spec[0].transform, "day");
    assert_eq!(spec[1].column, "country");
    assert_eq!(spec[1].transform, "identity");
}
