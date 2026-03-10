use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config::{SchemaEvolutionIncompatibleAction, SchemaEvolutionMode, WriteMode};
use floe_core::load_config;

static TEMP_CONFIG_SEQ: AtomicU64 = AtomicU64::new(0);

fn write_temp_config(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let seq = TEMP_CONFIG_SEQ.fetch_add(1, Ordering::Relaxed);
    path.push(format!("floe-config-{nanos}-{seq}.yml"));
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
    let schema_evolution = entity.schema.resolved_schema_evolution();
    assert_eq!(schema_evolution.mode, SchemaEvolutionMode::Strict);
    assert_eq!(
        schema_evolution.on_incompatible,
        SchemaEvolutionIncompatibleAction::Fail
    );
}

#[test]
fn parse_config_supports_version_0_2_and_schema_evolution() {
    let yaml = r#"
version: "0.2"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
        on_incompatible: "fail"
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.version, "0.2");
    let schema_evolution = config.entities[0]
        .schema
        .schema_evolution
        .expect("schema evolution");
    assert_eq!(schema_evolution.mode, SchemaEvolutionMode::AddColumns);
    assert_eq!(
        schema_evolution.on_incompatible,
        SchemaEvolutionIncompatibleAction::Fail
    );
}

#[test]
fn parse_config_supports_higher_version_with_schema_evolution() {
    let yaml = r#"
version: "0.3"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "delta"
        path: "/tmp/out"
    policy:
      severity: "warn"
    schema:
      schema_evolution:
        mode: "add_columns"
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");

    assert_eq!(config.version, "0.3");
    assert_eq!(
        config.entities[0]
            .schema
            .schema_evolution
            .expect("schema evolution")
            .mode,
        SchemaEvolutionMode::AddColumns
    );
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
fn parse_config_supports_sink_level_merge_scd1_write_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd1"
      accepted:
        format: "delta"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.sink.write_mode, WriteMode::MergeScd1);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::MergeScd1);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::MergeScd1
    );
    assert_eq!(entity.sink.resolved_write_mode(), WriteMode::MergeScd1);
}

#[test]
fn parse_config_supports_sink_level_merge_scd2_write_mode() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    assert_eq!(entity.sink.write_mode, WriteMode::MergeScd2);
    assert_eq!(entity.sink.accepted.write_mode, WriteMode::MergeScd2);
    assert_eq!(
        entity.sink.rejected.as_ref().unwrap().write_mode,
        WriteMode::MergeScd2
    );
    assert_eq!(entity.sink.resolved_write_mode(), WriteMode::MergeScd2);
}

#[test]
fn parse_config_supports_delta_merge_options_block() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      write_mode: "merge_scd2"
      accepted:
        format: "delta"
        path: "/tmp/out"
        merge:
          ignore_columns: ["ingested_at", "load_ts"]
          compare_columns: ["name", "address", "status"]
          scd2:
            current_flag_column: "__is_current"
            valid_from_column: "__valid_from"
            valid_to_column: "__valid_to"
      rejected:
        format: "csv"
        path: "/tmp/rejected"
    policy:
      severity: "reject"
    schema:
      primary_key: ["customer_id"]
      columns:
        - name: "customer_id"
          type: "string"
        - name: "name"
          type: "string"
        - name: "address"
          type: "string"
        - name: "status"
          type: "string"
        - name: "ingested_at"
          type: "datetime"
        - name: "load_ts"
          type: "datetime"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let entity = config.entities.first().expect("entity");
    let merge = entity
        .sink
        .accepted
        .merge
        .as_ref()
        .expect("sink.accepted.merge");

    assert_eq!(
        merge.ignore_columns.as_ref().expect("ignore columns"),
        &vec!["ingested_at".to_string(), "load_ts".to_string()]
    );
    assert_eq!(
        merge.compare_columns.as_ref().expect("compare columns"),
        &vec![
            "name".to_string(),
            "address".to_string(),
            "status".to_string()
        ]
    );
    let scd2 = merge.scd2.as_ref().expect("scd2 options");
    assert_eq!(scd2.current_flag_column.as_deref(), Some("__is_current"));
    assert_eq!(scd2.valid_from_column.as_deref(), Some("__valid_from"));
    assert_eq!(scd2.valid_to_column.as_deref(), Some("__valid_to"));
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
fn parse_config_supports_schema_primary_key_and_unique_keys() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
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
      severity: "reject"
    schema:
      primary_key: ["id", "country"]
      unique_keys:
        - ["id", "country"]
        - ["email"]
      columns:
        - name: "id"
          type: "string"
        - name: "country"
          type: "string"
        - name: "email"
          type: "string"
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let schema = &config.entities[0].schema;
    assert_eq!(
        schema.primary_key.as_ref().expect("primary key"),
        &vec!["id".to_string(), "country".to_string()]
    );
    assert_eq!(
        schema.unique_keys.as_ref().expect("unique keys"),
        &vec![
            vec!["id".to_string(), "country".to_string()],
            vec!["email".to_string()]
        ]
    );
}

#[test]
fn parse_config_prefers_schema_unique_keys_over_legacy_column_unique_flags() {
    let yaml = r#"
version: "0.1"
entities:
  - name: "users"
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
      severity: "reject"
    schema:
      unique_keys:
        - ["id", "country"]
      columns:
        - name: "id"
          type: "string"
          unique: true
        - name: "country"
          type: "string"
        - name: "email"
          type: "string"
          unique: true
"#;
    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let schema = &config.entities[0].schema;
    let constraints = floe_core::check::resolve_schema_unique_keys(schema);
    assert_eq!(
        constraints,
        vec![vec!["id".to_string(), "country".to_string()]]
    );
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

#[test]
fn parse_config_supports_catalogs_and_iceberg_catalog_binding() {
    let yaml = r#"
version: "0.1"
storages:
  default: "s3_out"
  definitions:
    - name: "s3_out"
      type: "s3"
      bucket: "data-bucket"
      region: "us-east-1"
      prefix: "accepted"
catalogs:
  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
      warehouse_storage: "s3_out"
      warehouse_prefix: "iceberg"
domains:
  - name: "sales"
    incoming_dir: "/tmp/incoming/sales"
entities:
  - name: "orders"
    domain: "sales"
    source:
      format: "csv"
      path: "/tmp/input"
    sink:
      accepted:
        format: "iceberg"
        path: "orders_table"
        iceberg:
          catalog: "glue_main"
          namespace: "sales_ops"
          table: "orders_fact"
          location: "custom/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
"#;

    let path = write_temp_config(yaml);
    let config = load_config(&path).expect("parse config");
    let catalogs = config.catalogs.as_ref().expect("catalogs");
    assert_eq!(catalogs.default.as_deref(), Some("glue_main"));
    assert_eq!(catalogs.definitions.len(), 1);
    assert_eq!(catalogs.definitions[0].catalog_type, "glue");
    assert_eq!(
        catalogs.definitions[0].database.as_deref(),
        Some("lakehouse")
    );
    assert_eq!(
        catalogs.definitions[0].warehouse_prefix.as_deref(),
        Some("iceberg")
    );

    let iceberg = config.entities[0]
        .sink
        .accepted
        .iceberg
        .as_ref()
        .expect("iceberg options");
    assert_eq!(iceberg.catalog.as_deref(), Some("glue_main"));
    assert_eq!(iceberg.namespace.as_deref(), Some("sales_ops"));
    assert_eq!(iceberg.table.as_deref(), Some("orders_fact"));
    assert_eq!(iceberg.location.as_deref(), Some("custom/orders"));
}
