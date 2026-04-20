use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use floe_core::config::{ConfigBase, StorageResolver};
use floe_core::load_config;
use floe_core::state::{
    read_entity_state, resolve_entity_state_path, write_entity_state_atomic, EntityFileState,
    EntityState, ENTITY_STATE_SCHEMA_V1,
};

fn load_config_from_yaml(
    temp_dir: &tempfile::TempDir,
    yaml: &str,
) -> floe_core::config::RootConfig {
    let path = temp_dir.path().join("floe.yml");
    fs::write(&path, yaml).expect("write config");
    load_config(&path).expect("load config")
}

#[test]
fn resolves_default_entity_state_path_from_local_source_directory() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_dir = temp_dir.path().join("incoming");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{}"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        source_dir.display(),
        temp_dir.path().join("out").display()
    );
    let config = load_config_from_yaml(&temp_dir, &yaml);
    let resolver = StorageResolver::new(
        &config,
        ConfigBase::local_from_path(&temp_dir.path().join("floe.yml")),
    )
    .expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(source_dir.join(".floe/state/sales/state.json").as_path())
    );
}

#[test]
fn resolves_default_entity_state_path_from_glob_source_prefix() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "incoming/*.csv"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        temp_dir.path().join("out").display()
    );
    let config = load_config_from_yaml(&temp_dir, &yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(
            temp_dir
                .path()
                .join("incoming/.floe/state/sales/state.json")
                .as_path()
        )
    );
}

#[test]
fn resolves_default_entity_state_path_from_dotted_source_directory() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_dir = temp_dir.path().join("incoming/v1.2");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{}"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        source_dir.display(),
        temp_dir.path().join("out").display()
    );
    let config = load_config_from_yaml(&temp_dir, &yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(source_dir.join(".floe/state/sales/state.json").as_path())
    );
}

#[test]
fn resolves_default_entity_state_path_from_hidden_source_directory() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_dir = temp_dir.path().join("data/.snapshot");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{}"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        source_dir.display(),
        temp_dir.path().join("out").display()
    );
    let config = load_config_from_yaml(&temp_dir, &yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(source_dir.join(".floe/state/sales/state.json").as_path())
    );
}

#[test]
fn resolves_default_entity_state_path_from_explicit_source_file() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_file = temp_dir.path().join("incoming/v1.2.csv");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{}"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        source_file.display(),
        temp_dir.path().join("out").display()
    );
    let config = load_config_from_yaml(&temp_dir, &yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(
            temp_dir
                .path()
                .join("incoming/.floe/state/sales/state.json")
                .as_path()
        )
    );
}

#[test]
fn resolves_default_entity_state_path_from_remote_source_context() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let yaml = r#"version: "0.1"
storages:
  default: "lake"
  definitions:
    - name: "lake"
      type: "s3"
      bucket: "raw-bucket"
      prefix: "landing"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "incoming/sales"
    sink:
      accepted:
        format: "parquet"
        path: "curated/sales"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    let config = load_config_from_yaml(&temp_dir, yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.uri,
        "s3://raw-bucket/landing/incoming/sales/.floe/state/sales/state.json"
    );
}

#[test]
fn resolves_entity_state_path_override() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    state:
      path: "custom/state/sales.json"
    source:
      format: "csv"
      path: "incoming"
    sink:
      accepted:
        format: "parquet"
        path: "{}"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        temp_dir.path().join("out").display()
    );
    let config = load_config_from_yaml(&temp_dir, &yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(temp_dir.path().join("custom/state/sales.json").as_path())
    );
}

#[test]
fn writes_and_reads_entity_state_atomically() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let state_path = temp_dir
        .path()
        .join("incoming/.floe/state/sales/state.json");
    let mut files = BTreeMap::new();
    files.insert(
        "local:///tmp/sales.csv".to_string(),
        EntityFileState {
            processed_at: "2026-04-20T12:59:14Z".to_string(),
            size: Some(182731),
            mtime: Some("2026-04-20T11:30:00Z".to_string()),
        },
    );
    let state = EntityState {
        schema: ENTITY_STATE_SCHEMA_V1.to_string(),
        entity: "sales".to_string(),
        updated_at: Some("2026-04-20T13:00:00Z".to_string()),
        files,
    };

    write_entity_state_atomic(&state_path, &state).expect("write state");
    let loaded = read_entity_state(&state_path)
        .expect("read state")
        .expect("state exists");

    assert_eq!(loaded, state);
    assert!(Path::new(&state_path).exists());
    assert_eq!(
        fs::read_dir(state_path.parent().expect("parent"))
            .expect("read dir")
            .filter_map(Result::ok)
            .count(),
        1
    );
}

#[test]
fn read_entity_state_returns_none_when_missing() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let missing = temp_dir.path().join("missing/state.json");
    assert!(read_entity_state(&missing).expect("read missing").is_none());
}
