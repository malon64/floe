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

fn build_local_resolver(
    temp_dir: &tempfile::TempDir,
    yaml: &str,
) -> (floe_core::config::RootConfig, StorageResolver) {
    let config = load_config_from_yaml(temp_dir, yaml);
    let config_path = temp_dir.path().join("floe.yml");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");
    (config, resolver)
}

fn short_stable_hash_hex(value: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

fn with_cache_home<T>(cache_home: &Path, test: impl FnOnce() -> T) -> T {
    let previous = std::env::var_os("XDG_CACHE_HOME");
    std::env::set_var("XDG_CACHE_HOME", cache_home);
    let output = test();
    match previous {
        Some(value) => std::env::set_var("XDG_CACHE_HOME", value),
        None => std::env::remove_var("XDG_CACHE_HOME"),
    }
    output
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
    fs::create_dir_all(&source_dir).expect("create dotted dir");
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
    let (config, resolver) = build_local_resolver(&temp_dir, &yaml);

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(source_dir.join(".floe/state/sales/state.json").as_path())
    );
}

#[test]
fn resolves_default_entity_state_path_from_local_file_with_uppercase_extension() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_file = temp_dir.path().join("incoming/INPUT.CSV");
    fs::create_dir_all(source_file.parent().expect("parent")).expect("create parent");
    fs::write(&source_file, "id\n1\n").expect("write source file");
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
    let (config, resolver) = build_local_resolver(&temp_dir, &yaml);

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
fn resolves_default_entity_state_path_from_local_extensionless_file() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_file = temp_dir.path().join("incoming/manifest");
    fs::create_dir_all(source_file.parent().expect("parent")).expect("create parent");
    fs::write(&source_file, "id\n1\n").expect("write source file");
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
    let (config, resolver) = build_local_resolver(&temp_dir, &yaml);

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
fn resolves_default_entity_state_path_from_local_directory_with_file_like_suffix() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_dir = temp_dir.path().join("landing/archive.csv");
    fs::create_dir_all(&source_dir).expect("create source dir");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: "{}/"
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
    let (config, resolver) = build_local_resolver(&temp_dir, &yaml);

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
fn resolves_default_entity_state_path_from_windows_style_source_file() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: 'C:\data\INPUT.CSV'
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
                .join(r"C:\data")
                .join(".floe/state/sales/state.json")
                .as_path()
        )
    );
}

#[test]
fn resolves_default_entity_state_path_from_windows_style_glob_prefix() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let yaml = format!(
        r#"version: "0.1"
entities:
  - name: "sales"
    incremental_mode: "file"
    source:
      format: "csv"
      path: 'C:\data\*.csv'
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
                .join(r"C:\data")
                .join(".floe/state/sales/state.json")
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
    assert_eq!(
        resolved.local_path.as_deref(),
        Some(
            temp_dir
                .path()
                .join(".floe/state/sales/state.json")
                .as_path()
        )
    );
}

#[test]
fn resolves_default_entity_state_path_from_remote_config_into_persistent_cache() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache_home = temp_dir.path().join("cache-home");
    fs::create_dir_all(&cache_home).expect("cache dir");
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
    let remote_base = ConfigBase::remote_from_uri(
        temp_dir.path().join("ephemeral-config-download"),
        "s3://raw-bucket/configs/floe.yml",
    )
    .expect("remote base");
    let resolver = StorageResolver::new(&config, remote_base).expect("resolver");

    let resolved = with_cache_home(&cache_home, || {
        resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path")
    });
    let expected_uri = "s3://raw-bucket/landing/incoming/sales/.floe/state/sales/state.json";

    assert_eq!(resolved.uri, expected_uri);
    assert_eq!(
        resolved.local_path.as_deref(),
        Some(
            cache_home
                .join("floe/state")
                .join(short_stable_hash_hex(expected_uri))
                .join("sales/state.json")
                .as_path()
        )
    );
}

#[test]
fn resolves_remote_config_state_cache_for_all_remote_storage_backends() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let cache_home = temp_dir.path().join("cache-home");
    fs::create_dir_all(&cache_home).expect("cache dir");

    let cases = [
        (
            r#"version: "0.1"
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
"#,
            "s3://raw-bucket/configs/floe.yml",
            "s3://raw-bucket/landing/incoming/sales/.floe/state/sales/state.json",
        ),
        (
            r#"version: "0.1"
storages:
  default: "lake"
  definitions:
    - name: "lake"
      type: "gcs"
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
"#,
            "gs://raw-bucket/configs/floe.yml",
            "gs://raw-bucket/landing/incoming/sales/.floe/state/sales/state.json",
        ),
        (
            r#"version: "0.1"
storages:
  default: "lake"
  definitions:
    - name: "lake"
      type: "adls"
      account: "acct"
      container: "cont"
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
"#,
            "abfs://cont@acct.dfs.core.windows.net/configs/floe.yml",
            "abfs://cont@acct.dfs.core.windows.net/landing/incoming/sales/.floe/state/sales/state.json",
        ),
    ];

    with_cache_home(&cache_home, || {
        for (yaml, config_uri, expected_uri) in cases {
            let config = load_config_from_yaml(&temp_dir, yaml);
            let base = ConfigBase::remote_from_uri(
                temp_dir.path().join("ephemeral-config-download"),
                config_uri,
            )
            .expect("remote base");
            let resolver = StorageResolver::new(&config, base).expect("resolver");
            let resolved =
                resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

            assert_eq!(resolved.uri, expected_uri);
            assert_eq!(
                resolved.local_path,
                Some(
                    cache_home
                        .join("floe/state")
                        .join(short_stable_hash_hex(expected_uri))
                        .join("sales/state.json")
                )
            );
        }
    });
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
fn resolves_remote_entity_state_override_to_local_path() {
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
    state:
      path: "custom/state/sales.json"
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
    let (config, resolver) = build_local_resolver(&temp_dir, yaml);

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");

    assert_eq!(
        resolved.local_path.as_deref(),
        Some(temp_dir.path().join("custom/state/sales.json").as_path())
    );
    assert!(resolved.uri.starts_with("local://"));
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
