use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use floe_core::config::{ConfigBase, StorageResolver};
use floe_core::io::format::InputFile;
use floe_core::io::storage::CloudClient;
use floe_core::load_config;
use floe_core::state::{
    claim_entity_inputs, inspect_entity_state_with_base, read_entity_state,
    reset_entity_state_with_base, resolve_entity_state_path, write_entity_state_atomic,
    EntityFileClaim, EntityFileState, EntityState, ENTITY_STATE_SCHEMA_V1, ENTITY_STATE_SCHEMA_V2,
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
    assert_eq!(resolved.local_path, None);
}

#[test]
fn resolves_default_entity_state_path_from_remote_config_into_remote_state() {
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
    let remote_base = ConfigBase::remote_from_uri(
        temp_dir.path().join("ephemeral-config-download"),
        "s3://raw-bucket/configs/floe.yml",
    )
    .expect("remote base");
    let resolver = StorageResolver::new(&config, remote_base).expect("resolver");

    let resolved = resolve_entity_state_path(&resolver, &config.entities[0]).expect("state path");
    let expected_uri = "s3://raw-bucket/landing/incoming/sales/.floe/state/sales/state.json";

    assert_eq!(resolved.uri, expected_uri);
    assert_eq!(resolved.local_path, None);
}

#[test]
fn resolves_remote_config_state_uri_for_all_remote_storage_backends() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");

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
        assert_eq!(resolved.local_path, None);
    }
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
        claims: Default::default(),
    };

    write_entity_state_atomic(&state_path, &state).expect("write state");
    let loaded = read_entity_state(&state_path)
        .expect("read state")
        .expect("state exists");

    let mut expected = state;
    expected.schema = ENTITY_STATE_SCHEMA_V2.to_string();
    assert_eq!(loaded, expected);
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

fn state_claim_config(
    temp_dir: &tempfile::TempDir,
) -> (
    floe_core::config::RootConfig,
    StorageResolver,
    std::path::PathBuf,
) {
    let source_dir = temp_dir.path().join("incoming");
    fs::create_dir_all(&source_dir).expect("mkdir source");
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
    let (config, resolver) = build_local_resolver(temp_dir, &yaml);
    (config, resolver, source_dir)
}

fn claim_input(uri: &str) -> InputFile {
    InputFile {
        source_uri: uri.to_string(),
        source_name: "sales.csv".to_string(),
        source_stem: "sales".to_string(),
        source_size: Some(42),
        source_mtime: Some("2026-04-22T08:00:00Z".to_string()),
    }
}

#[test]
fn claim_entity_inputs_skips_active_claims_from_other_runs() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let (config, resolver, source_dir) = state_claim_config(&temp_dir);
    let entity = &config.entities[0];
    let input = claim_input("local:///tmp/incoming/sales.csv");
    let mut cloud = CloudClient::new();

    let first = claim_entity_inputs(&resolver, &mut cloud, entity, "run-a", vec![input.clone()])
        .expect("first claim");
    assert_eq!(first.pending_inputs.len(), 1);
    assert!(first.claimed_state.is_some());

    let second = claim_entity_inputs(&resolver, &mut cloud, entity, "run-b", vec![input.clone()])
        .expect("second claim");
    assert!(second.pending_inputs.is_empty());
    assert_eq!(second.active_claims, vec![input.source_uri.clone()]);

    let state = read_entity_state(&source_dir.join(".floe/state/sales/state.json"))
        .expect("read state")
        .expect("state exists");
    assert_eq!(state.files.len(), 0);
    assert_eq!(state.claims.len(), 1);
    assert_eq!(state.claims[&input.source_uri].run_id, "run-a");
}

#[test]
fn claim_entity_inputs_removes_expired_claims_before_acquiring() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let (config, resolver, source_dir) = state_claim_config(&temp_dir);
    let entity = &config.entities[0];
    let input = claim_input("local:///tmp/incoming/sales.csv");
    let state_path = source_dir.join(".floe/state/sales/state.json");
    let mut state = EntityState::new("sales");
    state.claims.insert(
        input.source_uri.clone(),
        EntityFileClaim {
            run_id: "crashed-run".to_string(),
            acquired_at: "2000-01-01T00:00:00Z".to_string(),
            expires_at: "2000-01-01T01:00:00Z".to_string(),
            size: input.source_size,
            mtime: input.source_mtime.clone(),
        },
    );
    write_entity_state_atomic(&state_path, &state).expect("write expired claim");
    let mut cloud = CloudClient::new();

    let outcome = claim_entity_inputs(&resolver, &mut cloud, entity, "run-b", vec![input.clone()])
        .expect("claim after expiry");

    assert_eq!(outcome.pending_inputs.len(), 1);
    assert!(outcome.active_claims.is_empty());
    let state = read_entity_state(&state_path)
        .expect("read state")
        .expect("state exists");
    assert_eq!(state.claims.len(), 1);
    assert_eq!(state.claims[&input.source_uri].run_id, "run-b");
}

#[test]
fn inspect_entity_state_reports_current_state() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_dir = temp_dir.path().join("incoming");
    fs::create_dir_all(&source_dir).expect("mkdir source");
    let config_path = temp_dir.path().join("floe.yml");
    fs::write(
        &config_path,
        format!(
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
        ),
    )
    .expect("write config");

    let config = load_config(&config_path).expect("load config");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");
    let state_path = resolve_entity_state_path(&resolver, &config.entities[0])
        .expect("state path")
        .local_path
        .expect("local path");
    let state = EntityState {
        schema: ENTITY_STATE_SCHEMA_V1.to_string(),
        entity: "sales".to_string(),
        updated_at: Some("2026-04-22T09:00:00Z".to_string()),
        files: BTreeMap::from([(
            "local:///tmp/incoming/sales.csv".to_string(),
            EntityFileState {
                processed_at: "2026-04-22T08:59:00Z".to_string(),
                size: Some(42),
                mtime: Some("2026-04-22T08:00:00Z".to_string()),
            },
        )]),
        claims: Default::default(),
    };
    write_entity_state_atomic(&state_path, &state).expect("write state");

    let inspection = inspect_entity_state_with_base(
        &config_path,
        ConfigBase::local_from_path(&config_path),
        "sales",
    )
    .expect("inspect state");

    assert_eq!(inspection.entity_name, "sales");
    assert_eq!(inspection.incremental_mode.as_str(), "file");
    assert_eq!(
        inspection.path.local_path.as_deref(),
        Some(state_path.as_path())
    );
    let mut expected = state;
    expected.schema = ENTITY_STATE_SCHEMA_V2.to_string();
    assert_eq!(inspection.state, Some(expected));
}

fn write_reset_test_config() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let source_dir = temp_dir.path().join("incoming");
    fs::create_dir_all(&source_dir).expect("mkdir source");
    let config_path = temp_dir.path().join("floe.yml");
    fs::write(
        &config_path,
        format!(
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
        ),
    )
    .expect("write config");

    let config = load_config(&config_path).expect("load config");
    let resolver =
        StorageResolver::new(&config, ConfigBase::local_from_path(&config_path)).expect("resolver");
    let state_path = resolve_entity_state_path(&resolver, &config.entities[0])
        .expect("state path")
        .local_path
        .expect("local path");

    (temp_dir, config_path, state_path)
}

#[test]
fn reset_entity_state_removes_existing_state_file() {
    let (_temp_dir, config_path, state_path) = write_reset_test_config();
    write_entity_state_atomic(&state_path, &EntityState::new("sales")).expect("write state");

    let removed = reset_entity_state_with_base(
        &config_path,
        ConfigBase::local_from_path(&config_path),
        "sales",
    )
    .expect("reset state");

    assert!(removed);
    assert!(!state_path.exists());
}

#[test]
fn reset_entity_state_removes_malformed_state_file() {
    let (_temp_dir, config_path, state_path) = write_reset_test_config();
    fs::create_dir_all(state_path.parent().expect("parent")).expect("mkdir state parent");
    fs::write(&state_path, "{not valid json").expect("write malformed state");

    let removed = reset_entity_state_with_base(
        &config_path,
        ConfigBase::local_from_path(&config_path),
        "sales",
    )
    .expect("reset malformed state");

    assert!(removed);
    assert!(!state_path.exists());
}

#[test]
fn reset_entity_state_removes_mismatched_state_file() {
    let (_temp_dir, config_path, state_path) = write_reset_test_config();
    fs::create_dir_all(state_path.parent().expect("parent")).expect("mkdir state parent");
    fs::write(
        &state_path,
        r#"{"schema":"wrong.schema","entity":"other","updated_at":null,"files":{}}"#,
    )
    .expect("write mismatched state");

    let removed = reset_entity_state_with_base(
        &config_path,
        ConfigBase::local_from_path(&config_path),
        "sales",
    )
    .expect("reset mismatched state");

    assert!(removed);
    assert!(!state_path.exists());
}
