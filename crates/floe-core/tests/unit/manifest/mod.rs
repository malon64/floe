use floe_core::{
    build_common_manifest_json, load_config, parse_profile_from_str, resolve_config_location,
    ManifestOptions, PathMode,
};
use serde_json::Value;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn manifest_uses_local_uri_for_local_config() {
    let config_path = repo_root().join("example/config.yml");
    let expected_config_uri = format!(
        "local://{}",
        std::fs::canonicalize(&config_path)
            .expect("canonicalize config path")
            .display()
    );

    let config_location = resolve_config_location(
        config_path
            .to_str()
            .expect("example/config.yml path should be valid UTF-8"),
    )
    .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build common manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest is valid json");

    assert_eq!(value["schema"], "floe.manifest.v1");
    assert_eq!(value["config_uri"], expected_config_uri);
}

#[test]
fn manifest_id_is_stable_for_same_config() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(
        config_path
            .to_str()
            .expect("example/config.yml path should be valid UTF-8"),
    )
    .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let first = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let second = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");

    let first_value: Value = serde_json::from_str(&first).expect("valid json");
    let second_value: Value = serde_json::from_str(&second).expect("valid json");

    let first_id = first_value["manifest_id"]
        .as_str()
        .expect("manifest_id should be string");
    let second_id = second_value["manifest_id"]
        .as_str()
        .expect("manifest_id should be string");

    assert!(first_id.starts_with("mfv1-"));
    assert_eq!(first_id.len(), 21);
    assert_eq!(first_id, second_id);
}

#[test]
fn manifest_local_uris_are_normalized() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("manifest.yml");

    let yaml = r#"version: "0.1"
report:
  path: "./report/../report_out/./base"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "./data/../input/./customer.csv"
    sink:
      accepted:
        format: "parquet"
        path: "./out/../out_norm/./accepted/customer"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("config path utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("build common manifest");
    let value: Value = serde_json::from_str(&payload).expect("manifest json");

    let cfg_base = std::fs::canonicalize(&cfg_dir).expect("canonicalize cfg dir");
    assert_eq!(
        value["report_base_uri"],
        format!("local://{}/report_out/base", cfg_base.display())
    );
    let entity = &value["entities"][0];
    assert_eq!(
        entity["source"]["uri"],
        format!("local://{}/input/customer.csv", cfg_base.display())
    );
    assert_eq!(
        entity["accepted_sink_uri"],
        format!("local://{}/out_norm/accepted/customer", cfg_base.display())
    );
}

#[test]
fn manifest_without_profile_has_local_runner() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runners"]["default"], "local");
    assert_eq!(
        value["runners"]["definitions"]["local"]["type"],
        "local_process"
    );
}

#[test]
fn manifest_with_local_profile_has_local_runner() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: dev
execution:
  runner:
    type: local
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runners"]["default"], "local");
    assert_eq!(
        value["runners"]["definitions"]["local"]["type"],
        "local_process"
    );
}

#[test]
fn manifest_with_kubernetes_profile_has_kubernetes_runner() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-k8s
execution:
  runner:
    type: kubernetes_job
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["runners"]["default"], "default");
    assert_eq!(
        value["runners"]["definitions"]["default"]["type"],
        "kubernetes_job"
    );
}

#[test]
fn manifest_with_kubernetes_profile_serializes_k8_runner_fields() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-k8s
execution:
  runner:
    type: kubernetes_job
    image: my-registry/floe:latest
    namespace: floe-prod
    service_account: floe-sa
    command: floe
    args:
      - run
      - -c
      - /config/config.yml
    timeout_seconds: 3600
    ttl_seconds_after_finished: 600
    poll_interval_seconds: 15
    env:
      FLOE_ENV: prod
    resources:
      cpu: "500m"
      memory_mb: 512
    secrets:
      - name: DB_PASSWORD
        secret_name: floe-db-secret
        key: password
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(runner["type"], "kubernetes_job");
    assert_eq!(runner["image"], "my-registry/floe:latest");
    assert_eq!(runner["namespace"], "floe-prod");
    assert_eq!(runner["service_account"], "floe-sa");
    assert_eq!(runner["command"], "floe");
    assert_eq!(
        runner["args"],
        serde_json::json!(["run", "-c", "/config/config.yml"])
    );
    assert_eq!(runner["timeout_seconds"], 3600);
    assert_eq!(runner["ttl_seconds_after_finished"], 600);
    assert_eq!(runner["poll_interval_seconds"], 15);
    assert_eq!(runner["env"]["FLOE_ENV"], "prod");
    assert_eq!(runner["resources"]["cpu"], "500m");
    assert_eq!(runner["resources"]["memory_mb"], 512);
    assert_eq!(
        runner["secrets"],
        serde_json::json!([{
            "name": "DB_PASSWORD",
            "secret_name": "floe-db-secret",
            "key": "password"
        }])
    );
}

#[test]
fn manifest_with_databricks_profile_serializes_runner_fields() {
    let profile_yaml = r#"
apiVersion: floe/v1
kind: EnvironmentProfile
metadata:
  name: prod-dbx
execution:
  runner:
    type: databricks_job
    workspace_url: https://adb-1234.5.azuredatabricks.net
    existing_cluster_id: 1111-222222-abc123
    config_uri: dbfs:/floe/configs/prod.yml
    python_file_uri: dbfs:/floe/bin/floe_entry.py
    command: floe
    args:
      - run
      - -c
      - dbfs:/floe/configs/prod.yml
    poll_interval_seconds: 12
    timeout_seconds: 1800
    auth:
      service_principal_oauth_ref: env://DATABRICKS_TOKEN
    env_parameters:
      FLOE_ENV: prod
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        Some(&profile),
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(value["runners"]["default"], "default");
    assert_eq!(runner["type"], "databricks_job");
    assert_eq!(
        runner["workspace_url"],
        "https://adb-1234.5.azuredatabricks.net"
    );
    assert_eq!(runner["existing_cluster_id"], "1111-222222-abc123");
    assert_eq!(runner["config_uri"], "dbfs:/floe/configs/prod.yml");
    assert_eq!(runner["python_file_uri"], "dbfs:/floe/bin/floe_entry.py");
    assert_eq!(runner["job_name"], "floe-{domain}-{env}");
    assert_eq!(
        runner["auth"]["service_principal_oauth_ref"],
        "env://DATABRICKS_TOKEN"
    );
}

#[test]
fn manifest_deterministic_mode_produces_stable_output() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        deterministic: true,
        ..ManifestOptions::default()
    };

    let first =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let second =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");

    assert_eq!(
        first, second,
        "deterministic manifests must be byte-identical"
    );
}

#[test]
fn manifest_deterministic_mode_sets_timestamp_to_zero() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        deterministic: true,
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["generated_at_ts_ms"], 0);
}

#[test]
fn manifest_config_checksum_is_populated() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let payload = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let checksum = value["config_checksum"]
        .as_str()
        .expect("config_checksum should be present");
    assert!(
        checksum.starts_with("sha256:"),
        "config_checksum should start with 'sha256:'"
    );
}

#[test]
fn manifest_revision_is_present_and_stable() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");

    let first = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");
    let second = build_common_manifest_json(
        &config_location,
        &config,
        &[],
        None,
        &ManifestOptions::default(),
    )
    .expect("manifest");

    let first_value: Value = serde_json::from_str(&first).expect("valid json");
    let second_value: Value = serde_json::from_str(&second).expect("valid json");

    let first_rev = first_value["manifest_revision"]
        .as_str()
        .expect("manifest_revision should be present");
    let second_rev = second_value["manifest_revision"]
        .as_str()
        .expect("manifest_revision should be present");

    assert!(
        first_rev.starts_with("sha256:"),
        "revision should be a sha256 hash"
    );
    assert_eq!(
        first_rev, second_rev,
        "manifest_revision must be stable across calls"
    );
}

#[test]
fn manifest_name_is_stored_when_provided() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        manifest_name: Some("sales.prod".to_string()),
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    assert_eq!(value["manifest_name"], "sales.prod");
}

#[test]
fn manifest_manifest_uri_renders_placeholder() {
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        manifest_uri: Some("s3://my-bucket/manifests/example.json".to_string()),
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let base_args = value["execution"]["base_args"]
        .as_array()
        .expect("base_args should be array");
    let args_strs: Vec<&str> = base_args
        .iter()
        .map(|v| v.as_str().expect("string"))
        .collect();

    assert!(
        args_strs.contains(&"s3://my-bucket/manifests/example.json"),
        "manifest URI should be rendered into base_args"
    );
    assert!(
        !args_strs.contains(&"{manifest_uri}"),
        "placeholder should be replaced"
    );
}

#[test]
fn manifest_default_domain_applied_when_entity_has_none() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
entities:
  - name: "orders"
    source:
      format: "csv"
      path: "./in/orders.csv"
    sink:
      accepted:
        format: "parquet"
        path: "./out/accepted/orders"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        default_domain: Some("sales".to_string()),
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let entity = &value["entities"][0];
    assert_eq!(entity["domain"], "sales");
    assert_eq!(entity["group_name"], "sales");
    assert_eq!(
        entity["asset_key"],
        serde_json::json!(["sales", "orders"]),
        "asset_key should be prefixed with default domain"
    );
}

#[test]
fn manifest_path_mode_resolved_uri_sets_path_from_uri() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let cfg_dir = root.join("cfg");
    std::fs::create_dir_all(&cfg_dir).expect("cfg dir");
    let in_dir = cfg_dir.join("in");
    std::fs::create_dir_all(&in_dir).expect("in dir");
    std::fs::write(in_dir.join("data.csv"), "id\n1\n").expect("write csv");
    let config_path = cfg_dir.join("config.yml");

    let yaml = r#"version: "0.1"
entities:
  - name: "data"
    source:
      format: "csv"
      path: "./in/data.csv"
    sink:
      accepted:
        format: "parquet"
        path: "./out/accepted/data"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#;
    std::fs::write(&config_path, yaml).expect("write config");

    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let opts = ManifestOptions {
        path_mode: PathMode::ResolvedUri,
        ..ManifestOptions::default()
    };

    let payload =
        build_common_manifest_json(&config_location, &config, &[], None, &opts).expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let entity = &value["entities"][0];
    let source_path = entity["source"]["path"].as_str().expect("source.path");
    let source_uri = entity["source"]["uri"].as_str().expect("source.uri");

    assert!(
        entity["source"]["resolved"].as_bool().unwrap_or(false),
        "source should be resolved for a local path"
    );
    // Local URIs have the local:// scheme stripped so the StorageResolver receives
    // a plain filesystem path rather than an unrecognised local:// prefix.
    let expected_path = source_uri.strip_prefix("local://").unwrap_or(source_uri);
    assert_eq!(
        source_path, expected_path,
        "path should equal the filesystem path (local:// prefix stripped) when path_mode=resolved-uri"
    );
}
