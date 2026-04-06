use floe_core::{
    build_common_manifest_json, load_config, parse_profile_from_str, resolve_config_location,
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

    let payload = build_common_manifest_json(&config_location, &config, &[], None)
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

    let first = build_common_manifest_json(&config_location, &config, &[], None).expect("manifest");
    let second =
        build_common_manifest_json(&config_location, &config, &[], None).expect("manifest");

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
    let payload = build_common_manifest_json(&config_location, &config, &[], None)
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
    let payload =
        build_common_manifest_json(&config_location, &config, &[], None).expect("manifest");
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
    let payload = build_common_manifest_json(&config_location, &config, &[], Some(&profile))
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
    let payload = build_common_manifest_json(&config_location, &config, &[], Some(&profile))
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
    command: floe
    args:
      - run
      - -c
      - /config/config.yml
    timeout_seconds: 3600
    ttl_seconds_after_finished: 600
    poll_interval_seconds: 15
    secrets:
      - floe-db
      - floe-warehouse
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(&config_location, &config, &[], Some(&profile))
        .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(runner["type"], "kubernetes_job");
    assert_eq!(runner["command"], "floe");
    assert_eq!(
        runner["args"],
        serde_json::json!(["run", "-c", "/config/config.yml"])
    );
    assert_eq!(runner["timeout_seconds"], 3600);
    assert_eq!(runner["ttl_seconds_after_finished"], 600);
    assert_eq!(runner["poll_interval_seconds"], 15);
    assert_eq!(
        runner["secrets"],
        serde_json::json!(["floe-db", "floe-warehouse"])
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
    command: floe
    args:
      - run
      - -c
      - dbfs:/floe/configs/prod.yml
    poll_interval_seconds: 12
    timeout_seconds: 1800
    auth:
      service_principal_oauth_ref: secret://kv/databricks/oauth
    env_parameters:
      FLOE_ENV: prod
"#;
    let profile = parse_profile_from_str(profile_yaml).expect("parse profile");
    let config_path = repo_root().join("example/config.yml");
    let config_location = resolve_config_location(config_path.to_str().expect("utf8"))
        .expect("resolve config location");
    let config = load_config(&config_location.path).expect("load config");
    let payload = build_common_manifest_json(&config_location, &config, &[], Some(&profile))
        .expect("manifest");
    let value: Value = serde_json::from_str(&payload).expect("valid json");

    let runner = &value["runners"]["definitions"]["default"];
    assert_eq!(value["runners"]["default"], "default");
    assert_eq!(runner["type"], "databricks_job");
    assert_eq!(runner["workspace_url"], "https://adb-1234.5.azuredatabricks.net");
    assert_eq!(runner["existing_cluster_id"], "1111-222222-abc123");
    assert_eq!(runner["config_uri"], "dbfs:/floe/configs/prod.yml");
    assert_eq!(runner["job_name"], "floe-{domain}-{env}");
    assert_eq!(
        runner["auth"]["service_principal_oauth_ref"],
        "secret://kv/databricks/oauth"
    );
}
