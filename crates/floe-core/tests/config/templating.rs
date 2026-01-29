use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::load_config;

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_file(path: &Path, contents: &str) -> PathBuf {
    fs::write(path, contents).expect("write file");
    path.to_path_buf()
}

fn write_config(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("config.yml");
    write_file(&path, contents)
}

fn write_env(dir: &Path, contents: &str) -> PathBuf {
    let path = dir.join("env.yml");
    write_file(&path, contents)
}

#[test]
fn env_file_vars_apply_to_source_path() {
    let root = temp_dir("floe-env-file");
    let env_path = write_env(&root, "incoming_path: \"/data/incoming\"");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  file: "{env_path}"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{{{{incoming_path}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        env_path = env_path.display(),
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let parsed = load_config(&config_path).expect("parse config");
    assert_eq!(parsed.entities[0].source.path, "/data/incoming/customer");
}

#[test]
fn env_vars_override_env_file() {
    let root = temp_dir("floe-env-override");
    let env_path = write_env(&root, "incoming_path: \"/data/incoming\"");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  file: "{env_path}"
  vars:
    incoming_path: "/override"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{{{{incoming_path}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        env_path = env_path.display(),
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let parsed = load_config(&config_path).expect("parse config");
    assert_eq!(parsed.entities[0].source.path, "/override/customer");
}

#[test]
fn domain_incoming_dir_uses_globals() {
    let root = temp_dir("floe-domain");
    let config_yaml = format!(
        r#"version: "0.1"
env:
  vars:
    base_path: "/data/base"
domains:
  - name: "sales"
    incoming_dir: "{{{{base_path}}}}/sales"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "{{{{domain.incoming_dir}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let parsed = load_config(&config_path).expect("parse config");
    let domain = parsed
        .domains
        .iter()
        .find(|domain| domain.name == "sales")
        .expect("domain found");
    assert_eq!(
        domain.resolved_incoming_dir.as_deref(),
        Some("/data/base/sales")
    );
    assert_eq!(parsed.entities[0].source.path, "/data/base/sales/customer");
}

#[test]
fn unresolved_placeholder_errors_with_entity_context() {
    let root = temp_dir("floe-unresolved");
    let config_yaml = format!(
        r#"version: "0.1"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{{{{missing}}}}/customer"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let err = load_config(&config_path).expect_err("placeholder error");
    let msg = err.to_string();
    assert!(msg.contains("entity.name=customer"));
    assert!(msg.contains("entities.source.path references unknown variable missing"));
}

#[test]
fn unknown_domain_errors() {
    let root = temp_dir("floe-unknown-domain");
    let config_yaml = format!(
        r#"version: "0.1"
report:
  path: "{root}/out"
entities:
  - name: "customer"
    domain: "missing"
    source:
      format: "csv"
      path: "/tmp"
    sink:
      accepted:
        format: "parquet"
        path: "{root}/out/accepted"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
"#,
        root = root.display(),
    );
    let config_path = write_config(&root, &config_yaml);

    let err = load_config(&config_path).expect_err("domain error");
    let msg = err.to_string();
    assert!(msg.contains("entity.name=customer references unknown domain missing"));
}
