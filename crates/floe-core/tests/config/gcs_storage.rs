use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::config::{RootConfig, StorageResolver};
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

fn load_temp_config(contents: &str) -> (PathBuf, RootConfig) {
    let path = write_temp_config(contents);
    let config = load_config(&path).expect("parse config");
    (path, config)
}

fn resolver_from(yaml: &str) -> StorageResolver {
    let (path, config) = load_temp_config(yaml);
    StorageResolver::new(&config, Path::new(&path)).expect("storage resolver")
}

#[test]
fn gcs_uri_with_prefix_is_resolved() {
    let yaml = r#"
version: "0.1"
storages:
  default: gcs_raw
  definitions:
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
      prefix: data/root
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "parquet"
        path: "out/customers"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
          nullable: false
"#;
    let resolver = resolver_from(yaml);
    let resolved = resolver
        .resolve_path(
            "customer",
            "source.path",
            Some("gcs_raw"),
            "incoming/customers.csv",
        )
        .expect("resolve");
    assert_eq!(
        resolved.uri,
        "gs://my-bucket/data/root/incoming/customers.csv"
    );
    assert!(resolved.local_path.is_none());
}

#[test]
fn gcs_uri_without_prefix_is_resolved() {
    let yaml = r#"
version: "0.1"
storages:
  default: gcs_raw
  definitions:
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "parquet"
        path: "out/customers"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
          nullable: false
"#;
    let resolver = resolver_from(yaml);
    let resolved = resolver
        .resolve_path(
            "customer",
            "source.path",
            Some("gcs_raw"),
            "/incoming/customers.csv",
        )
        .expect("resolve");
    assert_eq!(resolved.uri, "gs://my-bucket/incoming/customers.csv");
}

#[test]
fn gcs_uri_allows_explicit_bucket_path() {
    let yaml = r#"
version: "0.1"
storages:
  default: gcs_raw
  definitions:
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
      prefix: data/root
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "in/customers.csv"
    sink:
      accepted:
        format: "parquet"
        path: "out/customers"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "string"
          nullable: false
"#;
    let resolver = resolver_from(yaml);
    let resolved = resolver
        .resolve_path(
            "customer",
            "source.path",
            Some("gcs_raw"),
            "gs://my-bucket/explicit.csv",
        )
        .expect("resolve");
    assert_eq!(resolved.uri, "gs://my-bucket/explicit.csv");
}
