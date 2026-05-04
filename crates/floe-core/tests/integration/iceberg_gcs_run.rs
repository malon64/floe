use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::io::storage::{gcs::GcsClient, StorageClient};
use floe_core::{run, RunOptions};

fn write_csv(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write csv");
    path
}

fn write_config(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).expect("write config");
    path
}

#[test]
fn dry_run_accepts_iceberg_sink_on_gcs_storage() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");

    fs::create_dir_all(&input_dir).expect("create input dir");
    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n");

    let yaml = format!(
        r#"version: "0.1"
storages:
  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "gcs_out"
      type: "gcs"
      bucket: "example-bucket"
      prefix: "floe-tests"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "customer_iceberg"
        storage: "gcs_out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
        - name: "name"
          type: "string"
"#,
        input_dir = input_dir.display(),
    );
    let config_path = write_config(root, "config.yml", &yaml);

    let outcome = run(
        &config_path,
        RunOptions {
            profile: None,
            run_id: Some("it-iceberg-gcs-dry".to_string()),
            entities: Vec::new(),
            dry_run: true,
        },
    )
    .expect("dry run config");

    let previews = outcome.dry_run_previews.expect("dry run previews");
    assert_eq!(previews.len(), 1);
    assert_eq!(previews[0].name, "customer");
    assert_eq!(previews[0].scanned_files.len(), 1);
}

#[test]
fn manual_gcs_iceberg_append_and_overwrite_write_layout_and_cleanup() {
    if env::var("FLOE_RUN_MANUAL_GCS_ICEBERG_TEST").as_deref() != Ok("1") {
        eprintln!("skipping manual GCS Iceberg test; set FLOE_RUN_MANUAL_GCS_ICEBERG_TEST=1");
        return;
    }

    let bucket = env::var("FLOE_TEST_GCS_BUCKET").unwrap_or_else(|_| "floe-test".to_string());
    let prefix = env::var("FLOE_TEST_GCS_PREFIX").unwrap_or_else(|_| {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis();
        format!("manual/floe/iceberg-gcs-{stamp}")
    });

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let table_rel = "customer_iceberg";
    let table_prefix = join_key(&prefix, table_rel);
    let mut gcs = GcsClient::new(bucket.clone()).expect("gcs client");
    cleanup_gcs_prefix(&mut gcs, &table_prefix);

    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n2,bob\n");
    let append_cfg = write_config(
        root,
        "config_append.yml",
        &build_gcs_iceberg_config(
            &input_dir,
            &report_dir,
            &bucket,
            &prefix,
            table_rel,
            "append",
        ),
    );
    let out1 = run(
        &append_cfg,
        RunOptions {
            profile: None,
            run_id: Some("it-iceberg-gcs-append1".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("append run 1");
    let report1 = &out1.entity_outcomes[0].report;
    assert_eq!(report1.sink.accepted.format, "iceberg");
    assert_eq!(
        report1.accepted_output.write_mode.as_deref(),
        Some("append")
    );
    assert!(report1.accepted_output.snapshot_id.is_some());
    let snapshot1 = report1.accepted_output.snapshot_id;
    let version1 = report1.accepted_output.table_version;

    write_csv(&input_dir, "data.csv", "id,name\n3,cara\n");
    let out2 = run(
        &append_cfg,
        RunOptions {
            profile: None,
            run_id: Some("it-iceberg-gcs-append2".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("append run 2");
    let report2 = &out2.entity_outcomes[0].report;
    assert_eq!(
        report2.accepted_output.write_mode.as_deref(),
        Some("append")
    );
    assert!(report2.accepted_output.snapshot_id.is_some());
    assert_ne!(report2.accepted_output.snapshot_id, snapshot1);
    assert!(report2.accepted_output.table_version > version1);

    write_csv(&input_dir, "data.csv", "id,name\n10,zed\n");
    let overwrite_cfg = write_config(
        root,
        "config_overwrite.yml",
        &build_gcs_iceberg_config(
            &input_dir,
            &report_dir,
            &bucket,
            &prefix,
            table_rel,
            "overwrite",
        ),
    );
    let out3 = run(
        &overwrite_cfg,
        RunOptions {
            profile: None,
            run_id: Some("it-iceberg-gcs-overwrite".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("overwrite run");
    let report3 = &out3.entity_outcomes[0].report;
    assert_eq!(
        report3.accepted_output.write_mode.as_deref(),
        Some("overwrite")
    );
    assert!(report3.accepted_output.snapshot_id.is_some());

    let objects = gcs.list(&table_prefix).expect("list table objects");
    assert!(objects.iter().any(|obj| obj.key.contains("/metadata/")));
    assert!(objects.iter().any(|obj| obj.key.contains("/data/")));
    assert!(objects
        .iter()
        .any(|obj| obj.key.ends_with(".metadata.json")));

    cleanup_gcs_prefix(&mut gcs, &table_prefix);
}

fn build_gcs_iceberg_config(
    input_dir: &Path,
    report_dir: &Path,
    bucket: &str,
    prefix: &str,
    table_rel: &str,
    write_mode: &str,
) -> String {
    format!(
        r#"version: "0.1"
report:
  path: "{report_dir}"
storages:
  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "gcs_out"
      type: "gcs"
      bucket: "{bucket}"
      prefix: "{prefix}"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "{input_dir}"
      storage: "local_fs"
    sink:
      write_mode: "{write_mode}"
      accepted:
        format: "iceberg"
        path: "{table_rel}"
        storage: "gcs_out"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "id"
          type: "number"
        - name: "name"
          type: "string"
"#,
        report_dir = report_dir.display(),
        input_dir = input_dir.display(),
        bucket = bucket,
        prefix = prefix,
        write_mode = write_mode,
        table_rel = table_rel,
    )
}

fn join_key(prefix: &str, suffix: &str) -> String {
    let left = prefix.trim_matches('/');
    let right = suffix.trim_matches('/');
    if left.is_empty() {
        right.to_string()
    } else if right.is_empty() {
        left.to_string()
    } else {
        format!("{left}/{right}")
    }
}

fn cleanup_gcs_prefix(gcs: &mut GcsClient, prefix: &str) {
    let objects = match gcs.list(prefix) {
        Ok(objects) => objects,
        Err(_) => return,
    };
    for object in objects {
        let _ = gcs.delete_object(&object.uri);
    }
}
