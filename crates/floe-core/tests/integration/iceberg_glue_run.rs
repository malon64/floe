use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_glue::config::Region as GlueRegion;
use aws_sdk_glue::Client as GlueClient;
use floe_core::io::storage::{s3::S3Client, StorageClient};
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
fn dry_run_accepts_iceberg_sink_with_glue_catalog_binding_on_s3() {
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
    - name: "s3_out"
      type: "s3"
      bucket: "example-bucket"
      region: "us-east-1"
catalogs:
  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
      warehouse_storage: "s3_out"
      warehouse_prefix: "floe-tests/warehouse"
domains:
  - name: "sales"
    incoming_dir: "{input_dir}"
entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "{input_dir}"
      storage: "local_fs"
    sink:
      accepted:
        format: "iceberg"
        path: "unused/customer_iceberg"
        storage: "s3_out"
        iceberg:
          catalog: "glue_main"
          table: "customer_glue"
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
            run_id: Some("it-iceberg-glue-dry".to_string()),
            entities: Vec::new(),
            dry_run: true,
        },
    )
    .expect("dry run config");

    let previews = outcome.dry_run_previews.expect("dry run previews");
    assert_eq!(previews.len(), 1);
    assert_eq!(previews[0].name, "customer");
}

#[test]
fn manual_glue_iceberg_append_and_overwrite_updates_glue_table_and_s3_layout() {
    if env::var("FLOE_RUN_MANUAL_GLUE_ICEBERG_TEST").as_deref() != Ok("1") {
        eprintln!("skipping manual Glue Iceberg test; set FLOE_RUN_MANUAL_GLUE_ICEBERG_TEST=1");
        return;
    }

    let bucket = env::var("FLOE_TEST_S3_BUCKET").unwrap_or_else(|_| "floe-test".to_string());
    let region = env::var("FLOE_TEST_GLUE_REGION")
        .or_else(|_| env::var("FLOE_TEST_S3_REGION"))
        .or_else(|_| env::var("AWS_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    let database = env::var("FLOE_TEST_GLUE_DATABASE").unwrap_or_else(|_| "floe_test".to_string());
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_millis();
    let prefix = env::var("FLOE_TEST_GLUE_PREFIX")
        .unwrap_or_else(|_| format!("manual/floe/iceberg-glue-{stamp}"));
    let table_name = format!("customer_glue_{stamp}");

    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let root = temp_dir.path();
    let input_dir = root.join("in");
    let report_dir = root.join("report");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let mut s3 = S3Client::new(bucket.clone(), Some(&region)).expect("s3 client");
    cleanup_s3_prefix(&mut s3, &prefix);
    let glue_client = glue_client(&region);
    cleanup_glue_table(&glue_client, &database, &table_name);

    write_csv(&input_dir, "data.csv", "id,name\n1,alice\n2,bob\n");
    let append_cfg = write_config(
        root,
        "config_append.yml",
        &build_glue_iceberg_config(
            &input_dir,
            &report_dir,
            &bucket,
            &region,
            &database,
            &prefix,
            &table_name,
            "append",
        ),
    );
    let out1 = run(
        &append_cfg,
        RunOptions {
            run_id: Some("it-iceberg-glue-append1".to_string()),
            entities: Vec::new(),
            dry_run: false,
        },
    )
    .expect("append run 1");
    let report1 = &out1.entity_outcomes[0].report;
    let expected_database = database.to_ascii_lowercase();
    assert_eq!(report1.sink.accepted.format, "iceberg");
    assert_eq!(
        report1.accepted_output.iceberg_catalog_name.as_deref(),
        Some("glue_main")
    );
    assert_eq!(
        report1.accepted_output.iceberg_database.as_deref(),
        Some(expected_database.as_str())
    );
    assert_eq!(
        report1.accepted_output.iceberg_table.as_deref(),
        Some(table_name.as_str())
    );
    assert_eq!(
        report1.accepted_output.write_mode.as_deref(),
        Some("append")
    );
    assert!(report1.accepted_output.snapshot_id.is_some());
    let snapshot1 = report1.accepted_output.snapshot_id;
    let version1 = report1.accepted_output.table_version;
    let table_root = report1
        .accepted_output
        .table_root_uri
        .as_ref()
        .expect("table_root_uri")
        .clone();
    assert!(table_root.starts_with(&format!("s3://{bucket}/")));
    assert!(table_root.contains("/warehouse/"));

    write_csv(&input_dir, "data.csv", "id,name\n3,cara\n");
    let out2 = run(
        &append_cfg,
        RunOptions {
            run_id: Some("it-iceberg-glue-append2".to_string()),
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
        &build_glue_iceberg_config(
            &input_dir,
            &report_dir,
            &bucket,
            &region,
            &database,
            &prefix,
            &table_name,
            "overwrite",
        ),
    );
    let out3 = run(
        &overwrite_cfg,
        RunOptions {
            run_id: Some("it-iceberg-glue-overwrite".to_string()),
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

    let glue_table = get_glue_table(&glue_client, &database, &table_name);
    let metadata_location = glue_table
        .parameters()
        .and_then(|params| params.get("metadata_location"))
        .expect("glue metadata_location");
    assert!(metadata_location.ends_with(".metadata.json"));
    assert_eq!(
        glue_table
            .storage_descriptor()
            .and_then(|sd| sd.location())
            .expect("glue location"),
        table_root
    );

    let objects = s3.list(prefix.as_str()).expect("list table objects");
    assert!(objects.iter().any(|obj| obj.key.contains("/metadata/")));
    assert!(objects.iter().any(|obj| obj.key.contains("/data/")));

    cleanup_glue_table(&glue_client, &database, &table_name);
    cleanup_s3_prefix(&mut s3, &prefix);
}

fn build_glue_iceberg_config(
    input_dir: &Path,
    report_dir: &Path,
    bucket: &str,
    region: &str,
    database: &str,
    prefix: &str,
    table_name: &str,
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
    - name: "s3_out"
      type: "s3"
      bucket: "{bucket}"
      region: "{region}"
catalogs:
  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "{region}"
      database: "{database}"
      warehouse_storage: "s3_out"
      warehouse_prefix: "{prefix}/warehouse"
domains:
  - name: "sales"
    incoming_dir: "{input_dir}"
entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "{input_dir}"
      storage: "local_fs"
    sink:
      write_mode: "{write_mode}"
      accepted:
        format: "iceberg"
        path: "unused/customer_iceberg"
        storage: "s3_out"
        iceberg:
          catalog: "glue_main"
          table: "{table_name}"
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
        region = region,
        database = database,
        prefix = prefix.trim_matches('/'),
        write_mode = write_mode,
        table_name = table_name,
    )
}

fn glue_client(region: &str) -> GlueClient {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let region_provider = RegionProviderChain::first_try(GlueRegion::new(region.to_string()))
            .or_default_provider();
        let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        GlueClient::new(&cfg)
    })
}

fn cleanup_glue_table(glue: &GlueClient, database: &str, table: &str) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let _ = runtime.block_on(async {
        glue.delete_table()
            .database_name(database)
            .name(table)
            .send()
            .await
    });
}

fn get_glue_table(glue: &GlueClient, database: &str, table: &str) -> aws_sdk_glue::types::Table {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime
        .block_on(async {
            glue.get_table()
                .database_name(database)
                .name(table)
                .send()
                .await
        })
        .expect("get glue table")
        .table()
        .cloned()
        .expect("table")
}

fn cleanup_s3_prefix(s3: &mut S3Client, prefix: &str) {
    let objects = match s3.list(prefix) {
        Ok(objects) => objects,
        Err(_) => return,
    };
    for object in objects {
        let _ = s3.delete_object(&object.uri);
    }
}
