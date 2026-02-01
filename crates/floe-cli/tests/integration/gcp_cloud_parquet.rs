use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::io::storage::gcs::GcsClient;
use floe_core::io::storage::StorageClient;
use floe_core::FloeResult;

fn should_run() -> bool {
    matches!(env::var("FLOE_IT_GCP").as_deref(), Ok("1"))
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("lakehouse/it/gcp-{nanos}")
}

fn write_config(path: &PathBuf, bucket: &str, prefix: &str) -> FloeResult<()> {
    let config = format!(
        r#"
version: "0.1"

storages:
  default: "gcs_it"
  definitions:
    - name: "gcs_it"
      type: "gcs"
      bucket: "{bucket}"
      prefix: "{prefix}"

report:
  path: "report"
  storage: "gcs_it"

entities:
  - name: "orders"
    source:
      format: "parquet"
      path: "in/orders"
      storage: "gcs_it"
    sink:
      accepted:
        format: "parquet"
        path: "out/accepted/orders"
        storage: "gcs_it"
      rejected:
        format: "csv"
        path: "out/rejected/orders"
        storage: "gcs_it"
      archive:
        path: "archive"
        storage: "gcs_it"
    policy:
      severity: "warn"
    schema:
      mismatch:
        missing_columns: "fill_nulls"
        extra_columns: "ignore"
      columns:
        - name: "order_id"
          type: "string"
        - name: "customer_id"
          type: "string"
        - name: "order_total"
          type: "number"
        - name: "currency"
          type: "string"
"#
    );
    std::fs::write(path, config)?;
    Ok(())
}

struct GcsCleanup {
    bucket: String,
    prefix: String,
}

impl Drop for GcsCleanup {
    fn drop(&mut self) {
        if let Ok(client) = GcsClient::new(self.bucket.clone()) {
            if let Ok(objects) = client.list(&self.prefix) {
                for object in objects {
                    let _ = client.delete_object(&object.uri);
                }
            }
        }
    }
}

#[test]
fn it_gcs_parquet_cloud_end_to_end() -> FloeResult<()> {
    if !should_run() {
        eprintln!("skipping gcp integration test (set FLOE_IT_GCP=1)");
        return Ok(());
    }

    if env::var("GOOGLE_APPLICATION_CREDENTIALS").is_err() {
        return Err("GOOGLE_APPLICATION_CREDENTIALS must be set for GCP integration test".into());
    }

    let bucket = "floe-test";
    let prefix = unique_prefix();
    let _cleanup = GcsCleanup {
        bucket: bucket.to_string(),
        prefix: prefix.clone(),
    };

    let client = GcsClient::new(bucket.to_string())?;
    let local_parquet = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../example/out/accepted/orders/part-00000.parquet");
    let input_uri = format!("gs://{bucket}/{prefix}/in/orders/orders.parquet");
    client.upload_from_path(&local_parquet, &input_uri)?;

    let temp_dir = tempfile::TempDir::new()?;
    let config_path = temp_dir.path().join("it_config.yml");
    write_config(&config_path, bucket, &prefix)?;

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe"));
    cmd.arg("run").arg("-c").arg(&config_path);
    if let Ok(value) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        cmd.env("GOOGLE_APPLICATION_CREDENTIALS", value);
    }
    let status = cmd.status()?;
    assert!(status.success(), "floe run failed for gcp integration");

    let accepted_prefix = format!("{prefix}/out/accepted/orders");
    let accepted = client.list(&accepted_prefix)?;
    assert!(
        accepted.iter().any(|obj| obj.uri.ends_with(".parquet")),
        "expected accepted parquet output under {accepted_prefix}"
    );

    let report_prefix = format!("{prefix}/report");
    let reports = client.list(&report_prefix)?;
    assert!(
        reports
            .iter()
            .any(|obj| obj.uri.ends_with("/orders/run.json")),
        "expected run.json under {report_prefix}"
    );

    let archive_prefix = format!("{prefix}/archive/orders");
    let archived = client.list(&archive_prefix)?;
    assert!(
        archived.iter().any(|obj| obj.uri.ends_with(".parquet")),
        "expected archived input under {archive_prefix}"
    );

    Ok(())
}
