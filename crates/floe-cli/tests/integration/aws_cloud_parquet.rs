use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use floe_core::io::storage::s3::S3Client;
use floe_core::io::storage::StorageClient;
use floe_core::FloeResult;

fn should_run() -> bool {
    matches!(env::var("FLOE_IT_AWS").as_deref(), Ok("1"))
}

fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("lakehouse/it/aws-{nanos}")
}

fn write_config(path: &PathBuf, bucket: &str, region: &str, prefix: &str) -> FloeResult<()> {
    let config = format!(
        r#"
version: "0.1"

storages:
  default: "s3_it"
  definitions:
    - name: "s3_it"
      type: "s3"
      bucket: "{bucket}"
      region: "{region}"
      prefix: "{prefix}"

report:
  path: "report"
  storage: "s3_it"

entities:
  - name: "orders"
    source:
      format: "parquet"
      path: "in/orders"
      storage: "s3_it"
    sink:
      accepted:
        format: "parquet"
        path: "out/accepted/orders"
        storage: "s3_it"
      rejected:
        format: "csv"
        path: "out/rejected/orders"
        storage: "s3_it"
      archive:
        path: "archive"
        storage: "s3_it"
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

struct S3Cleanup {
    bucket: String,
    region: String,
    prefix: String,
}

impl Drop for S3Cleanup {
    fn drop(&mut self) {
        if let Ok(client) = S3Client::new(self.bucket.clone(), Some(self.region.as_str())) {
            if let Ok(objects) = client.list(&self.prefix) {
                for object in objects {
                    let _ = client.delete_object(&object.uri);
                }
            }
        }
    }
}

#[test]
fn it_s3_parquet_cloud_end_to_end() -> FloeResult<()> {
    if !should_run() {
        eprintln!("skipping aws integration test (set FLOE_IT_AWS=1)");
        return Ok(());
    }

    let bucket = "floe-test";
    let region = "eu-west-1";
    let prefix = unique_prefix();
    let _cleanup = S3Cleanup {
        bucket: bucket.to_string(),
        region: region.to_string(),
        prefix: prefix.clone(),
    };

    let client = S3Client::new(bucket.to_string(), Some(region))?;
    let local_parquet = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../example/out/accepted/orders/part-00000.parquet");
    let input_uri = format!("s3://{bucket}/{prefix}/in/orders/orders.parquet");
    client.upload_from_path(&local_parquet, &input_uri)?;

    let temp_dir = tempfile::TempDir::new()?;
    let config_path = temp_dir.path().join("it_config.yml");
    write_config(&config_path, bucket, region, &prefix)?;

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_floe"));
    cmd.arg("run").arg("-c").arg(&config_path);
    if let Ok(value) = env::var("AWS_SHARED_CREDENTIALS_FILE") {
        cmd.env("AWS_SHARED_CREDENTIALS_FILE", value);
    }
    if let Ok(value) = env::var("AWS_CONFIG_FILE") {
        cmd.env("AWS_CONFIG_FILE", value);
    }
    if env::var("AWS_REGION").is_err() {
        cmd.env("AWS_REGION", region);
    }
    let status = cmd.status()?;
    assert!(status.success(), "floe run failed for aws integration");

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
