use floe_core::io::storage::gcs::{format_gcs_uri, parse_gcs_uri};
use floe_core::FloeResult;

#[test]
fn parse_gcs_uri_extracts_bucket_and_key() -> FloeResult<()> {
    let location = parse_gcs_uri("gs://my-bucket/data/file.csv")?;
    assert_eq!(location.bucket, "my-bucket");
    assert_eq!(location.key, "data/file.csv");
    Ok(())
}

#[test]
fn format_gcs_uri_handles_empty_key() {
    assert_eq!(format_gcs_uri("my-bucket", ""), "gs://my-bucket");
}
