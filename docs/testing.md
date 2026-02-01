# Testing

## Cloud integration tests (gated)

Cloud integration tests live under `crates/floe-cli/tests/integration/` and are **not** run by
default. They are gated by env flags:

- AWS S3: `FLOE_IT_AWS=1`
- GCP GCS: `FLOE_IT_GCP=1`

These tests:
- upload a small parquet fixture to the bucket under a temporary prefix
- run `floe run` with a local config pointing at cloud inputs/outputs
- verify accepted output, report JSON, and archive presence
- cleanup by deleting all objects under the test prefix

### AWS (S3)

Prereqs:
- AWS credentials available via the standard chain
- Bucket: `floe-test` (region `eu-west-1`)

Example:

```bash
FLOE_IT_AWS=1 \
AWS_SHARED_CREDENTIALS_FILE=/aws/credentials \
AWS_CONFIG_FILE=/aws/config \
AWS_REGION=eu-west-1 \
cargo test -p floe-cli --test integration it_s3_parquet_cloud_end_to_end
```

### GCP (GCS)

Prereqs:
- `GOOGLE_APPLICATION_CREDENTIALS` points to a service account JSON
- Bucket: `floe-test` (region `europe-west1`)

Example:

```bash
FLOE_IT_GCP=1 \
GOOGLE_APPLICATION_CREDENTIALS=/gcp/service-account.json \
cargo test -p floe-cli --test integration it_gcs_parquet_cloud_end_to_end
```

## Notes

- These tests are integration-level and should only be run when cloud credentials are available.
- Cleanup is performed at the end of each test by deleting all objects under the test prefix.
