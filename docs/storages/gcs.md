# GCS storage (MVP)

GCS is supported for list/download/upload using the Google Application Default
Credentials chain. This MVP uses temp downloads/uploads (no streaming).

## Config

```yaml
storages:
  default: gcs_raw
  definitions:
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
      prefix: data/incoming
```

## Canonical URI

Floe resolves paths to canonical GCS URIs:

```
gs://<bucket>/<prefix>/<path>
```

Examples:

- `bucket=my-bucket`, `prefix=data`, `path=customers.csv` → `gs://my-bucket/data/customers.csv`
- `bucket=my-bucket`, no prefix, `path=customers.csv` → `gs://my-bucket/customers.csv`

## Auth

The client uses Application Default Credentials. For local development, set
`GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON file path:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

## Limitations

- IO uses temp download/upload (no streaming) for file formats.
- Source path is treated as a prefix (no glob patterns).
- Parquet input is local-only.
- Accepted Delta uses direct object_store writes (no temp upload).
