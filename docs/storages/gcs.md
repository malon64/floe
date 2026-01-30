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

- IO uses temp download/upload (no streaming).
- Only file-based formats (csv/parquet/json) are supported.

GCS auth will follow the standard Google ADC (Application Default Credentials) chain.
This will be documented once list/download/upload are implemented.
