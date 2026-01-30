# GCS storage (planned)

GCS is a first-class storage definition in the config, but the client is **not implemented yet**.
You can use it today for validation and URI resolution in reports; actual IO will be added in a later phase.

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

## Auth (planned)

GCS auth will follow the standard Google ADC (Application Default Credentials) chain.
This will be documented once list/download/upload are implemented.
