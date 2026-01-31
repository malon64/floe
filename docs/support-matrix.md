# Floe v0.2 Support Matrix

This matrix reflects **current behavior** in the codebase (not aspirational).
Cloud storage uses temp download/upload for file IO, while Delta uses direct
object_store transactions.

## Inputs

| Format | Local | S3 | ADLS | GCS | Notes |
|---|---|---|---|---|---|
| CSV | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Suffix filter `.csv` |
| JSON (array) | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | `source.options.json_mode=array` (default) |
| JSON (ndjson) | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | `source.options.json_mode=ndjson` |
| Parquet | ✅ | ❌ | ❌ | ❌ | Input parquet is local-only |

Notes:
- Cloud inputs are resolved by **prefix listing** + suffix filtering, then downloaded to temp files.
- Globs and recursive options apply to local inputs only.
- JSON file extensions accepted: `.json`, `.jsonl`, `.ndjson`, `.djson` (case-insensitive).
- Nested JSON values (objects/arrays) are rejected.

## Outputs

| Output | Local | S3 | ADLS | GCS | Notes |
|---|---|---|---|---|---|
| Accepted: Parquet | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Writes `part-00000.parquet` etc. |
| Accepted: Delta | ✅ | ✅ (object_store) | ✅ (object_store) | ✅ (object_store) | Transactional `_delta_log` |
| Rejected: CSV | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Per-file rejected output |
| Reports: JSON | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Uploaded via temp file |

Notes:
- Parquet outputs to cloud are written locally then uploaded.
- Delta outputs to cloud are **direct** via object_store (no temp upload).

## Cloud storage behavior

- Storage paths resolve to canonical URIs:
  - S3: `s3://<bucket>/<prefix>/<path>`
  - ADLS: `abfs://<container>@<account>.dfs.core.windows.net/<prefix>/<path>`
  - GCS: `gs://<bucket>/<prefix>/<path>`
- Prefix listing is stable and lexicographically sorted.
- No glob support for S3/ADLS/GCS inputs (prefix only).

## Config keys (storage)

```yaml
storages:
  default: local
  definitions:
    - name: local
      type: local
    - name: s3_raw
      type: s3
      bucket: my-bucket
      region: eu-west-1
      prefix: data
    - name: adls_raw
      type: adls
      account: myaccount
      container: raw
      prefix: ingest
    - name: gcs_raw
      type: gcs
      bucket: my-bucket
      prefix: data
```

Storage references:
- `entities[].source.storage`
- `entities[].sink.accepted.storage`
- `entities[].sink.rejected.storage`
