# Floe Support Matrix (Current)

This matrix reflects **current behavior** in the codebase (not aspirational).
Cloud storage uses temp download/upload for file IO, except Delta and S3/GCS Iceberg
accepted sinks which use direct object_store-backed transactions/writes.

## Inputs

| Format | Local | S3 | ADLS | GCS | Notes |
|---|---|---|---|---|---|
| CSV | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Suffix filter `.csv` |
| TSV | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Suffix filter `.tsv` |
| Fixed-width | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Suffix filter `.txt`, `.fw` |
| JSON (array) | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | `source.options.json_mode=array` (default) |
| JSON (ndjson) | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | `source.options.json_mode=ndjson` |
| ORC | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Suffix filter `.orc` |
| Parquet | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Temp download then read locally |
| XLSX | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | `source.options.sheet` (name), `header_row`/`data_row` (1-based) |
| Avro | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Suffix filter `.avro` |
| XML | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | `source.options.row_tag` required; selectors in `schema.columns[].source` |

Notes:
- Cloud inputs are resolved by **prefix listing** + suffix filtering, then downloaded to temp files.
- Globs and recursive options apply to local inputs only.
- Fixed-width file extensions accepted: `.txt`, `.fw` (case-insensitive).
- TSV file extensions accepted: `.tsv` (case-insensitive).
- JSON file extensions accepted: `.json`, `.jsonl`, `.ndjson`, `.djson` (case-insensitive).
- XLSX file extensions accepted: `.xlsx` (case-insensitive).
- Avro file extensions accepted: `.avro` (case-insensitive).
- XML file extensions accepted: `.xml` (case-insensitive).
- Nested JSON values (objects/arrays) are rejected.

## Outputs

| Output | Local | S3 | ADLS | GCS | Notes |
|---|---|---|---|---|---|
| Accepted: Parquet | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Writes `part-*.parquet` (overwrite: sequential parts, append: UUID parts) |
| Accepted: Delta | ✅ | ✅ (object_store) | ✅ (object_store) | ✅ (object_store) | Transactional `_delta_log` |
| Accepted: Iceberg | ✅ | ✅ (filesystem catalog or Glue catalog over object_store) | ❌ | ✅ (filesystem catalog over object_store) | `metadata/` + `data/`; append/overwrite; partition spec runtime supported; no schema evolution/GC |
| Rejected: CSV | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Dataset parts `part-*.csv` |
| Reports: JSON | ✅ | ✅ (temp) | ✅ (temp) | ✅ (temp) | Uploaded via temp file |

Notes:
- Parquet outputs to cloud are written locally then uploaded.
- Delta outputs to cloud are **direct** via object_store (no temp upload).
- Delta `partition_by` is runtime-supported on local/S3/ADLS/GCS accepted sinks.
- Delta accepted-output report metrics use committed Delta log `add` actions; remote metrics are best-effort and remain nullable on post-write collection failure.
- Iceberg on S3 supports filesystem-catalog semantics and AWS Glue catalog registration (S3 data location).
- Iceberg `partition_spec` is runtime-supported (validated subset: `identity`, `year`, `month`, `day`, `hour`).
- Iceberg accepted-output reports include snapshot/version metadata and file sizing metrics for data files.
- Iceberg on GCS uses filesystem-catalog semantics (no external catalog yet).
- Iceberg cloud support is currently S3 and GCS only (ADLS is follow-up work).
- `sink.write_mode` applies to accepted and rejected outputs (`overwrite`, `append`, or `merge_scd1`).
- `merge_scd1` is accepted-Delta only; non-Delta accepted sinks fail validation.

## Accepted-output report metadata (current)

- `accepted_output` in each entity report can include:
  - `files_written`, `parts_written`, `part_files`
  - `table_version` (Delta version or Iceberg metadata version, format-dependent)
  - `snapshot_id` (Iceberg)
  - `total_bytes_written`, `avg_file_size_mb`, `small_files_count`
  - Iceberg catalog identifiers when Glue mode is configured (`iceberg_catalog_name`, `iceberg_database`, `iceberg_namespace`, `iceberg_table`)

Operational boundary:
- Floe writes tables and reports write-time metrics, but does not run downstream
  table optimization/maintenance (Delta optimize/vacuum, Iceberg compaction/maintenance).

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
