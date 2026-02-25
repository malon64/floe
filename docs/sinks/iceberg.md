# Iceberg sink (accepted output)

Floe writes Iceberg tables for `sink.accepted.format: iceberg` using filesystem
catalog layout semantics (table root with `metadata/` and `data/`) on local
storage, S3, and GCS. Floe also supports AWS Glue catalog registration for
S3-backed Iceberg tables.

## Storage catalog model

- Table identity: `sink.accepted.path` (table root directory).
- Metadata lives under `<path>/metadata/` (storage catalog layout).
- Supported storage in this phase:
  - local filesystem (filesystem catalog semantics)
  - S3 (filesystem catalog semantics or AWS Glue catalog)
  - GCS (filesystem catalog semantics)

## Example config

```yaml
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      accepted:
        format: "iceberg"
        path: "/data/out/customer_iceberg"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
```

## AWS Glue catalog on S3 (optional)

```yaml
storages:
  default: "local_fs"
  definitions:
    - name: "local_fs"
      type: "local"
    - name: "s3_out"
      type: "s3"
      bucket: "my-bucket"
      region: "us-east-1"

catalogs:
  default: "glue_main"
  definitions:
    - name: "glue_main"
      type: "glue"
      region: "us-east-1"
      database: "lakehouse"
      warehouse_storage: "s3_out"
      warehouse_prefix: "warehouse/iceberg"

entities:
  - name: "customer"
    domain: "sales"
    source:
      format: "csv"
      path: "/data/in/customers.csv"
    sink:
      write_mode: "append"
      accepted:
        format: "iceberg"
        path: "unused/customer_iceberg" # data location is derived from the catalog warehouse
        storage: "s3_out"
        iceberg:
          catalog: "glue_main"
          table: "customer"
    policy:
      severity: "warn"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
```

## Partition spec config

Iceberg accepted sinks can declare a partition spec in config, and Floe executes
it at write time for Iceberg table creation/data file layout:

```yaml
sink:
  accepted:
    format: "iceberg"
    path: "/data/out/customer_iceberg"
    partition_spec:
      - column: "event_date"
        transform: "day"
      - column: "country"
        transform: "identity"
```

Current scope:
- partition columns must exist in `schema.columns`
- supported transforms: `identity`, `year`, `month`, `day`, `hour`
- partition spec is applied to Iceberg table metadata and partitioned file writes
- compaction/optimization remains external to Floe

## Supported scope (current)

- Accepted sink only (`sink.accepted.format: iceberg`)
- Local, S3, or GCS storage (`sink.accepted.storage` must resolve to `local`, `s3`, or `gcs`)
- `sink.write_mode`: `overwrite` and `append`
- Scalar column types only (for example: string, bool, signed ints, floats, date/time/datetime)
- Append creates a new Iceberg snapshot/metadata version
- Overwrite replaces logical table contents by recreating table metadata at the same root (no cleanup/GC)
- S3/GCS writes use direct Iceberg/object_store `FileIO`
- S3 supports optional AWS Glue catalog registration (`sink.accepted.iceberg.catalog`)
- Glue mode uses S3 for actual table data/metadata location
- Run report includes sink format, table root URI, write mode, files written, snapshot id (when present), and Iceberg catalog identity when configured

## Limitations / non-goals (v0.2)

- No schema evolution
- No merge/upsert
- No cleanup/garbage collection of orphaned files/metadata
- No ADLS Iceberg sink yet (follow-up work)

## Manual S3 integration test

- A gated test exists in `crates/floe-core/tests/integration/iceberg_s3_run.rs`.
- It is skipped by default. To run it:
  - set AWS credentials in your environment
  - set `FLOE_RUN_MANUAL_S3_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_S3_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_S3_REGION` (falls back to `AWS_REGION`, then `us-east-1`)
  - optionally set `FLOE_TEST_S3_PREFIX` for a custom cleanup prefix

## Manual GCS integration test

- A gated test exists in `crates/floe-core/tests/integration/iceberg_gcs_run.rs`.
- It is skipped by default. To run it:
  - set Google Cloud credentials for ADC (Application Default Credentials)
  - set `FLOE_RUN_MANUAL_GCS_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_GCS_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_GCS_PREFIX` for a custom cleanup prefix

## Manual AWS Glue integration test (S3-backed)

- A gated test exists in `crates/floe-core/tests/integration/iceberg_glue_run.rs`.
- It is skipped by default. To run it:
  - set AWS credentials with S3 + Glue permissions
  - ensure the target Glue database already exists (default: `floe_test`)
  - set `FLOE_RUN_MANUAL_GLUE_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_S3_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_GLUE_REGION` or `FLOE_TEST_S3_REGION` (falls back to `AWS_REGION`)
  - optionally set `FLOE_TEST_GLUE_DATABASE`
  - optionally set `FLOE_TEST_GLUE_PREFIX` for a custom cleanup prefix

## Empty accepted dataset behavior

- Overwrite/create with an empty accepted dataframe creates the table metadata layout
  (no data files, no snapshot yet).
- Append with an empty accepted dataframe is a no-op for table contents.
