# Iceberg sink (accepted output)

Floe writes Iceberg tables for `sink.accepted.format: iceberg` using filesystem
catalog layout semantics (table root with `metadata/` and `data/`) on local
storage and S3.

## Storage catalog model

- Table identity: `sink.accepted.path` (table root directory).
- Metadata lives under `<path>/metadata/` (storage catalog layout).
- Supported storage in this phase: local filesystem and S3 (filesystem catalog semantics).
- No external catalog integration in v0.2.

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

## Partition spec config (Phase A scaffolding)

Iceberg accepted sinks can now declare a validated partition spec in config
(execution wiring is pending):

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

Current validation scope:
- partition columns must exist in `schema.columns`
- supported transforms (parse/validation phase): `identity`, `year`, `month`, `day`, `hour`
- execution wiring for Iceberg partitioned writes remains a follow-up PR
- compaction/optimization remains external to Floe

## Supported scope (current)

- Accepted sink only (`sink.accepted.format: iceberg`)
- Local or S3 storage (`sink.accepted.storage` must resolve to `local` or `s3`)
- `sink.write_mode`: `overwrite` and `append`
- Scalar column types only (for example: string, bool, signed ints, floats, date/time/datetime)
- Append creates a new Iceberg snapshot/metadata version
- Overwrite replaces logical table contents by recreating table metadata at the same root (no cleanup/GC)
- S3 writes use direct Iceberg/object_store `FileIO` (filesystem catalog layout, no Glue)
- Run report includes sink format, table root URI, write mode, files written, and snapshot id (when present)

## Limitations / non-goals (v0.2)

- No schema evolution
- No merge/upsert
- No external catalog integration (AWS Glue is follow-up work)
- No cleanup/garbage collection of orphaned files/metadata
- No GCS/ADLS Iceberg sink yet (follow-up work)

## Manual S3 integration test

- A gated test exists in `crates/floe-core/tests/integration/iceberg_s3_run.rs`.
- It is skipped by default. To run it:
  - set AWS credentials in your environment
  - set `FLOE_RUN_MANUAL_S3_ICEBERG_TEST=1`
  - optionally set `FLOE_TEST_S3_BUCKET` (default: `floe-test`)
  - optionally set `FLOE_TEST_S3_REGION` (falls back to `AWS_REGION`, then `us-east-1`)
  - optionally set `FLOE_TEST_S3_PREFIX` for a custom cleanup prefix

## Empty accepted dataset behavior

- Overwrite/create with an empty accepted dataframe creates the table metadata layout
  (no data files, no snapshot yet).
- Append with an empty accepted dataframe is a no-op for table contents.
