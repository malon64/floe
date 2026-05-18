# Floe — Project Context

## What it is

Floe is a single-node, YAML-driven data ingestion framework written in Rust. You describe a data contract in a config file; Floe reads raw files, enforces schema and quality rules, and writes clean rows to a sink — routing invalid rows to a separate rejected output.

It is designed for data platform teams who need reliable, auditable file ingestion without running a Spark or Flink cluster.

## Core value proposition

- **No JVM, no cluster.** A single static binary. Runs on a laptop, a Lambda function, or a Kubernetes job.
- **Declarative contracts.** Schema, quality rules, and routing are expressed in YAML — not code.
- **Audit trail by default.** Every run produces a JSON report with row counts, rejected file paths, schema evolution info, and timing.
- **Cloud-native storage.** S3, GCS, ADLS, and local — all treated uniformly.

## What it is NOT

- Not a streaming engine (no Kafka, no continuous processing).
- Not a transformation framework (no SQL, no dbt-style models).
- Not distributed — one process, one machine. Concurrency is handled via the CAS-based incremental state system, not worker coordination.

## Target users

- **Data engineers** building file ingestion pipelines (CSV drops, API exports, partner feeds).
- **Platform teams** standardising ingestion contracts across data products.
- **ML teams** needing validated, typed parquet landing zones.

## Supported formats

| Direction | Formats |
|---|---|
| Input | CSV · TSV · JSON · Parquet · ORC · Avro · XLSX · XML · Fixed-width |
| Accepted output | Parquet · Delta Lake · Apache Iceberg |
| Rejected output | CSV · Parquet |

## Supported storage

Local · AWS S3 · Azure ADLS · Google Cloud Storage

## Supported catalogs

AWS Glue · Iceberg REST (Polaris, Nessie, Snowflake) · Databricks Unity Catalog

## Versioning

The project follows semantic versioning. The config file schema is versioned independently (`version: "0.1"` in YAML). Breaking config changes bump the minor version and are documented in `CHANGELOG.md`.
