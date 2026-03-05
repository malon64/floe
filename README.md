![Floe logo](docs/assets/floe-banner.png)

# Floe

Technical ingestion on a single node, driven by YAML contracts.

Floe is a Rust + Polars tool for technical ingestion on a single node. It
ingests raw files into typed datasets using YAML contracts, applies schema
enforcement and data quality rules, and produces clear, auditable outputs.

Start here: [docs/summary.md](docs/summary.md)

## What Floe solves

- Schema enforcement and type casting (`strict` vs `coerce`)
- Nullability checks (`not_null`)
- Uniqueness checks (`unique`)
- Policy behavior: `warn` / `reject` / `abort`
- Accepted vs rejected outputs for clean separation
- JSON run reports for observability and audit

## Why Polars + Rust

- Polars provides fast, columnar execution on a single node without JVM overhead.
- Rust gives predictable performance and low-level control while keeping memory usage tight.
- The combo fits contract-driven ingestion: schema checks, deterministic outputs, and stable reports.

## Minimal config example

```yaml
version: "0.1"
report:
  path: "./reports"
entities:
  - name: "customer"
    source:
      format: "csv"
      path: "./example/in/customer"
    sink:
      accepted:
        format: "parquet"
        path: "./example/out/accepted/customer"
      rejected:
        format: "csv"
        path: "./example/out/rejected/customer"
    policy:
      severity: "reject"
    schema:
      columns:
        - name: "customer_id"
          type: "string"
          nullable: false
          unique: true
        - name: "created_at"
          type: "datetime"
          nullable: true
```

Full example: [example/config.yml](example/config.yml)

Config reference: [docs/config.md](docs/config.md)
Support matrix: [docs/support-matrix.md](docs/support-matrix.md)

## Quickstart (Homebrew)

### Install

```bash
brew tap malon64/floe
brew install floe
floe --version
```

### Validate

```bash
floe validate -c example/config.yml
```

Automation / orchestrators (single JSON object on stdout):

```bash
floe validate -c example/config.yml --output json
```

### Run

```bash
floe run -c example/config.yml
```

### Troubleshooting

If Homebrew is unavailable:

- GitHub Releases: download the prebuilt binary from the latest release
- Cargo: `cargo install floe-cli`

More CLI details: [docs/cli.md](docs/cli.md)
Full installation guide: [docs/installation.md](docs/installation.md)

## Local development

Prerequisites:
- Rust stable toolchain (`rustup toolchain install stable`)
- A C/C++ toolchain available in `PATH`

Build and run from this repo:

```bash
cargo build --all
cargo run -p floe-cli -- --help
```

Run checks before opening a PR:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

Run tests:

```bash
cargo test -p floe-core --test unit
cargo test -p floe-core --test integration
cargo test -p floe-cli --tests
```

For orchestrators (Dagster + Airflow) local setup, see [orchestrators/LOCAL_DEV.md](orchestrators/LOCAL_DEV.md).

## Run with Docker

### Pull

```bash
docker pull ghcr.io/malon64/floe:latest
```

### Run (mount local folder)

Run Floe against the repo example config by mounting the current directory to `/work`:

```bash
docker run --rm -v "$PWD:/work" ghcr.io/malon64/floe:latest run -c /work/example/config.yml
```

Notes:
- All CLI arguments are identical to local usage.
- Cloud credentials are passed via environment variables (or runtime identity), not baked into the image.

## Sample console output

```text
run id: run-123
report base: ./reports
==> entity customer (severity=reject, format=csv)
  REJECTED customers.csv rows=10 accepted=8 rejected=2 elapsed_ms=12 accepted_out=customer rejected_out=customers_rejected.csv
Totals: files=1 rows=10 accepted=8 rejected=2
Overall: rejected (exit_code=0)
Run summary: ./reports/run_run-123/run.summary.json
```

## Outputs explained

- Accepted output: `entities[].sink.accepted.path`
- Rejected output: `entities[].sink.rejected.path`
- Reports: `<report.path>/run_<run_id>/<entity.name>/run.json`

Reports include per-entity JSON, a run summary, and key counters (rows, accepted/rejected, errors).

Report details: [docs/report.md](docs/report.md)

## Severity policy

- `warn`: keep all rows and report violations
- `reject`: reject only rows with violations; keep valid rows
- `abort`: reject the entire file on first violation

Checks and policy details: [docs/checks.md](docs/checks.md)

## Supported formats

Inputs:
- CSV (local + S3/ADLS/GCS via temp download)
- Fixed-width (local + S3/ADLS/GCS via temp download)
- TSV (tab-delimited; local + S3/ADLS/GCS via temp download)
- JSON (array/ndjson; local + S3/ADLS/GCS via temp download)
- ORC (local + S3/ADLS/GCS via temp download)
- Parquet (local + S3/ADLS/GCS via temp download)
- XLSX (local + S3/ADLS/GCS via temp download)
- Avro (local + S3/ADLS/GCS via temp download)
- XML (local + S3/ADLS/GCS via temp download)

Outputs:
- Accepted: Parquet (local + cloud via temp upload), Delta (local + cloud via object_store)
- Rejected: CSV (local + cloud via temp upload)
- Reports: JSON (local + cloud via temp upload)

Sink details:
- Options: [docs/sinks/options.md](docs/sinks/options.md)
- Delta: [docs/sinks/delta.md](docs/sinks/delta.md)
- Iceberg: [docs/sinks/iceberg.md](docs/sinks/iceberg.md)
- Support matrix: [docs/support-matrix.md](docs/support-matrix.md)

## Cloud integration and storages

Floe resolves all paths through a storage registry in the config. By default,
paths use `local://`. To use cloud storage, define a storage (with credentials
or bucket info) and reference it on `source`/`sink`. S3, ADLS, and GCS are
implemented; `dbfs://` (Databricks) is on the roadmap.

Example (S3 storage):

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
      # credentials via standard AWS env vars or profile
entities:
  - name: customer
    source:
      storage: s3_raw
      path: raw/customer/
```

Storage guides:
- [docs/storages/s3.md](docs/storages/s3.md)
- [docs/storages/adls.md](docs/storages/adls.md)
- [docs/storages/gcs.md](docs/storages/gcs.md)

## More docs

- How it works: [docs/how-it-works.md](docs/how-it-works.md)
- Checks: [docs/checks.md](docs/checks.md)
- Reports: [docs/report.md](docs/report.md)

## License

MIT
