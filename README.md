![Floe logo](docs/assets/floe-banner.png)

# Floe

**Floe** is a single-node, YAML-driven data ingestion framework written in Rust. You describe your data contract in a config file; Floe reads raw files, enforces your schema and quality rules, and writes clean rows to your sink — routing invalid rows to a separate rejected output.

## How it works

![Floe architecture](docs/assets/architecture.png)

Each `floe run` executes a four-stage pipeline per entity:

| Stage | What happens |
|---|---|
| **1. Resolve inputs** | Discover and download source files from local or cloud storage |
| **2. File-level checks** | Validate schema structure, file format, and headers |
| **3. Row-level checks** | Apply type casting and `not_null` checks row by row |
| **4. Entity-level checks** | Apply `unique` / primary-key checks across all input rows plus existing accepted data |

Rows that pass all checks go to the **accepted** sink. Rows that fail go to the **rejected** sink. A JSON run report is written after every run.

**Inputs:** CSV · TSV · JSON · Parquet · ORC · Avro · XLSX · XML · Fixed-width  
**Accepted outputs:** Parquet · Delta Lake · Apache Iceberg  
**Storage:** local · S3 · ADLS · GCS  
**Catalogs:** AWS Glue · Iceberg REST (Polaris, Nessie, Snowflake) · Databricks Unity Catalog

## Install

**macOS / Linux — [Homebrew](https://brew.sh)**

```bash
brew tap malon64/floe
brew install floe
```

**Windows — [Scoop](https://scoop.sh)**

```bash
scoop bucket add floe https://github.com/malon64/scoop-floe
scoop install floe
```

**Docker**

```bash
docker pull ghcr.io/malon64/floe:latest
docker run --rm -v "$PWD:/work" ghcr.io/malon64/floe:latest run -c /work/config.yml
```

Or download a prebuilt binary from [GitHub Releases](https://github.com/malon64/floe/releases), or `cargo install floe-cli`.  
→ [Full installation guide](docs/installation.md)

## Quick start

```bash
floe validate -c config.yml   # validate config and schema
floe run      -c config.yml   # run the pipeline
```

→ [Config reference](docs/config.md) · [Example config](example/config.yml)

## Documentation

| Topic | Doc |
|---|---|
| Configuration reference | [docs/config.md](docs/config.md) |
| How it works (deep dive) | [docs/how-it-works.md](docs/how-it-works.md) |
| Checks (not_null, unique, cast) | [docs/checks.md](docs/checks.md) |
| Delta sink + Unity Catalog | [docs/sinks/delta.md](docs/sinks/delta.md) |
| Iceberg sink + Glue / REST | [docs/sinks/iceberg.md](docs/sinks/iceberg.md) |
| Sink options | [docs/sinks/options.md](docs/sinks/options.md) |
| Write modes | [docs/write_modes.md](docs/write_modes.md) |
| S3 storage | [docs/storages/s3.md](docs/storages/s3.md) |
| ADLS storage | [docs/storages/adls.md](docs/storages/adls.md) |
| GCS storage | [docs/storages/gcs.md](docs/storages/gcs.md) |
| Environement specific profile config | [docs/profiles.md](docs/profiles.md) [docs/variables.md](docs/variables.md) |
| Run reports | [docs/report.md](docs/report.md) |
| CLI reference | [docs/cli.md](docs/cli.md) |
| Orchestration (Dagster / Airflow) | [docs/summary.md](docs/summary.md) |
| Support matrix | [docs/support-matrix.md](docs/support-matrix.md) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |

## License

MIT
