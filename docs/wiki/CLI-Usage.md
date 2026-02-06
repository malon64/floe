# CLI Usage

## Commands

- `floe validate -c <config> [--entities <name[,name...]>] [--output text|json]`
  - Validates YAML config.
  - `--output json` prints a single JSON object to stdout.

- `floe run -c <config> [--run-id <id>] [--entities <name[,name...]>]`
  - Executes ingestion.
  - Reports are written under `<report.path>/run_<run_id>/...`.
  - `-c` accepts local paths or cloud URIs (`s3://`, `gs://`, `abfs://`).

## Examples

Validate a config:

```bash
floe validate -c example/config.yml
floe validate -c example/config.yml --entities customer
floe validate -c example/config.yml --output json
```

Run a single entity:

```bash
floe run -c example/config.yml --entities customer
```

Run with a custom run id:

```bash
floe run -c example/config.yml --entities customer --run-id dev-001
```

## Where outputs go

- Reports: `report/run_<run_id>/run.summary.json`
- Per-entity report: `report/run_<run_id>/<entity.name>/run.json`

See `docs/report.md` for details.
