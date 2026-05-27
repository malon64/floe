# Floe + Airflow (MVP example)

This folder contains:

- a reusable Python connector package (`src/airflow_floe`)
- example DAGs (`dags/`) that consume the package

For local setup of both Dagster and Airflow with isolated virtual environments, see:

- `orchestrators/LOCAL_DEV.md`

## Contents

- `INTEGRATION_SPEC.md`: contract between Airflow and Floe CLI
- `schemas/`: JSON Schemas for XCom payloads and manifest contract
- `src/airflow_floe/manifest.py`: manifest loader (`floe.manifest.v1`)
- `src/airflow_floe/runtime.py`: shared runtime helpers (manifest context, run event parsing, summary loading)
- `src/airflow_floe/hooks.py`: reusable manifest hook (`FloeManifestHook`)
- `src/airflow_floe/operators.py`: reusable run hook/operator (`FloeRunHook`, `FloeRunOperator`)
- `example/config.yml`: small Floe config for demo
- `dags/floe_example_operator_dag.py`: example DAG using `FloeRunOperator`

## Configuration

The connector is driven entirely by environment variables. No code changes are needed to switch between single-manifest, multi-manifest, or fallback mode.

| Env var | What it does |
| --- | --- |
| `FLOE_MANIFEST` | Primary: path to a `floe.manifest.v1` JSON — enables asset registration at parse time |
| `FLOE_MANIFEST_DIR` | Multi-manifest mode: directory of `*.manifest.json` files; one DAG per file |
| `FLOE_CONFIG` | Fallback: path to a raw Floe YAML config — DAG runs but no assets are registered |
| `FLOE_CMD` | Override the `floe` binary path (default: `floe` on `$PATH`) |

**Fallback mode:** If `FLOE_MANIFEST` is not set, the connector falls back to `FLOE_CONFIG`. The DAG still runs and calls `floe run`, but because there is no manifest to read at parse time, no assets are registered and no asset events are emitted. Use this for quick local testing when you haven't generated a manifest yet.

**Multi-manifest mode:** Each JSON file in `FLOE_MANIFEST_DIR` becomes a separate DAG. The DAG ID is derived from the manifest file name — for example `orders.manifest.json` → DAG `floe_orders`. Entity tasks within the DAG are generated from the manifest's `entities[]` list.

## Quick usage

1. Make sure `floe` is available in PATH (or set `FLOE_CMD`).

2. (Optional but recommended) install the connector package:

   ```bash
   python -m pip install -e orchestrators/airflow-floe
   ```

3. Point Airflow DAGs folder to `orchestrators/airflow-floe/dags`.

4. Set env vars:

   ```bash
   export FLOE_CMD="floe"
   export FLOE_MANIFEST="/absolute/path/to/orchestrators/airflow-floe/example/manifest.airflow.json"
   # optional multi-manifest mode (1 manifest => 1 DAG):
   # export FLOE_MANIFEST_DIR="/absolute/path/to/orchestrators/airflow-floe/example/manifests"
   # optional fallback (no assets registered):
   # export FLOE_CONFIG="/absolute/path/to/orchestrators/airflow-floe/example/config.yml"
   ```

5. Generate manifest from Floe config:

   ```bash
   floe manifest generate \
     -c orchestrators/airflow-floe/example/config.yml \
     --output orchestrators/airflow-floe/example/manifest.airflow.json
   ```

   For multi-manifest mode (one DAG per manifest), generate domain manifests:

   ```bash
   floe manifest generate \
     -c orchestrators/airflow-floe/example/config.hr.yml \
     --output orchestrators/airflow-floe/example/manifests/hr.manifest.json

   floe manifest generate \
     -c orchestrators/airflow-floe/example/config.sales.yml \
     --output orchestrators/airflow-floe/example/manifests/sales.manifest.json
   ```

6. Trigger DAG `floe_example_operator`.
   In multi-manifest mode (`FLOE_MANIFEST_DIR`), DAGs are generated as `floe_<manifest_name>`.

## Notes

- The DAG uses Floe CLI contracts directly and expects:
  - run log schema: `floe.log.v1`
  - terminal event: `run_finished`
- Assets are created at parse time from `FLOE_MANIFEST` and materialized when run tasks finish.
- `floe_example_operator` also publishes asset events when manifest assets are available.
- The returned task payload shape follows `floe.airflow.run.v1`.
- Local run summaries emitted as `local://...` are resolved and loaded by the connector runtime helpers.
- Floe NDJSON stdout/stderr are streamed into task logs (Audit/Task Log view) and still parsed for `run_finished`.
