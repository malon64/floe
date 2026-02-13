# Floe + Airflow (MVP example)

This folder contains:
- a reusable Python connector package (`src/airflow_floe`)
- example DAGs (`dags/`) that consume the package

For local setup of both Dagster and Airflow with isolated virtual environments, see:
- `orchestrators/LOCAL_DEV.md`

## Contents

- `INTEGRATION_SPEC.md`: contract between Airflow and Floe CLI
- `schemas/`: JSON Schemas for XCom payloads and manifest contract
- `src/airflow_floe/manifest.py`: manifest loader/converter (`floe.plan.v1` -> `floe.airflow.manifest.v1`)
- `src/airflow_floe/runtime.py`: shared runtime helpers (manifest context, run event parsing, summary loading)
- `src/airflow_floe/hooks.py`: reusable manifest hook (`FloeManifestHook`)
- `src/airflow_floe/operators.py`: reusable run hook/operator (`FloeRunHook`, `FloeRunOperator`)
- `example/config.yml`: small Floe config for demo
- `dags/floe_example_simple_dag.py`: default DAG that runs the full config once
- `dags/floe_example_entity_mapped_dag.py`: advanced DAG that maps one run task per entity
- `dags/floe_example_operator_dag.py`: simple DAG using `FloeRunOperator`

## Quick usage

1. Make sure `floe` is available in PATH (or set `FLOE_CMD`).
2. (Optional but recommended) install the connector package:

```bash
python -m pip install -e orchestrators/airflow-floe
```
3. Point Airflow DAGs folder to `orchestrators/airflow-floe/dags`.
4. Set optional env vars:

```bash
export FLOE_CMD="floe"
export FLOE_MANIFEST="/absolute/path/to/orchestrators/airflow-floe/example/manifest.airflow.json"
# optional override:
# export FLOE_CONFIG="/absolute/path/to/orchestrators/airflow-floe/example/config.yml"
```

5. Generate manifest from Floe config:

```bash
floe manifest generate \
  -c orchestrators/airflow-floe/example/config.yml \
  --target airflow \
  --output orchestrators/airflow-floe/example/manifest.airflow.json
```

6. Trigger DAG `floe_example_simple`.
   - Alternative operator-first demo: `floe_example_operator`

## Notes

- The DAG uses Floe CLI contracts directly and expects:
  - run log schema: `floe.log.v1`
  - terminal event: `run_finished`
- Assets are created at parse time from `FLOE_MANIFEST` and materialized when run tasks finish.
- `floe_example_operator` also publishes asset events when manifest assets are available.
- If `FLOE_MANIFEST` is missing/invalid, DAGs still run with `FLOE_CONFIG` (or example config fallback) but no assets are loaded/materialized.
- The returned task payload shape follows `floe.airflow.run.v1`.
- Use the simple DAG as default architecture. Use entity-mapped only when you need per-entity retries/concurrency.
- Local run summaries emitted as `local://...` are resolved and loaded by the connector runtime helpers.
