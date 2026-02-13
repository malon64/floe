# Floe + Airflow (MVP example)

This folder contains an Airflow-oriented integration spec and a minimal runnable DAG example.

For local setup of both Dagster and Airflow with isolated virtual environments, see:
- `orchestrators/LOCAL_DEV.md`

## Contents

- `INTEGRATION_SPEC.md`: contract between Airflow and Floe CLI
- `schemas/`: JSON Schemas for XCom payloads and manifest contract
- `dags/floe_manifest.py`: manifest loader/converter (`floe.plan.v1` -> `floe.airflow.manifest.v1`)
- `example/config.yml`: small Floe config for demo
- `dags/floe_example_simple_dag.py`: default DAG that runs the full config once
- `dags/floe_example_entity_mapped_dag.py`: advanced DAG that maps one run task per entity

## Quick usage

1. Make sure `floe` is available in PATH (or set `FLOE_CMD`).
2. Point Airflow DAGs folder to `orchestrators/airflow-floe/dags`.
3. Set optional env vars:

```bash
export FLOE_CMD="floe"
export FLOE_MANIFEST="/absolute/path/to/orchestrators/airflow-floe/example/manifest.airflow.json"
# optional override:
# export FLOE_CONFIG="/absolute/path/to/orchestrators/airflow-floe/example/config.yml"
```

4. Trigger DAG `floe_example_simple`.

## Notes

- The DAG uses Floe CLI contracts directly and expects:
  - run log schema: `floe.log.v1`
  - terminal event: `run_finished`
- Assets are created at parse time from `FLOE_MANIFEST` and materialized when run tasks finish.
- The returned task payload shape follows `floe.airflow.run.v1`.
- Use the simple DAG as default architecture. Use entity-mapped only when you need per-entity retries/concurrency.
- Next architecture step: generate a static Airflow manifest from Floe (see `INTEGRATION_SPEC.md`, section "Manifest command"), then load DAG definitions from that manifest at import time.
