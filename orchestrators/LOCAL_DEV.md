# Local Orchestrators Setup (Dagster + Airflow)

This guide sets up two isolated Python virtual environments from the repo root:
- `.venv-dagster` for `orchestrators/dagster-floe`
- `.venv-airflow` for `orchestrators/airflow-floe`

Recommended working directory for all commands in this guide:
- repo root (`floe/`)

Using repo root keeps relative paths stable for:
- `orchestrators/*/example/config.yml`
- Airflow DAG folder path
- Floe input/output paths in examples

## Prerequisites

- Python 3.12 installed (recommended for Airflow stability)
- Floe CLI available (`floe --version`) or use `cargo run -p floe-cli --`

## 1) Dagster environment

From `floe/`:

```bash
python3.12 -m venv .venv-dagster
source .venv-dagster/bin/activate
pip install --upgrade pip
pip install -e orchestrators/dagster-floe[dev]
```

`uv` variant:

```bash
uv venv .venv-dagster --python 3.12
source .venv-dagster/bin/activate
uv pip install -e orchestrators/dagster-floe[dev]
```

Run Dagster example:

```bash
cd orchestrators/dagster-floe
dagster dev
```

Open Dagster UI at `http://127.0.0.1:3000`.

Deactivate when done:

```bash
deactivate
```

## 2) Airflow environment

From `floe/`:

```bash
python3.12 -m venv .venv-airflow
source .venv-airflow/bin/activate
pip install --upgrade pip
pip install "apache-airflow==3.1.7"
```

`uv` variant:

```bash
uv venv .venv-airflow --python 3.12
source .venv-airflow/bin/activate
uv pip install "apache-airflow==3.1.7"
```

Set runtime environment variables (still from `floe/`):

```bash
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/orchestrators/airflow-floe/dags"
export FLOE_CMD="floe"
export FLOE_MANIFEST="$PWD/orchestrators/airflow-floe/example/manifest.airflow.json"
# optional override:
# export FLOE_CONFIG="$PWD/orchestrators/airflow-floe/example/config.yml"
```

Start Airflow:

```bash
airflow standalone
```

Open Airflow UI at `http://127.0.0.1:8080`.

- Username: `admin`
- Password: generated in terminal (or in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`)

Trigger DAG:
- `floe_example_simple` (default config-level run)
- `floe_example_entity_mapped` (advanced per-entity mapped runs)

After at least one successful run, check Airflow Assets UI:
- assets are declared from `FLOE_MANIFEST` at parse time
- materialization metadata is published when run tasks finish

Deactivate when done:

```bash
deactivate
```

## 3) Optional: use local Cargo build instead of installed `floe`

In Airflow env:

```bash
export FLOE_CMD="cargo run -p floe-cli --"
```

## 4) Troubleshooting

- Task stuck in `queued`:
  - verify scheduler is alive:

```bash
airflow jobs check --job-type SchedulerJob --allow-multiple --limit 5
```

- If scheduler crashes on Python 3.14:
  - use Python 3.12 for `.venv-airflow`.

- DAGs not visible:
  - confirm `AIRFLOW__CORE__DAGS_FOLDER` points to `orchestrators/airflow-floe/dags`
  - unpause DAGs if needed:

```bash
airflow dags unpause floe_example_simple
airflow dags unpause floe_example_entity_mapped
```
