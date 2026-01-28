# Benchmarking Step-by-Step (Floe vs Pandas vs Spark)

This is a reproducible walkthrough to run the benchmark end-to-end on a single
machine. Run these steps from the repo root unless noted otherwise.

## 1) Install Floe (Homebrew)

```
brew tap malon64/floe
brew install floe
floe --version
```

## 2) Create a Python virtual environment

```
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r bench/requirements.txt
```

PySpark requires Java (8+). If you hit a Java error, ensure `JAVA_HOME` points
to a valid JDK.

## 3) Prepare benchmark datasets

From the `bench/` directory:

```
cd bench
python3 scripts/prepare_data.py
```

To limit sizes (optional):

```
python3 scripts/prepare_data.py --sizes 100000,1000000
```

## 4) Run the full benchmark

```
./scripts/run_all.sh
```

This builds the local Floe binary with Cargo and runs the benchmark against
`../target/release/floe`. To skip the build step, set `SKIP_BUILD=1` and ensure
`FLOE_BIN` points to a valid binary.

Optional environment flags:

- Skip Spark:

```
SKIP_SPARK=1 ./scripts/run_all.sh
```

- Limit sizes:

```
SIZES=100000,1000000 ./scripts/run_all.sh
```

## 5) Review results

Results are written to:

```
bench/results/results.csv
```

This CSV is slide-ready and includes tool, dataset, row count, wall time, and
(best-effort) peak RSS. Per-run reports are written under:

```
bench/results/report/run_<run_id>/uber/run.json
```
