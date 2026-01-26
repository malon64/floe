# Benchmarking Step-by-Step (Floe vs Pandas vs Spark)

This is a full, reproducible walkthrough to run the benchmark end-to-end on a
single machine.

## 1) Install Floe (Homebrew)

```
brew tap malon64/floe
brew install floe
floe --version
```

## 2) Create a Python virtual environment

From the repo root:

```
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r bench/requirements.txt
```

## 3) Install Spark (PySpark)

```
pip install pyspark
```

Note: PySpark requires Java (8+). If you hit a Java error, ensure `JAVA_HOME`
points to a valid JDK.

## 4) Prepare benchmark datasets

```
python3 scripts/prepare_data.py
```

To limit sizes (optional):

```
python3 scripts/prepare_data.py --sizes 100000,1000000
```

## 5) Run the full benchmark

```
cd bench
./scripts/run_all.sh
```

Optional environment flags:

- Skip Spark:

```
SKIP_SPARK=1 ./scripts/run_all.sh
```

- Limit sizes:

```
SIZES=100000,1000000 ./scripts/run_all.sh
```

## 6) Review results

Results are written to:

```
bench/results/results.csv
```

This CSV is slide-ready and includes tool, dataset, row count, wall time, and
(best-effort) peak RSS.
