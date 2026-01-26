# Benchmarking Floe vs Pandas vs Spark (local)

This benchmark harness runs Floe, Pandas, and Spark on the same Uber pickup CSV
schema using a single-node laptop setup. It is deterministic and designed to
produce slide-ready `bench/results/results.csv`.

## Dataset + preparation

- Base file: `bench/data/uber-raw-data-apr14.csv` (Kaggle Uber pickups).
- Generated inputs (CSV) are written to `bench/generated/`:
  - `uber_100k.csv`
  - `uber_1m.csv`
  - `uber_5m.csv`
- Generation is deterministic: rows are replayed from the base file in order.
- A synthetic `row_id` column is injected (strictly increasing).
- Every 10,000th row has an empty `pickup_datetime` to exercise not-null logic.

Generate inputs (run from the `bench/` directory):

```
python3 scripts/prepare_data.py
```

To limit sizes:

```
python3 scripts/prepare_data.py --sizes 100000,1000000
```

## Validation rules (aligned across tools)

- Casts:
  - `row_id` -> integer
  - `pickup_datetime` -> timestamp (format: `M/d/yyyy H:mm:ss`)
  - `lat`, `lon` -> float
  - `base` -> string
- Not-null: `row_id` (pickup_datetime is nullable for the benchmark)
- Unique: `row_id` (keep first, reject duplicates)
 - Cast mode: coerce

## Run all benchmarks

From repo root:

```
cd bench
./scripts/run_all.sh
```

Options:
- `SKIP_BUILD=1` to skip rebuilding `../target/release/floe`
- Limit sizes: `SIZES=100000,1000000 ./scripts/run_all.sh`
- Skip a tool: `SKIP_SPARK=1 ./scripts/run_all.sh`

Results land in:

```
bench/results/results.csv
```

## Prerequisites

- Floe installed (Homebrew recommended):
  - `brew tap malon64/floe`
  - `brew install floe`
- Python 3.x + pandas:
  - `pip install -r bench/requirements.txt`
- Spark (local mode) for PySpark runner:
  - `pip install pyspark`
  - Java 8+ available (`JAVA_HOME` set if needed)

## Fairness rules

- Same generated inputs for all tools.
- Same schema casts, not-null, and unique rule.
- Same accepted/rejected counting strategy.
- Spark runs in `local[*]` mode with minimal configuration.

## Limitations

- Spark startup overhead is included in the timing.
- Peak RSS uses best-effort process metrics and may under-report Spark JVM usage.
- Floe writes accepted/rejected outputs; pandas/spark runners only compute counts
  (output writing can be added if desired).
- The 5M dataset is constructed by replaying the base file; it is not a distinct
  source file.

## Hardware notes (fill in)

- CPU:
- RAM:
- OS:
- Storage:
