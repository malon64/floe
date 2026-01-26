#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_FILE="$ROOT/bench/results/results.csv"

mkdir -p "$ROOT/bench/results" "$ROOT/bench/results/report" "$ROOT/bench/out/accepted" "$ROOT/bench/out/rejected"

: > "$RESULTS_FILE"

if [[ "${SKIP_PREP:-0}" != "1" ]]; then
  if [[ -n "${SIZES:-}" ]]; then
    python3 "$ROOT/bench/scripts/prepare_data.py" --sizes "$SIZES"
  else
    python3 "$ROOT/bench/scripts/prepare_data.py"
  fi
fi

if [[ "${SKIP_FLOE:-0}" != "1" ]]; then
  bash "$ROOT/bench/scripts/run_floe.sh"
fi

if [[ "${SKIP_PANDAS:-0}" != "1" ]]; then
  if [[ -n "${SIZES:-}" ]]; then
    python3 "$ROOT/bench/scripts/run_pandas.py" --sizes "$SIZES"
  else
    python3 "$ROOT/bench/scripts/run_pandas.py"
  fi
fi

if [[ "${SKIP_SPARK:-0}" != "1" ]]; then
  if [[ -n "${SIZES:-}" ]]; then
    python3 "$ROOT/bench/scripts/run_spark.py" --sizes "$SIZES"
  else
    python3 "$ROOT/bench/scripts/run_spark.py"
  fi
fi

echo "Benchmark complete: $RESULTS_FILE"
