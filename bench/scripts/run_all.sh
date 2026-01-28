#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_ROOT="$(cd "$ROOT/.." && pwd)"
RESULTS_FILE="$ROOT/results/results.csv"

mkdir -p "$ROOT/results" "$ROOT/results/report" "$ROOT/out/accepted" "$ROOT/out/rejected"

: > "$RESULTS_FILE"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  cargo build -p floe-cli --release --manifest-path "$PROJECT_ROOT/Cargo.toml"
fi

export FLOE_BIN="${FLOE_BIN:-$PROJECT_ROOT/target/release/floe}"

if [[ "${SKIP_PREP:-0}" != "1" ]]; then
  if [[ -n "${SIZES:-}" ]]; then
    python3 "$ROOT/scripts/prepare_data.py" --sizes "$SIZES"
  else
    python3 "$ROOT/scripts/prepare_data.py"
  fi
fi

if [[ "${SKIP_FLOE:-0}" != "1" ]]; then
  bash "$ROOT/scripts/run_floe.sh"
fi

if [[ "${SKIP_PANDAS:-0}" != "1" ]]; then
  if [[ -n "${SIZES:-}" ]]; then
    python3 "$ROOT/scripts/run_pandas.py" --sizes "$SIZES"
  else
    python3 "$ROOT/scripts/run_pandas.py"
  fi
fi

if [[ "${SKIP_SPARK:-0}" != "1" ]]; then
  if [[ -n "${SIZES:-}" ]]; then
    python3 "$ROOT/scripts/run_spark.py" --sizes "$SIZES"
  else
    python3 "$ROOT/scripts/run_spark.py"
  fi
fi

echo "Benchmark complete: $RESULTS_FILE"
