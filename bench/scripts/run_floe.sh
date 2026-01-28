#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_FILE="$ROOT/results/results.csv"
CONFIG_BASE="$ROOT/config/bench.yml"
REPORT_DIR="$ROOT/results/report"
if [[ -z "${FLOE_BIN:-}" ]]; then
  if command -v brew >/dev/null 2>&1; then
    candidate="$(brew --prefix)/bin/floe"
    if [[ -x "$candidate" ]]; then
      FLOE_BIN="$candidate"
    else
      echo "floe not found via Homebrew at $candidate" >&2
      echo "Install with: brew install floe or set FLOE_BIN" >&2
      exit 1
    fi
  else
    echo "Homebrew not found; set FLOE_BIN to the brew-installed floe" >&2
    exit 1
  fi
fi

if [[ -n "${SIZES:-}" ]]; then
  IFS=',' read -r -a sizes <<< "$SIZES"
else
  sizes=(100000 1000000 5000000)
fi

label_for_size() {
  local size="$1"
  if [[ "$size" -ge 1000000 ]]; then
    echo "$((size / 1000000))m"
  else
    echo "$((size / 1000))k"
  fi
}

for size in "${sizes[@]}"; do
  label="$(label_for_size "$size")"
  input_file="$ROOT/generated/uber_${label}.csv"
  if [[ ! -f "$input_file" ]]; then
    echo "missing input: $input_file (run scripts/prepare_data.py)" >&2
    exit 1
  fi

  tmp_config="$(mktemp "$ROOT/config/bench_${label}_XXXX.yml")"
  sed "s|../generated/uber_100k.csv|../generated/uber_${label}.csv|" "$CONFIG_BASE" > "$tmp_config"

  run_id="bench-${label}"
  metrics_json="$(python3 "$ROOT/scripts/time_cmd.py" -- "$FLOE_BIN" run -c "$tmp_config" --run-id "$run_id" --quiet)"

  wall_time_s="$(python3 - <<'PY' "$metrics_json"
import json
import sys
payload = json.loads(sys.argv[1])
print(f"{payload['wall_time_s']:.4f}")
PY
)"
  peak_rss_mb="$(python3 - <<'PY' "$metrics_json"
import json
import sys
payload = json.loads(sys.argv[1])
value = payload.get("peak_rss_mb")
if value is None:
    print("")
else:
    print(f"{value:.2f}")
PY
)"

  report_path="$REPORT_DIR/run_${run_id}/uber/run.json"
  if [[ ! -f "$report_path" ]]; then
    echo "report not found: $report_path" >&2
    exit 1
  fi

  read_counts="$(python3 - <<'PY' "$report_path"
import json
import sys
with open(sys.argv[1], "r") as handle:
    data = json.load(handle)
results = data.get("results", {})
accepted = results.get("accepted_total", 0)
rejected = results.get("rejected_total", 0)
print(f"{accepted} {rejected}")
PY
)"
  accepted_rows="${read_counts%% *}"
  rejected_rows="${read_counts##* }"

  python3 "$ROOT/scripts/write_result.py" \
    --results-file "$RESULTS_FILE" \
    --tool "floe" \
    --dataset "uber" \
    --rows "$size" \
    --wall-time "$wall_time_s" \
    --peak-rss "$peak_rss_mb" \
    --accepted "$accepted_rows" \
    --rejected "$rejected_rows" \
    --notes ""

  rm -f "$tmp_config"
  echo "floe ${label} done"
done
