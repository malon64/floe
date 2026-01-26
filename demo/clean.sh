#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

rm -rf "$ROOT/out" "$ROOT/report"

mkdir -p \
  "$ROOT/out/accepted/customer" \
  "$ROOT/out/accepted/orders" \
  "$ROOT/out/accepted/events" \
  "$ROOT/out/accepted/customer_delta" \
  "$ROOT/out/rejected/customer" \
  "$ROOT/out/rejected/orders" \
  "$ROOT/out/rejected/events" \
  "$ROOT/report"

touch \
  "$ROOT/out/accepted/customer/.gitkeep" \
  "$ROOT/out/accepted/orders/.gitkeep" \
  "$ROOT/out/accepted/events/.gitkeep" \
  "$ROOT/out/accepted/customer_delta/.gitkeep" \
  "$ROOT/out/rejected/customer/.gitkeep" \
  "$ROOT/out/rejected/orders/.gitkeep" \
  "$ROOT/out/rejected/events/.gitkeep" \
  "$ROOT/report/.gitkeep"

echo "demo outputs cleared."
