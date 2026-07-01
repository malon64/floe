#!/usr/bin/env bash
# Smoke test: verify that a Glue table with StorageDescriptor.Columns populated
# returns a non-empty columns list — confirming the API shape Floe's patched code
# produces is accepted by Glue and reflected back on get-table.
#
# Usage: ./scripts/smoke_test_glue_schema.sh [--region <region>]
#
# Prerequisites: aws CLI v2, credentials with glue:CreateDatabase,
#   glue:CreateTable, glue:GetTable, glue:DeleteTable, glue:DeleteDatabase.

set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-eu-west-1}"
while [[ $# -gt 0 ]]; do
  case $1 in
    --region) REGION="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

DATABASE="floe-smoke-test"
TABLE="accounts"
LOCATION="s3://floe-smoke-test-placeholder/warehouse/accounts"
METADATA="s3://floe-smoke-test-placeholder/warehouse/accounts/metadata/00001-abc.metadata.json"

cleanup() {
  echo "--- cleanup ---"
  aws glue delete-table --database-name "$DATABASE" --name "$TABLE" --region "$REGION" 2>/dev/null || true
  aws glue delete-database --name "$DATABASE" --region "$REGION" 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Floe Glue schema smoke test (region: $REGION) ==="

# 1. Create database
echo "[1] Creating Glue database: $DATABASE"
aws glue create-database \
  --database-input "{\"Name\":\"$DATABASE\"}" \
  --region "$REGION"

# 2. Create table with StorageDescriptor.Columns populated (mimics patched Floe output)
echo "[2] Creating Glue table: $TABLE"
aws glue create-table \
  --database-name "$DATABASE" \
  --table-input '{
    "Name": "'"$TABLE"'",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {
      "table_type":          "ICEBERG",
      "EXTERNAL":            "TRUE",
      "metadata_location":   "'"$METADATA"'",
      "floe.iceberg.namespace": "'"$DATABASE"'",
      "floe.managed":        "true"
    },
    "StorageDescriptor": {
      "Location": "'"$LOCATION"'",
      "Columns": [
        {"Name": "account_id",   "Type": "bigint"},
        {"Name": "account_name", "Type": "string"},
        {"Name": "segment",      "Type": "string"},
        {"Name": "region",       "Type": "string"},
        {"Name": "signup_date",  "Type": "date"},
        {"Name": "arr",          "Type": "double"}
      ],
      "InputFormat":  "org.apache.hadoop.mapred.FileInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
      "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}
    }
  }' \
  --region "$REGION"

# 3. Read back and assert columns are non-empty
echo "[3] Verifying StorageDescriptor.Columns is non-empty"
COLUMNS=$(aws glue get-table \
  --database-name "$DATABASE" \
  --name "$TABLE" \
  --region "$REGION" \
  --query 'Table.StorageDescriptor.Columns' \
  --output json)

echo "    Columns: $COLUMNS"

COUNT=$(echo "$COLUMNS" | python3 -c "import sys, json; print(len(json.load(sys.stdin)))")
if [[ "$COUNT" -eq 0 ]]; then
  echo "FAIL: StorageDescriptor.Columns is empty — schema will not appear in Glue REST loadTable."
  exit 1
fi

# 4. Verify metadata_location is preserved
ML=$(aws glue get-table \
  --database-name "$DATABASE" \
  --name "$TABLE" \
  --region "$REGION" \
  --query "Table.Parameters.metadata_location" \
  --output text)

if [[ "$ML" != "$METADATA" ]]; then
  echo "FAIL: metadata_location mismatch (got: $ML)"
  exit 1
fi

echo ""
echo "PASS: $COUNT columns returned in StorageDescriptor and metadata_location intact."
echo "      Glue Iceberg REST loadTable will include 'schemas' for this table."
