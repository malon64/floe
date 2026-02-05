import json
from pathlib import Path

from floe_dagster.plan import FloeValidatePlan


def test_plan_asset_keys_and_groups():
    fixture = Path(__file__).parent / "fixtures" / "validate_plan.json"
    data = json.loads(fixture.read_text(encoding="utf-8"))
    plan = FloeValidatePlan.from_validate_json(data)

    assert [e.asset_key_parts for e in plan.entities] == [["hr", "employees"], ["orders"]]
    assert [e.group_name for e in plan.entities] == ["hr", "floe"]


