from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FloeEntityPlan:
    name: str
    domain: str | None

    @property
    def asset_key_parts(self) -> list[str]:
        if self.domain:
            return [self.domain, self.name]
        return [self.name]

    @property
    def group_name(self) -> str:
        return self.domain or "floe"

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "FloeEntityPlan":
        name = str(data.get("name"))
        domain = data.get("domain")
        if domain is not None:
            domain = str(domain)
        return FloeEntityPlan(name=name, domain=domain)


@dataclass(frozen=True)
class FloeValidatePlan:
    entities: list[FloeEntityPlan]

    @staticmethod
    def from_validate_json(data: dict[str, Any]) -> "FloeValidatePlan":
        if data.get("schema") != "floe.plan.v1":
            raise ValueError("validate json schema mismatch (expected floe.plan.v1)")
        if not data.get("valid", False):
            errors = data.get("errors", [])
            raise ValueError(f"config invalid: {errors}")
        plan = data.get("plan") or {}
        entities_raw = plan.get("entities") or []
        entities = [FloeEntityPlan.from_dict(item) for item in entities_raw]
        return FloeValidatePlan(entities=entities)

