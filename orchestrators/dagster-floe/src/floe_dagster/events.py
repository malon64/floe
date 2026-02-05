from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class FloeRunFinished:
    run_id: str
    status: str
    exit_code: int
    summary_uri: str | None


def parse_ndjson_events(stdout: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON log line: {stripped!r}") from exc
        if not isinstance(value, dict):
            raise ValueError("log line must be a JSON object")
        events.append(value)
    return events


def last_run_finished(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    last: dict[str, Any] | None = None
    for event in events:
        if event.get("event") == "run_finished":
            last = event
    if last is None:
        raise ValueError("run_finished event not found in stdout stream")
    return last


def parse_run_finished(event: dict[str, Any]) -> FloeRunFinished:
    if event.get("event") != "run_finished":
        raise ValueError("expected run_finished event")
    return FloeRunFinished(
        run_id=str(event.get("run_id")),
        status=str(event.get("status")),
        exit_code=int(event.get("exit_code")),
        summary_uri=event.get("summary_uri"),
    )

