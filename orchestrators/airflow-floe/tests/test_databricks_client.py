from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow_floe.databricks_client import DatabricksJobSpec, DatabricksJobsClient


class _FakeHttp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []
        self.list_jobs: dict = {"jobs": []}
        self.created_job_id = 123
        self.run_id = 999
        self.run_state = {
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "ok",
            },
            "run_page_url": "https://example/run/999",
        }

    def request(self, method: str, url: str, *, headers=None, json_body=None, timeout_seconds=None):
        del headers, timeout_seconds
        self.calls.append((method, url, json_body))
        if "/jobs/list" in url:
            return self.list_jobs
        if "/jobs/create" in url:
            return {"job_id": self.created_job_id}
        if "/jobs/reset" in url:
            return {}
        if "/jobs/run-now" in url:
            return {"run_id": self.run_id}
        if "/jobs/runs/get" in url:
            return self.run_state
        raise AssertionError(f"unexpected call: {method} {url}")


class DatabricksClientTests(unittest.TestCase):
    def test_ensure_run_and_poll_success(self) -> None:
        http = _FakeHttp()
        client = DatabricksJobsClient(
            http_client=http,
            workspace_url="https://adb.example.com",
            oauth_bearer_token="token",
        )
        spec = DatabricksJobSpec(
            workspace_url="https://adb.example.com",
            existing_cluster_id="1111-222222-abc123",
            config_uri="dbfs:/floe/configs/prod.yml",
            job_name="floe-sales-prod",
            command=["floe"],
            args=["run", "-c", "dbfs:/floe/configs/prod.yml"],
        )

        job_id = client.ensure_domain_job(spec)
        run_id = client.run_now(job_id=job_id, env_parameters={"FLOE_ENV": "prod"})
        terminal = client.poll_run_to_terminal(run_id=run_id, poll_interval_seconds=1, timeout_seconds=5)

        self.assertEqual(job_id, 123)
        self.assertEqual(run_id, 999)
        self.assertEqual(terminal.result_state, "SUCCESS")


if __name__ == "__main__":
    unittest.main()
