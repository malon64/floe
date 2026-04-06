from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from airflow_floe.databricks_client import DatabricksRunResult
from airflow_floe.databricks_runner import run_databricks_job
from airflow_floe.manifest import ManifestRunnerDefinition


class DatabricksRunnerTests(unittest.TestCase):
    def _runner(self, **overrides: object) -> ManifestRunnerDefinition:
        base: dict[str, object] = {
            "runner_type": "databricks_job",
            "image": None,
            "namespace": None,
            "service_account": None,
            "resources": None,
            "env": None,
            "command": None,
            "args": None,
            "timeout_seconds": 30,
            "ttl_seconds_after_finished": None,
            "poll_interval_seconds": 1,
            "secrets": None,
            "workspace_url": "https://adb.example.com",
            "existing_cluster_id": "1111-222222-abc123",
            "config_uri": "dbfs:/floe/config.yml",
            "job_name": "floe-sales-prod",
            "auth": None,
            "env_parameters": {"FLOE_ENV": "prod"},
        }
        base.update(overrides)
        return ManifestRunnerDefinition(**base)

    @patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "token"}, clear=False)
    def test_success_returns_normalized_payload(self) -> None:
        runner = self._runner()

        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 123
            client.run_now.return_value = 456
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=456,
                state="TERMINATED",
                result_state="SUCCESS",
                state_message="ok",
                run_page_url="https://adb.example.com/jobs/456",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=runner,
                entities=["orders"],
            )

        self.assertEqual(payload["schema"], "floe.airflow.run.v1")
        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["backend_type"], "databricks")
        self.assertEqual(payload["backend_run_id"], "456")
        self.assertEqual(payload["backend_status"], "SUCCESS")
        self.assertEqual(payload["entity"], "orders")
        self.assertNotIn("failure_reason", payload)
        self.assertEqual(payload["backend_metadata"]["job_id"], 123)
        self.assertEqual(payload["backend_metadata"]["run_page_url"], "https://adb.example.com/jobs/456")

    @patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "token"}, clear=False)
    def test_failure_maps_to_failed_with_failure_reason(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="FAILED",
                state_message="task crashed",
                run_page_url="https://adb.example.com/jobs/77",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=None,
            )

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["exit_code"], 1)
        self.assertEqual(payload["failure_reason"], "task crashed")

    @patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "token"}, clear=False)
    def test_timeout_maps_to_timeout(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="TIMEDOUT",
                state_message="run timed out",
                run_page_url="https://adb.example.com/jobs/77",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=["orders", "customers"],
            )

        self.assertEqual(payload["status"], "timeout")
        self.assertEqual(payload["backend_type"], "databricks")
        self.assertEqual(payload["failure_reason"], "run timed out")
        self.assertNotIn("entity", payload)

    @patch.dict(os.environ, {"FLOE_DATABRICKS_OAUTH_TOKEN": "token"}, clear=False)
    def test_canceled_maps_to_canceled(self) -> None:
        with patch("airflow_floe.databricks_runner.DatabricksJobsClient") as client_cls:
            client = client_cls.return_value
            client.ensure_domain_job.return_value = 99
            client.run_now.return_value = 77
            client.poll_run_to_terminal.return_value = DatabricksRunResult(
                run_id=77,
                state="TERMINATED",
                result_state="CANCELED",
                state_message="terminated by user",
                run_page_url="https://adb.example.com/jobs/77",
            )

            payload = run_databricks_job(
                cmd_args=["floe", "run", "-c", "dbfs:/floe/config.yml"],
                runner=self._runner(),
                entities=None,
            )

        self.assertEqual(payload["status"], "canceled")
        self.assertEqual(payload["backend_status"], "CANCELED")
        self.assertEqual(payload["failure_reason"], "terminated by user")


if __name__ == "__main__":
    unittest.main()
