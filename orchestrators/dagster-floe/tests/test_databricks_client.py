from floe_dagster.databricks_client import DatabricksJobSpec, DatabricksJobsClient


class FakeHttp:
    def __init__(self) -> None:
        self.list_jobs = {"jobs": []}

    def request(self, method, url, *, headers=None, json_body=None, timeout_seconds=None):
        del headers, timeout_seconds
        if "/jobs/list" in url:
            return self.list_jobs
        if "/jobs/create" in url:
            return {"job_id": 42}
        if "/jobs/run-now" in url:
            return {"run_id": 88}
        if "/jobs/runs/get" in url:
            return {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                    "state_message": "ok",
                },
                "run_page_url": "https://example/run/88",
            }
        if "/jobs/reset" in url:
            return {}
        raise AssertionError(f"unexpected call: {method} {url} {json_body}")


def test_databricks_client_ensure_and_run() -> None:
    http = FakeHttp()
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

    assert job_id == 42
    assert run_id == 88
    assert terminal.result_state == "SUCCESS"
