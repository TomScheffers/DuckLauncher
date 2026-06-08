import asyncio
import time

import httpx
import pytest


async def submit_query(client: httpx.AsyncClient, sql: str, **resources) -> dict:
    response = await client.post("/queries", json={"query": sql, **resources})
    response.raise_for_status()
    return response.json()


async def wait_for_status(
    client: httpx.AsyncClient,
    query_id: str,
    status: str,
    timeout: float = 30,
) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        response = await client.get(f"/queries/{query_id}")
        response.raise_for_status()
        payload = response.json()
        if payload["status"] == status:
            return payload
        if payload["status"] in ("failed", "cancelled") and status not in ("failed", "cancelled"):
            raise AssertionError(f"Query {query_id} reached {payload['status']} while waiting for {status}")
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Query {query_id} did not reach status {status} within {timeout}s")


@pytest.mark.asyncio
async def test_query_runs_to_completion(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        created = await submit_query(client, "SELECT 1 AS value")
        result = await wait_for_status(client, created["query_id"], "completed")
        assert result["error"] is None
        assert result["worker_id"] is not None


@pytest.mark.asyncio
async def test_sessions_are_stateless_between_queries(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        first = await submit_query(
            client,
            "CREATE TEMP TABLE session_mark AS SELECT 42 AS v; SELECT v FROM session_mark",
        )
        await wait_for_status(client, first["query_id"], "completed")

        second = await submit_query(client, "SELECT v FROM session_mark")
        result = await wait_for_status(client, second["query_id"], "failed", timeout=30)
        assert result["error"] is not None
        assert "session_mark" in result["error"].lower() or "does not exist" in result["error"].lower()


@pytest.mark.asyncio
async def test_cancel_running_query(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=60) as client:
        created = await submit_query(client, "SELECT sum(x) FROM range(500000000) t(x)")
        await wait_for_status(client, created["query_id"], "running", timeout=15)

        response = await client.post(f"/queries/{created['query_id']}/cancel")
        response.raise_for_status()
        result = await wait_for_status(client, created["query_id"], "cancelled", timeout=30)
        assert result["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_does_not_affect_other_running_queries(launcher_stack: dict[str, str]) -> None:
    long_query = "SELECT sum(x) FROM range(500000000) t(x)"
    short_query = "SELECT 123 AS value"

    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=60) as client:
        long_job = await submit_query(client, long_query)
        short_job = await submit_query(client, short_query)

        await wait_for_status(client, long_job["query_id"], "running", timeout=15)
        short_result = await wait_for_status(client, short_job["query_id"], "completed", timeout=30)
        assert short_result["status"] == "completed"

        response = await client.post(f"/queries/{long_job['query_id']}/cancel")
        response.raise_for_status()
        long_result = await wait_for_status(client, long_job["query_id"], "cancelled", timeout=30)
        assert long_result["status"] == "cancelled"
