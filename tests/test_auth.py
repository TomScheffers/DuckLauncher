from datetime import datetime, timedelta, timezone
from uuid import uuid4

import asyncpg
import httpx
import pytest

from ducklauncher.db import sessions as db_sessions
from ducklauncher.db import users as db_users
from ducklauncher.db.pool import create_pool


async def _create_test_session(db_url: str) -> tuple[str, str]:
    pool = await create_pool(db_url)
    try:
        user = await db_users.upsert_user(
            pool,
            sub=f"test-{uuid4()}",
            email="test@example.com",
            name="Test User",
        )
        session_id = await db_sessions.create_session(
            pool,
            user_id=user["user_id"],
            ttl_hours=1,
        )
        return str(session_id), str(user["user_id"])
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_anonymous_session_created_on_first_request(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        response = await client.get("/auth/me")
        response.raise_for_status()
        payload = response.json()
        assert payload["auth_enabled"] is False
        assert payload["authenticated"] is False
        assert payload["anonymous"] is True
        assert "session_id" in response.cookies


@pytest.mark.asyncio
async def test_anonymous_session_persists_sheets(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        await client.get("/auth/me")
        created = await client.post(
            "/sheets",
            json={"name": "My sheet", "sql": "SELECT 1"},
        )
        created.raise_for_status()

        listed = await client.get("/sheets")
        listed.raise_for_status()
        assert len(listed.json()) == 1
        assert listed.json()[0]["name"] == "My sheet"


@pytest.mark.asyncio
async def test_anonymous_queries_mine_returns_empty_initially(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        await client.get("/auth/me")
        response = await client.get("/queries/mine")
        response.raise_for_status()
        assert response.json() == []


@pytest.mark.asyncio
async def test_anonymous_query_history_persists(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        await client.get("/auth/me")
        created = await client.post("/queries", json={"query": "SELECT 42 AS value"})
        created.raise_for_status()
        query_id = created.json()["query_id"]

        deadline = datetime.now(timezone.utc) + timedelta(seconds=30)
        while datetime.now(timezone.utc) < deadline:
            status = await client.get(f"/queries/{query_id}")
            status.raise_for_status()
            if status.json()["status"] == "completed":
                break
        else:
            pytest.fail("query did not complete")

        history = await client.get("/queries/mine")
        history.raise_for_status()
        assert any(item["query_id"] == query_id for item in history.json())


@pytest.mark.asyncio
async def test_worker_complete_works_without_explicit_session(launcher_stack: dict[str, str]) -> None:
    async with httpx.AsyncClient(base_url=launcher_stack["coordinator"], timeout=30) as client:
        created = await client.post("/queries", json={"query": "SELECT 1"})
        created.raise_for_status()
        query_id = created.json()["query_id"]

        deadline = datetime.now(timezone.utc) + timedelta(seconds=30)
        while datetime.now(timezone.utc) < deadline:
            status = await client.get(f"/queries/{query_id}")
            status.raise_for_status()
            if status.json()["status"] == "completed":
                return
        pytest.fail("query did not complete")


@pytest.mark.asyncio
async def test_auth_mode_query_ownership(db_url: str, tmp_path) -> None:
    import os
    import subprocess
    import sys
    import time
    from pathlib import Path

    from tests.conftest import free_port, terminate_process, wait_for_coordinator

    project_root = Path(__file__).resolve().parents[1]
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(
            """
            TRUNCATE sheets, sessions, queries, workers, users;
            """
        )
    finally:
        await conn.close()

    session_cookie, user_id = await _create_test_session(db_url)
    other_user = await asyncpg.connect(db_url)
    try:
        other = await other_user.fetchrow(
            """
            INSERT INTO users (sub, email, name)
            VALUES ($1, $2, $3)
            RETURNING user_id
            """,
            f"other-{uuid4()}",
            "other@example.com",
            "Other",
        )
        other_query = await other_user.fetchval(
            """
            INSERT INTO queries (query, status, user_id, completed_at)
            VALUES ('SELECT 2', 'completed', $1, now())
            RETURNING query_id
            """,
            other["user_id"],
        )
        owned_query = await other_user.fetchval(
            """
            INSERT INTO queries (query, status, user_id, completed_at)
            VALUES ('SELECT 1', 'completed', $1, now())
            RETURNING query_id
            """,
            user_id,
        )
    finally:
        await other_user.close()

    coord_port = free_port()
    coordinator_url = f"http://127.0.0.1:{coord_port}"
    worker_port = free_port()
    worker_url = f"http://127.0.0.1:{worker_port}"
    empty_init = tmp_path / "empty_init.sql"
    empty_init.write_text("")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)
    env["DATABASE_URL"] = db_url
    env["COORDINATOR_URL"] = coordinator_url
    env["WORKER_ENDPOINT"] = worker_url
    env["OIDC_ISSUER_URL"] = "https://issuer.example.com"
    env["OIDC_CLIENT_ID"] = "client"
    env["OIDC_CLIENT_SECRET"] = "secret"
    env["SESSION_SECRET"] = "session-secret-for-tests"
    env["PUBLIC_BASE_URL"] = coordinator_url
    env["DUCKDB_PATH"] = str(tmp_path / "test.duckdb")
    env["MAX_CONCURRENT_QUERIES"] = "2"
    env["WORKER_ID_PATH"] = str(tmp_path / "worker_id")
    env["PYTHONUNBUFFERED"] = "1"

    coord_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "ducklauncher",
            "coordinator",
            "--database-url",
            db_url,
            "--host",
            "127.0.0.1",
            "--port",
            str(coord_port),
            "--init-scripts",
            str(empty_init),
        ],
        cwd=project_root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_coordinator(f"{coordinator_url}/")
        cookies = {"session_id": session_cookie}
        async with httpx.AsyncClient(
            base_url=coordinator_url,
            timeout=30,
            cookies=cookies,
        ) as client:
            me = await client.get("/auth/me")
            me.raise_for_status()
            assert me.json()["authenticated"] is True
            assert me.json()["anonymous"] is False

            history = await client.get("/queries/mine")
            history.raise_for_status()
            ids = {item["query_id"] for item in history.json()}
            assert str(owned_query) in ids
            assert str(other_query) not in ids

            forbidden = await client.get(f"/queries/{other_query}")
            assert forbidden.status_code == 403

            allowed = await client.get(f"/queries/{owned_query}")
            allowed.raise_for_status()
    finally:
        terminate_process(coord_proc)
        time.sleep(0.2)
