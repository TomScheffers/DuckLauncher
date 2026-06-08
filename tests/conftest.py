import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import asyncpg
import httpx
import pytest

from ducklauncher.db.pool import init_database

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def database_url() -> str:
    return os.environ.get(
        "TEST_DATABASE_URL",
        os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ducklauncher"),
    )


async def postgres_connect_error(url: str) -> str | None:
    try:
        conn = await asyncpg.connect(url, timeout=3)
        await conn.close()
        return None
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


async def reset_tables(url: str) -> None:
    await init_database(url)
    conn = await asyncpg.connect(url)
    try:
        await conn.execute("TRUNCATE sheets, sessions, queries, workers, users")
    finally:
        await conn.close()


def wait_for_coordinator(url: str, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = httpx.get(url, timeout=1)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Coordinator did not become ready: {url}")


def read_log_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(errors="replace")


def wait_for_worker(url: str, proc: subprocess.Popen, log_path: Path, timeout: float = 30) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        exit_code = proc.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"Worker exited with code {exit_code} before becoming ready: {url}\n"
                f"--- worker log ---\n{read_log_file(log_path)}"
            )
        try:
            response = httpx.post(f"{url}/metrics", timeout=1)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(
        f"Worker did not become ready: {url}\n--- worker log ---\n{read_log_file(log_path)}"
    )


def terminate_process(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@pytest.fixture
async def db_url() -> str:
    url = database_url()
    error = await postgres_connect_error(url)
    if error is not None:
        pytest.skip(
            f"PostgreSQL not available at {url} ({error}). "
            "If you use Docker, ensure Homebrew Postgres is not also bound to port 5432, "
            "or run ./scripts/run-postgres.sh and export the printed DATABASE_URL."
        )
    return url


@pytest.fixture
async def launcher_stack(db_url: str, tmp_path: Path) -> Iterator[dict[str, str]]:
    await reset_tables(db_url)

    coord_port = free_port()
    worker_port = free_port()
    coordinator_url = f"http://127.0.0.1:{coord_port}"
    worker_url = f"http://127.0.0.1:{worker_port}"
    duckdb_path = str(tmp_path / "test.duckdb")
    empty_init = tmp_path / "empty_init.sql"
    empty_init.write_text("")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    env["DATABASE_URL"] = db_url
    env["COORDINATOR_URL"] = coordinator_url
    env["WORKER_ENDPOINT"] = worker_url
    env["DUCKDB_PATH"] = duckdb_path
    env["MAX_CONCURRENT_QUERIES"] = "2"
    env["WORKER_ID_PATH"] = str(tmp_path / "worker_id")
    env["SCHEDULER_INTERVAL_SEC"] = "0.1"
    env["HEARTBEAT_INTERVAL_SEC"] = "1"
    env["WORKER_STALE_SEC"] = "60"
    env["PYTHONUNBUFFERED"] = "1"

    coord_log = tmp_path / "coordinator.log"
    worker_log = tmp_path / "worker.log"
    coord_stderr = coord_log.open("w")
    worker_stderr: object | None = None

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
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=coord_stderr,
    )
    try:
        wait_for_coordinator(f"{coordinator_url}/")

        worker_stderr = worker_log.open("w")
        worker_proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "ducklauncher",
                "worker",
                "--host",
                "127.0.0.1",
                "--port",
                str(worker_port),
                "--endpoint",
                worker_url,
                "--coordinator-url",
                coordinator_url,
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=worker_stderr,
        )
        wait_for_worker(worker_url, worker_proc, worker_log, timeout=30)
        time.sleep(0.5)
        yield {
            "coordinator": coordinator_url,
            "worker": worker_url,
            "duckdb_path": duckdb_path,
            "database_url": db_url,
        }
    finally:
        if "worker_proc" in locals():
            terminate_process(worker_proc)
        terminate_process(coord_proc)
        coord_stderr.close()
        if worker_stderr is not None:
            worker_stderr.close()
        await reset_tables(db_url)
