from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import click
import uvicorn

from ducklauncher import __version__
from ducklauncher.apps.coordinator import create_coordinator_app
from ducklauncher.apps.worker import create_worker_app
from ducklauncher.config import CoordinatorSettings, WorkerSettings

logging.basicConfig(level=logging.INFO)


def _build_settings(model: type, **overrides: Any):
    values = {key: value for key, value in overrides.items() if value is not None}
    return model(**values)


@click.group()
@click.version_option(version=__version__)
def main() -> None:
    """Coordinator-worker system for scheduling DuckDB queries."""


@main.command()
@click.option("--init-scripts", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
@click.option("--database-url", default=None, help="PostgreSQL connection URL")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
@click.option("--heartbeat-interval-sec", default=None, type=int)
@click.option("--worker-stale-sec", default=None, type=int)
@click.option("--scheduler-interval-sec", default=None, type=float)
def coordinator(
    init_scripts: Path | None,
    database_url: str | None,
    host: str,
    port: int,
    heartbeat_interval_sec: int | None,
    worker_stale_sec: int | None,
    scheduler_interval_sec: float | None,
) -> None:
    """Run the DuckLauncher coordinator."""
    settings = _build_settings(
        CoordinatorSettings,
        init_scripts_path=str(init_scripts) if init_scripts else None,
        database_url=database_url,
        heartbeat_interval_sec=heartbeat_interval_sec,
        worker_stale_sec=worker_stale_sec,
        scheduler_interval_sec=scheduler_interval_sec,
    )
    app = create_coordinator_app(settings)
    uvicorn.run(app, host=host, port=port)


@main.command()
@click.option("--cpus", default=None, type=int, help="CPUs to advertise to the coordinator")
@click.option("--memory", default=None, type=int, help="Available memory in MB")
@click.option("--disk-space", default=None, type=int, help="Available disk space in MB")
@click.option("--coordinator-url", default=None)
@click.option("--endpoint", "worker_endpoint", default=None, help="Reachable worker base URL")
@click.option("--duckdb-path", default=None)
@click.option("--max-concurrent-queries", default=None, type=int)
@click.option("--connection-pool-size", default=None, type=int, help="Warm DuckDB connections (defaults to max-concurrent-queries)")
@click.option("--worker-id", default=None)
@click.option("--worker-id-path", type=click.Path(path_type=Path), default=None)
@click.option("--heartbeat-interval-sec", default=None, type=int)
@click.option(
    "--shutdown-cancel-queries/--no-shutdown-cancel-queries",
    default=None,
    help="Cancel in-flight queries on SIGTERM",
)
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8001, show_default=True, type=int)
def worker(
    cpus: int | None,
    memory: int | None,
    disk_space: int | None,
    coordinator_url: str | None,
    worker_endpoint: str | None,
    duckdb_path: str | None,
    max_concurrent_queries: int | None,
    connection_pool_size: int | None,
    worker_id: str | None,
    worker_id_path: Path | None,
    heartbeat_interval_sec: int | None,
    shutdown_cancel_queries: bool | None,
    host: str,
    port: int,
) -> None:
    """Run a DuckLauncher worker."""
    resolved_endpoint = worker_endpoint or f"http://127.0.0.1:{port}"
    settings = _build_settings(
        WorkerSettings,
        cpus=cpus,
        memory=memory,
        disk_space=disk_space,
        coordinator_url=coordinator_url,
        worker_endpoint=resolved_endpoint,
        duckdb_path=duckdb_path,
        max_concurrent_queries=max_concurrent_queries,
        connection_pool_size=connection_pool_size,
        worker_id=worker_id,
        worker_id_path=worker_id_path,
        heartbeat_interval_sec=heartbeat_interval_sec,
        shutdown_cancel_queries=shutdown_cancel_queries,
    )
    logging.getLogger("ducklauncher").setLevel(logging.INFO)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    app = create_worker_app(settings)
    uvicorn.run(app, host=host, port=port, log_level="info")
