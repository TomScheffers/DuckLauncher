import asyncio
import logging

import asyncpg
import httpx

from ducklauncher.config import CoordinatorSettings
from ducklauncher.db import queries as db

logger = logging.getLogger(__name__)


async def dispatch_query(
    pool: asyncpg.Pool,
    settings: CoordinatorSettings,
    claimed: asyncpg.Record,
    http_client: httpx.AsyncClient,
) -> bool:
    payload = {
        "query_id": str(claimed["query_id"]),
        "query": claimed["query"],
        "cpus": claimed["cpus"],
        "memory": claimed["memory"],
        "disk_space": claimed["disk_space"],
    }
    endpoint = claimed["endpoint"].rstrip("/")
    try:
        response = await http_client.post(f"{endpoint}/query", json=payload)
        if response.status_code != 202:
            logger.warning(
                "Worker rejected query %s with status %s",
                claimed["query_id"],
                response.status_code,
            )
            await db.revert_query_to_pending(pool, claimed["query_id"])
            return False
        return True
    except httpx.HTTPError:
        logger.warning("Failed to dispatch query %s to %s", claimed["query_id"], endpoint)
        await db.revert_query_to_pending(pool, claimed["query_id"])
        return False


async def schedule_pending_queries(
    pool: asyncpg.Pool,
    settings: CoordinatorSettings,
    http_client: httpx.AsyncClient,
    query_id=None,
    max_batch: int = 10,
) -> int:
    scheduled = 0
    while scheduled < max_batch:
        if query_id is not None and scheduled == 0:
            claimed = await db.claim_query_by_id(
                pool,
                query_id=query_id,
                worker_stale_sec=settings.worker_stale_sec,
            )
        else:
            claimed = await db.claim_pending_query(
                pool,
                worker_stale_sec=settings.worker_stale_sec,
            )
        if claimed is None:
            break
        await dispatch_query(pool, settings, claimed, http_client)
        scheduled += 1
    return scheduled


def trigger_schedule(app, query_id=None, max_batch: int = 10) -> None:
    pool: asyncpg.Pool = app.state.pool
    settings: CoordinatorSettings = app.state.settings
    http_client: httpx.AsyncClient = app.state.http_client
    asyncio.create_task(schedule_pending_queries(pool, settings, http_client, query_id=query_id, max_batch=max_batch))


async def scheduler_loop(
    pool: asyncpg.Pool,
    settings: CoordinatorSettings,
    http_client: httpx.AsyncClient,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        try:
            await db.sweep_stale_workers(pool, worker_stale_sec=settings.worker_stale_sec)
            await schedule_pending_queries(pool, settings, http_client)
        except Exception:
            logger.exception("Scheduler iteration failed")
        await asyncio.sleep(settings.scheduler_interval_sec)
