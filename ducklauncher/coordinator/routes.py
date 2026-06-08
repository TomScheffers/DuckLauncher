import asyncio
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Request

from ducklauncher.config import CoordinatorSettings, load_init_scripts
from ducklauncher.coordinator.scheduler import dispatch_query, trigger_schedule
from ducklauncher.db import queries as db
from ducklauncher.models import (
    CompleteQueryRequest,
    QueryResponse,
    SubmitQueryRequest,
    WorkerHeartbeatRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
)

router = APIRouter()


def _query_response(row: asyncpg.Record) -> QueryResponse:
    return QueryResponse(
        query_id=row["query_id"],
        worker_id=row["worker_id"],
        status=row["status"],
        query=row["query"],
        error=row["error"],
        cpus=row["cpus"],
        memory=row["memory"],
        disk_space=row["disk_space"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


@router.get("/")
async def read_root() -> dict[str, str]:
    return {"message": "DuckLauncher coordinator"}


@router.post("/workers/register", response_model=WorkerRegisterResponse)
async def register_worker(request: Request, body: WorkerRegisterRequest) -> WorkerRegisterResponse:
    pool: asyncpg.Pool = request.app.state.pool
    settings: CoordinatorSettings = request.app.state.settings
    worker_id = await db.register_worker(
        pool,
        worker_id=body.worker_id,
        endpoint=body.endpoint,
        cpus=body.cpus,
        memory=body.memory,
        disk_space=body.disk_space,
        max_concurrent_queries=body.max_concurrent_queries,
    )
    response = WorkerRegisterResponse(
        worker_id=worker_id,
        init_scripts=load_init_scripts(settings.init_scripts_path),
    )
    trigger_schedule(request.app)
    return response


@router.post("/workers/{worker_id}/heartbeat")
async def heartbeat_worker(request: Request, worker_id: UUID, body: WorkerHeartbeatRequest) -> dict[str, str]:
    pool: asyncpg.Pool = request.app.state.pool
    updated = await db.heartbeat_worker(
        pool,
        worker_id,
        cpus=body.cpus,
        memory=body.memory,
        disk_space=body.disk_space,
        status=body.status,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Worker not found")
    return {"status": "ok"}


@router.post("/workers/{worker_id}/shutdown")
async def shutdown_worker(request: Request, worker_id: UUID) -> dict[str, str]:
    pool: asyncpg.Pool = request.app.state.pool
    updated = await db.shutdown_worker(pool, worker_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Worker not found or not running")
    return {"status": "shutting_down"}


@router.post("/queries", response_model=QueryResponse, status_code=201)
async def submit_query(request: Request, body: SubmitQueryRequest) -> QueryResponse:
    pool: asyncpg.Pool = request.app.state.pool
    settings: CoordinatorSettings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http_client
    query_id = await db.create_query(
        pool,
        query=body.query,
        cpus=body.cpus,
        memory=body.memory,
        disk_space=body.disk_space,
    )
    claimed = await db.claim_query_by_id(
        pool,
        query_id=query_id,
        worker_stale_sec=settings.worker_stale_sec,
    )
    if claimed is not None:
        asyncio.create_task(dispatch_query(pool, settings, claimed, http_client))
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to create query")
    return _query_response(row)


@router.get("/queries/{query_id}", response_model=QueryResponse)
async def get_query(request: Request, query_id: UUID) -> QueryResponse:
    pool: asyncpg.Pool = request.app.state.pool
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Query not found")
    return _query_response(row)


@router.post("/queries/{query_id}/complete")
async def complete_query(request: Request, query_id: UUID, body: CompleteQueryRequest) -> dict[str, str]:
    pool: asyncpg.Pool = request.app.state.pool
    updated = await db.complete_query(pool, query_id, status=body.status, error=body.error)
    if not updated:
        raise HTTPException(status_code=404, detail="Query not found or not active")
    trigger_schedule(request.app)
    return {"status": body.status}


@router.post("/queries/{query_id}/cancel", response_model=QueryResponse)
async def cancel_query(request: Request, query_id: UUID) -> QueryResponse:
    pool: asyncpg.Pool = request.app.state.pool
    settings: CoordinatorSettings = request.app.state.settings
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Query not found")
    if row["status"] in ("completed", "failed", "cancelled"):
        return _query_response(row)

    if row["status"] == "pending":
        cancelled = await db.mark_query_cancelled(pool, query_id, error="Cancelled before dispatch")
        if cancelled is None:
            raise HTTPException(status_code=409, detail="Query could not be cancelled")
        updated = await db.get_query(pool, query_id)
        return _query_response(updated)

    worker = await _get_worker_endpoint(pool, row["worker_id"])
    if worker is None:
        cancelled = await db.mark_query_cancelled(pool, query_id, error="Worker unavailable")
        updated = await db.get_query(pool, query_id)
        return _query_response(updated)

    async with httpx.AsyncClient(timeout=settings.dispatch_timeout_sec) as client:
        try:
            response = await client.post(
                f"{worker['endpoint'].rstrip('/')}/query/cancel",
                json={"query_id": str(query_id)},
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Failed to cancel on worker: {exc}") from exc

    for _ in range(20):
        updated = await db.get_query(pool, query_id)
        if updated and updated["status"] == "cancelled":
            return _query_response(updated)
        await asyncio.sleep(0.25)

    cancelled = await db.mark_query_cancelled(pool, query_id, error="Cancelled by request")
    if cancelled is None:
        updated = await db.get_query(pool, query_id)
        if updated and updated["status"] == "cancelled":
            return _query_response(updated)
        raise HTTPException(status_code=409, detail="Query could not be cancelled")
    updated = await db.get_query(pool, query_id)
    return _query_response(updated)


async def _get_worker_endpoint(pool: asyncpg.Pool, worker_id: UUID | None) -> asyncpg.Record | None:
    if worker_id is None:
        return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT worker_id, endpoint FROM workers WHERE worker_id = $1",
            worker_id,
        )
