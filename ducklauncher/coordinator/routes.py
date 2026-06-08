import asyncio
from collections.abc import AsyncIterator
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

from ducklauncher.config import CoordinatorSettings, load_init_scripts
from ducklauncher.coordinator.auth import get_current_user, require_user
from ducklauncher.coordinator.events import (
    TERMINAL_QUERY_STATUSES,
    QueryEventHub,
    format_sse,
    query_event_payload,
)
from ducklauncher.coordinator.scheduler import dispatch_query, trigger_schedule
from ducklauncher.db import queries as db
from ducklauncher.db import sheets as db_sheets
from ducklauncher.models import (
    CatalogResponse,
    CompleteQueryRequest,
    CreateSheetRequest,
    QueryResponse,
    QueryResultPage,
    SheetResponse,
    SubmitQueryRequest,
    UpdateSheetRequest,
    WorkerHeartbeatRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerResponse,
)

router = APIRouter()


def _query_response(row: asyncpg.Record) -> QueryResponse:
    return QueryResponse(
        query_id=row["query_id"],
        worker_id=row["worker_id"],
        user_id=row.get("user_id"),
        status=row["status"],
        query=row["query"],
        error=row["error"],
        cpus=row["cpus"],
        memory=row["memory"],
        disk_space=row["disk_space"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        result_row_count=row["result_row_count"],
    )


def _sheet_response(row: asyncpg.Record) -> SheetResponse:
    return SheetResponse(
        sheet_id=row["sheet_id"],
        name=row["name"],
        sql=row["sql"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _worker_response(row: asyncpg.Record) -> WorkerResponse:
    return WorkerResponse(
        worker_id=row["worker_id"],
        endpoint=row["endpoint"],
        status=row["status"],
        cpus=row["cpus"],
        memory=row["memory"],
        disk_space=row["disk_space"],
        max_concurrent_queries=row["max_concurrent_queries"],
        running_queries=row["running_queries"],
        memory_used_mb=row["memory_used_mb"],
        cpu_usage=row["cpu_usage"],
        started_at=row["started_at"],
        last_heartbeat_at=row["last_heartbeat_at"],
    )


def _assert_query_access(
    row: asyncpg.Record,
    user: asyncpg.Record | None,
    settings: CoordinatorSettings,
) -> None:
    if not settings.auth_enabled:
        return
    query_user_id = row.get("user_id")
    if query_user_id is None:
        return
    if user is None or query_user_id != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not allowed to access this query")


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


@router.get("/workers", response_model=list[WorkerResponse])
async def list_workers(request: Request) -> list[WorkerResponse]:
    pool: asyncpg.Pool = request.app.state.pool
    rows = await db.list_workers(pool)
    return [_worker_response(row) for row in rows]


@router.get("/catalog", response_model=CatalogResponse)
async def get_catalog(request: Request) -> CatalogResponse:
    pool: asyncpg.Pool = request.app.state.pool
    http_client: httpx.AsyncClient = request.app.state.http_client
    worker = await _get_first_worker(pool)
    if worker is None:
        raise HTTPException(status_code=404, detail="No workers available")
    endpoint = worker["endpoint"].rstrip("/")
    try:
        response = await http_client.get(f"{endpoint}/catalog")
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch catalog from worker: {exc}") from exc
    return CatalogResponse.model_validate(response.json())


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
        memory_used_mb=body.memory_used_mb,
        cpu_usage=body.cpu_usage,
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


@router.get("/queries/mine", response_model=list[QueryResponse])
async def list_my_queries(
    request: Request,
    limit: int = 50,
    offset: int = 0,
) -> list[QueryResponse]:
    settings: CoordinatorSettings = request.app.state.settings
    if not settings.auth_enabled:
        return []
    user = await require_user(request)
    pool: asyncpg.Pool = request.app.state.pool
    rows = await db.list_queries_for_user(pool, user["user_id"], limit=limit, offset=offset)
    return [_query_response(row) for row in rows]


@router.post("/queries", response_model=QueryResponse, status_code=201)
async def submit_query(request: Request, body: SubmitQueryRequest) -> QueryResponse:
    pool: asyncpg.Pool = request.app.state.pool
    settings: CoordinatorSettings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http_client
    user = await get_current_user(request)
    if settings.auth_enabled and user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    user_id = user["user_id"] if user is not None else None
    query_id = await db.create_query(
        pool,
        query=body.query,
        cpus=body.cpus,
        memory=body.memory,
        disk_space=body.disk_space,
        user_id=user_id,
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
    settings: CoordinatorSettings = request.app.state.settings
    user = await get_current_user(request)
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Query not found")
    _assert_query_access(row, user, settings)
    return _query_response(row)


@router.get("/queries/{query_id}/events")
async def query_events(request: Request, query_id: UUID) -> StreamingResponse:
    pool: asyncpg.Pool = request.app.state.pool
    hub: QueryEventHub = request.app.state.event_hub
    settings: CoordinatorSettings = request.app.state.settings
    user = await get_current_user(request)
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Query not found")
    _assert_query_access(row, user, settings)

    async def event_stream() -> AsyncIterator[str]:
        queue = hub.subscribe(query_id)
        try:
            yield format_sse(query_event_payload(row))
            if row["status"] in TERMINAL_QUERY_STATUSES:
                return

            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
                    continue
                yield format_sse(event)
                if event.get("status") in TERMINAL_QUERY_STATUSES:
                    break
        finally:
            hub.unsubscribe(query_id, queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/queries/{query_id}/result", response_model=QueryResultPage)
async def get_query_result(
    request: Request,
    query_id: UUID,
    offset: int = 0,
    limit: int = 100,
) -> QueryResultPage:
    pool: asyncpg.Pool = request.app.state.pool
    settings: CoordinatorSettings = request.app.state.settings
    http_client: httpx.AsyncClient = request.app.state.http_client
    user = await get_current_user(request)
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Query not found")
    _assert_query_access(row, user, settings)
    if row["status"] != "completed":
        raise HTTPException(status_code=409, detail="Query is not completed")
    if row["result_row_count"] is None:
        raise HTTPException(status_code=404, detail="Query has no result data")
    worker = await _get_worker_endpoint(pool, row["worker_id"])
    if worker is None:
        raise HTTPException(status_code=502, detail="Worker not available")
    endpoint = worker["endpoint"].rstrip("/")
    try:
        response = await http_client.get(
            f"{endpoint}/results/{query_id}",
            params={"offset": offset, "limit": limit},
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch result from worker: {exc}") from exc
    return QueryResultPage.model_validate(response.json())


@router.post("/queries/{query_id}/complete")
async def complete_query(request: Request, query_id: UUID, body: CompleteQueryRequest) -> dict[str, str]:
    pool: asyncpg.Pool = request.app.state.pool
    updated = await db.complete_query(
        pool,
        query_id,
        status=body.status,
        error=body.error,
        result_row_count=body.result_row_count,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Query not found or not active")
    trigger_schedule(request.app)
    return {"status": body.status}


@router.post("/queries/{query_id}/cancel", response_model=QueryResponse)
async def cancel_query(request: Request, query_id: UUID) -> QueryResponse:
    pool: asyncpg.Pool = request.app.state.pool
    settings: CoordinatorSettings = request.app.state.settings
    user = await get_current_user(request)
    row = await db.get_query(pool, query_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Query not found")
    _assert_query_access(row, user, settings)
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


@router.get("/sheets", response_model=list[SheetResponse])
async def list_sheets(request: Request) -> list[SheetResponse]:
    settings: CoordinatorSettings = request.app.state.settings
    if not settings.auth_enabled:
        return []
    user = await require_user(request)
    pool: asyncpg.Pool = request.app.state.pool
    rows = await db_sheets.list_sheets(pool, user["user_id"])
    return [_sheet_response(row) for row in rows]


@router.post("/sheets", response_model=SheetResponse, status_code=201)
async def create_sheet(request: Request, body: CreateSheetRequest) -> SheetResponse:
    settings: CoordinatorSettings = request.app.state.settings
    if not settings.auth_enabled:
        raise HTTPException(status_code=404, detail="Sheets require authentication")
    user = await require_user(request)
    pool: asyncpg.Pool = request.app.state.pool
    row = await db_sheets.create_sheet(
        pool,
        user_id=user["user_id"],
        name=body.name,
        sql=body.sql,
    )
    return _sheet_response(row)


@router.patch("/sheets/{sheet_id}", response_model=SheetResponse)
async def update_sheet(request: Request, sheet_id: UUID, body: UpdateSheetRequest) -> SheetResponse:
    settings: CoordinatorSettings = request.app.state.settings
    if not settings.auth_enabled:
        raise HTTPException(status_code=404, detail="Sheets require authentication")
    user = await require_user(request)
    pool: asyncpg.Pool = request.app.state.pool
    row = await db_sheets.update_sheet(
        pool,
        sheet_id=sheet_id,
        user_id=user["user_id"],
        name=body.name,
        sql=body.sql,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Sheet not found")
    return _sheet_response(row)


@router.delete("/sheets/{sheet_id}")
async def delete_sheet(request: Request, sheet_id: UUID) -> dict[str, str]:
    settings: CoordinatorSettings = request.app.state.settings
    if not settings.auth_enabled:
        raise HTTPException(status_code=404, detail="Sheets require authentication")
    user = await require_user(request)
    pool: asyncpg.Pool = request.app.state.pool
    deleted = await db_sheets.delete_sheet(pool, sheet_id, user["user_id"])
    if not deleted:
        raise HTTPException(status_code=404, detail="Sheet not found")
    return {"status": "deleted"}


async def _get_first_worker(pool: asyncpg.Pool) -> asyncpg.Record | None:
    workers = await db.list_workers(pool)
    if not workers:
        return None
    return workers[0]


async def _get_worker_endpoint(pool: asyncpg.Pool, worker_id: UUID | None) -> asyncpg.Record | None:
    if worker_id is None:
        return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT worker_id, endpoint FROM workers WHERE worker_id = $1",
            worker_id,
        )
