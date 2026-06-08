import asyncio
import logging
from uuid import UUID

import httpx
from fastapi import APIRouter, HTTPException, Request

from ducklauncher.models import (
    CancelQueryRequest,
    CompleteQueryRequest,
    Metrics,
    QueryResultPage,
    RunQueryRequest,
)
from ducklauncher.worker.duckdb import DuckDBExecutor, QueryCancelledError
from ducklauncher.worker.registration import collect_metrics

logger = logging.getLogger(__name__)

router = APIRouter()


def _running_task_count(app) -> int:
    tasks: dict[UUID, asyncio.Task] = app.state.query_tasks
    return sum(1 for task in tasks.values() if not task.done())


@router.post("/metrics", response_model=Metrics)
async def metrics() -> Metrics:
    return await collect_metrics()


@router.post("/query", status_code=202)
async def run_query(request: Request, body: RunQueryRequest) -> dict[str, str]:
    state = request.app.state.worker_state
    settings = request.app.state.settings

    if state.shutting_down:
        raise HTTPException(status_code=503, detail="Worker is shutting down")

    if _running_task_count(request.app) >= settings.max_concurrent_queries:
        raise HTTPException(status_code=429, detail="Worker is at max concurrent queries")

    executor: DuckDBExecutor = request.app.state.executor
    task = asyncio.create_task(_execute_query(request.app, body, executor, settings))
    request.app.state.query_tasks[body.query_id] = task
    return {"status": "accepted", "query_id": str(body.query_id)}


@router.get("/results/{query_id}", response_model=QueryResultPage)
async def get_result(
    request: Request,
    query_id: UUID,
    offset: int = 0,
    limit: int = 100,
) -> QueryResultPage:
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 1000")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be non-negative")
    executor: DuckDBExecutor = request.app.state.executor
    try:
        page = await asyncio.to_thread(executor.read_result_page, query_id, offset, limit)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return QueryResultPage(
        query_id=query_id,
        offset=offset,
        limit=limit,
        total_rows=page["total_rows"],
        columns=page["columns"],
        rows=page["rows"],
    )


@router.post("/query/cancel")
async def cancel_query(request: Request, body: CancelQueryRequest | None = None) -> dict[str, str]:
    if body is None or body.query_id is None:
        raise HTTPException(status_code=400, detail="query_id is required")
    executor: DuckDBExecutor = request.app.state.executor
    cancelled = executor.cancel(body.query_id)
    if not cancelled:
        return {"status": "no_active_query"}
    return {"status": "cancelling", "query_id": str(body.query_id)}


async def _execute_query(
    app,
    body: RunQueryRequest,
    executor: DuckDBExecutor,
    settings,
) -> None:
    try:
        result = await asyncio.to_thread(executor.execute, body.query_id, body.query)
        await _report_completion(
            app,
            body.query_id,
            "completed",
            None,
            settings,
            result_row_count=result.result_row_count,
        )
    except QueryCancelledError as exc:
        await _report_completion(app, body.query_id, "cancelled", str(exc), settings)
    except Exception as exc:
        logger.exception("Query %s failed", body.query_id)
        await _report_completion(app, body.query_id, "failed", str(exc), settings)
    finally:
        app.state.query_tasks.pop(body.query_id, None)


async def _report_completion(
    app,
    query_id: UUID,
    status: str,
    error: str | None,
    settings,
    result_row_count: int | None = None,
) -> None:
    client: httpx.AsyncClient = app.state.http_client
    payload = CompleteQueryRequest(status=status, error=error, result_row_count=result_row_count)
    try:
        response = await client.post(
            f"{settings.coordinator_url}/queries/{query_id}/complete",
            json=payload.model_dump(mode="json"),
        )
        if response.status_code == 404:
            logger.warning("Coordinator did not accept completion for query %s", query_id)
    except httpx.HTTPError:
        logger.exception("Failed to report query %s completion", query_id)


async def shutdown_worker_queries(app) -> None:
    executor: DuckDBExecutor = app.state.executor
    settings = app.state.settings
    tasks: dict[UUID, asyncio.Task] = app.state.query_tasks
    running = [task for task in tasks.values() if not task.done()]
    if not running:
        return

    if not settings.shutdown_cancel_queries:
        logger.info("Waiting for %d in-flight queries to finish during shutdown", len(running))
        try:
            await asyncio.wait_for(asyncio.gather(*running, return_exceptions=True), timeout=120)
        except asyncio.TimeoutError:
            logger.warning("Timed out waiting for queries to finish")
        return

    logger.info("Cancelling %d in-flight queries during shutdown", len(running))
    executor.cancel_all()
    try:
        await asyncio.wait_for(asyncio.gather(*running, return_exceptions=True), timeout=30)
    except asyncio.TimeoutError:
        logger.warning("Timed out waiting for query cancellation, abandoning in-flight tasks")
        for task in running:
            if not task.done():
                task.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(*running, return_exceptions=True), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("In-flight queries did not stop; continuing shutdown")
