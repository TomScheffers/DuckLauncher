import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from ducklauncher.config import (
    WorkerSettings,
    resolve_local_worker_id,
    resolve_worker_storage,
    worker_connection_pool_size,
)
from ducklauncher.worker.duckdb import DuckDBExecutor
from ducklauncher.worker.registration import (
    WorkerState,
    heartbeat_loop,
    install_signal_handlers,
    notify_ready,
    notify_shutdown,
    register_with_coordinator,
)
from ducklauncher.worker.routes import router, shutdown_worker_queries

logger = logging.getLogger(__name__)


def create_worker_app(settings: WorkerSettings | None = None) -> FastAPI:
    resolved_settings = settings or WorkerSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        state = WorkerState()
        state.worker_id = resolve_local_worker_id(resolved_settings)
        duckdb_path, result_dir = resolve_worker_storage(resolved_settings, state.worker_id)
        pool_size = worker_connection_pool_size(resolved_settings)
        logger.info(
            "Starting worker %s: endpoint=%s coordinator=%s duckdb=%s result_dir=%s max_concurrent_queries=%s",
            state.worker_id,
            resolved_settings.worker_endpoint,
            resolved_settings.coordinator_url,
            duckdb_path,
            result_dir,
            resolved_settings.max_concurrent_queries,
        )
        executor = DuckDBExecutor(duckdb_path, pool_size, result_dir)
        client = httpx.AsyncClient(timeout=30.0)
        heartbeat_task: asyncio.Task | None = None

        app.state.settings = resolved_settings
        app.state.worker_state = state
        app.state.executor = executor
        app.state.http_client = client
        app.state.query_tasks = {}

        try:
            loop = asyncio.get_running_loop()
            install_signal_handlers(state, loop, getattr(app.state, "uvicorn_server", None))

            await asyncio.to_thread(executor.warm_pool)
            init_scripts = await register_with_coordinator(client, resolved_settings, state)
            if init_scripts:
                logger.info("Running %d init script(s)", len(init_scripts))
                await asyncio.to_thread(executor.run_init_scripts, init_scripts)

            await notify_ready(client, resolved_settings, state)
            heartbeat_task = asyncio.create_task(
                heartbeat_loop(client, resolved_settings, state, executor)
            )
            logger.info("Worker %s ready at %s", state.worker_id, resolved_settings.worker_endpoint)
        except Exception:
            logger.exception("Worker startup failed")
            executor.close()
            await client.aclose()
            raise

        yield

        if not state.shutting_down:
            state.shutting_down = True
            state.shutdown_event.set()
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        await notify_shutdown(client, resolved_settings, state)
        await shutdown_worker_queries(app)
        executor.close()
        await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app
