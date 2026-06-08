import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from ducklauncher.config import WorkerSettings, worker_connection_pool_size
from ducklauncher.worker.duckdb import DuckDBExecutor
from ducklauncher.worker.registration import (
    WorkerState,
    heartbeat_loop,
    install_signal_handlers,
    notify_shutdown,
    register_with_coordinator,
)
from ducklauncher.worker.routes import router, shutdown_worker_queries

logger = logging.getLogger(__name__)


def create_worker_app(settings: WorkerSettings | None = None) -> FastAPI:
    resolved_settings = settings or WorkerSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info(
            "Starting worker: endpoint=%s coordinator=%s duckdb=%s max_concurrent_queries=%s",
            resolved_settings.worker_endpoint,
            resolved_settings.coordinator_url,
            resolved_settings.duckdb_path,
            resolved_settings.max_concurrent_queries,
        )
        state = WorkerState()
        pool_size = worker_connection_pool_size(resolved_settings)
        executor = DuckDBExecutor(resolved_settings.duckdb_path, pool_size=pool_size)
        client = httpx.AsyncClient(timeout=30.0)
        heartbeat_task: asyncio.Task | None = None

        app.state.settings = resolved_settings
        app.state.worker_state = state
        app.state.executor = executor
        app.state.http_client = client
        app.state.query_tasks = {}

        try:
            loop = asyncio.get_running_loop()
            install_signal_handlers(state, loop)

            await asyncio.to_thread(executor.warm_pool)
            init_scripts = await register_with_coordinator(client, resolved_settings, state)
            if init_scripts:
                logger.info("Running %d init script(s)", len(init_scripts))
                await asyncio.to_thread(executor.run_init_scripts, init_scripts)

            heartbeat_task = asyncio.create_task(heartbeat_loop(client, resolved_settings, state))
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
        await notify_shutdown(client, resolved_settings, state)
        await shutdown_worker_queries(app)
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
        executor.close()
        await client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    return app
