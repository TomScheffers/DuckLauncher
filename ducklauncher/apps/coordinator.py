import asyncio
import logging
from contextlib import asynccontextmanager
from importlib import resources

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ducklauncher.config import CoordinatorSettings
from ducklauncher.coordinator.routes import router
from ducklauncher.coordinator.scheduler import scheduler_loop
from ducklauncher.db.pool import create_pool, run_migrations

logger = logging.getLogger(__name__)


def create_coordinator_app(settings: CoordinatorSettings | None = None) -> FastAPI:
    resolved_settings = settings or CoordinatorSettings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = resolved_settings
        pool = await create_pool(resolved_settings.database_url)
        app.state.pool = pool
        http_client = httpx.AsyncClient(timeout=resolved_settings.dispatch_timeout_sec)
        app.state.http_client = http_client
        await run_migrations(pool)
        logger.info("Database migrations applied")

        stop_event = asyncio.Event()
        scheduler_task = asyncio.create_task(
            scheduler_loop(pool, resolved_settings, http_client, stop_event)
        )
        yield
        stop_event.set()
        await scheduler_task
        await http_client.aclose()
        await pool.close()

    app = FastAPI(lifespan=lifespan)
    app.include_router(router)
    static_dir = resources.files("ducklauncher") / "static"
    app.mount("/ui", StaticFiles(directory=str(static_dir), html=True), name="ui")
    return app
