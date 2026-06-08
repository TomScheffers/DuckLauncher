import asyncio
import logging
import signal
from uuid import UUID

import httpx
import os
import sys

from ducklauncher.config import WorkerSettings, resolve_local_worker_id
from ducklauncher.models import Metrics, WorkerHeartbeatRequest, WorkerRegisterRequest

logger = logging.getLogger(__name__)


class WorkerState:
    def __init__(self) -> None:
        self.worker_id: UUID | None = None
        self.shutting_down = False
        self.shutdown_event = asyncio.Event()


def persist_worker_id(settings: WorkerSettings, worker_id: UUID) -> None:
    path = settings.worker_id_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(worker_id))


async def collect_metrics() -> Metrics:
    mem_total_mb = 8192
    mem_free_mb = 4096
    mem_used_mb = mem_total_mb - mem_free_mb
    try:
        meminfo: dict[str, int] = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key, value = parts
                    meminfo[key.strip()] = int(value.strip().split()[0])
        mem_total_mb = meminfo.get("MemTotal", 0) // 1024
        mem_free_mb = (meminfo.get("MemAvailable", meminfo.get("MemFree", 0))) // 1024
        mem_used_mb = mem_total_mb - mem_free_mb
    except FileNotFoundError:
        if sys.platform != "darwin":
            raise

    load_avg_1min = 0.0
    if hasattr(os, "getloadavg"):
        try:
            load_avg_1min = os.getloadavg()[0]
        except OSError:
            pass
    cpu_count = os.cpu_count() or 1
    cpu_usage = min(100.0, (load_avg_1min / cpu_count) * 100)

    return Metrics(
        memory_used_mb=mem_used_mb,
        memory_left_mb=mem_free_mb,
        cpu_count=cpu_count,
        cpu_usage=round(cpu_usage, 2),
    )


def available_disk_space_mb() -> int:
    try:
        stat = os.statvfs("/")
        return (stat.f_bavail * stat.f_frsize) // (1024 * 1024)
    except OSError:
        return 1024


def worker_resources(settings: WorkerSettings, metrics: Metrics) -> tuple[int, int, int]:
    cpus = settings.cpus if settings.cpus is not None else metrics.cpu_count
    memory = settings.memory if settings.memory is not None else metrics.memory_left_mb
    disk_space = settings.disk_space if settings.disk_space is not None else available_disk_space_mb()
    return cpus, memory, disk_space


async def register_with_coordinator(
    client: httpx.AsyncClient,
    settings: WorkerSettings,
    state: WorkerState,
    *,
    max_attempts: int = 30,
    retry_delay_sec: float = 0.5,
) -> list[str]:
    metrics = await collect_metrics()
    cpus, memory, disk_space = worker_resources(settings, metrics)
    existing_id = resolve_local_worker_id(settings)
    payload = WorkerRegisterRequest(
        worker_id=existing_id,
        endpoint=settings.worker_endpoint,
        cpus=cpus,
        memory=memory,
        disk_space=disk_space,
        max_concurrent_queries=settings.max_concurrent_queries,
    )
    register_url = f"{settings.coordinator_url}/workers/register"
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(
                "Registering worker at %s with coordinator %s (attempt %d/%d)",
                settings.worker_endpoint,
                settings.coordinator_url,
                attempt,
                max_attempts,
            )
            response = await client.post(register_url, json=payload.model_dump(mode="json"))
            response.raise_for_status()
            data = response.json()
            state.worker_id = UUID(data["worker_id"])
            persist_worker_id(settings, state.worker_id)
            logger.info("Worker registered with id %s", state.worker_id)
            return data.get("init_scripts", [])
        except httpx.HTTPError as exc:
            last_error = exc
            response_body = ""
            if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
                response_body = exc.response.text
            logger.warning(
                "Worker registration failed (attempt %d/%d): %s %s",
                attempt,
                max_attempts,
                exc,
                response_body,
            )
            if attempt < max_attempts:
                await asyncio.sleep(retry_delay_sec)

    logger.error("Worker registration failed after %d attempts", max_attempts)
    assert last_error is not None
    raise last_error


async def _wait_or_shutdown(state: WorkerState, seconds: float) -> bool:
    try:
        await asyncio.wait_for(state.shutdown_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


async def heartbeat_loop(
    client: httpx.AsyncClient,
    settings: WorkerSettings,
    state: WorkerState,
    executor=None,
) -> None:
    while not state.shutdown_event.is_set():
        if state.worker_id is None:
            if await _wait_or_shutdown(state, settings.heartbeat_interval_sec):
                break
            continue
        try:
            metrics = await collect_metrics()
            cpus, memory, disk_space = worker_resources(settings, metrics)
            status = "shutting_down" if state.shutting_down else "running"
            payload = WorkerHeartbeatRequest(
                cpus=cpus,
                memory=memory,
                disk_space=disk_space,
                status=status,
                memory_used_mb=metrics.memory_used_mb,
                cpu_usage=metrics.cpu_usage,
                running_queries=executor.active_count if executor is not None else None,
            )
            await client.post(
                f"{settings.coordinator_url}/workers/{state.worker_id}/heartbeat",
                json=payload.model_dump(mode="json"),
            )
        except httpx.HTTPError:
            logger.warning("Heartbeat failed")
        if await _wait_or_shutdown(state, settings.heartbeat_interval_sec):
            break


async def notify_shutdown(client: httpx.AsyncClient, settings: WorkerSettings, state: WorkerState) -> None:
    if state.worker_id is None:
        return
    try:
        await asyncio.wait_for(
            client.post(f"{settings.coordinator_url}/workers/{state.worker_id}/shutdown"),
            timeout=5.0,
        )
    except (httpx.HTTPError, asyncio.TimeoutError):
        logger.warning("Failed to notify coordinator of shutdown")


def install_signal_handlers(
    state: WorkerState,
    loop: asyncio.AbstractEventLoop,
    server: object | None = None,
) -> None:
    def handle_shutdown() -> None:
        if state.shutting_down:
            logger.warning("Forced shutdown")
            os._exit(1)
        logger.info("Received shutdown signal, initiating graceful shutdown")
        state.shutting_down = True
        state.shutdown_event.set()
        should_exit = getattr(server, "should_exit", None)
        if should_exit is not None:
            server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_shutdown)
