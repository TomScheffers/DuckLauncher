from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class CoordinatorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://postgres:postgres@localhost:5432/ducklauncher"
    init_scripts_path: str | None = None
    heartbeat_interval_sec: int = 10
    worker_stale_sec: int = 30
    scheduler_interval_sec: float = 0.1
    dispatch_timeout_sec: float = 5.0


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    coordinator_url: str = "http://127.0.0.1:8000"
    worker_endpoint: str = "http://127.0.0.1:8001"
    worker_id: str | None = None
    worker_id_path: Path = Path.home() / ".ducklauncher" / "worker_id"
    duckdb_path: str = ":memory:"
    max_concurrent_queries: int = 10
    connection_pool_size: int | None = None
    heartbeat_interval_sec: int = 10
    shutdown_cancel_queries: bool = True
    cpus: int | None = None
    memory: int | None = None
    disk_space: int | None = None


def worker_connection_pool_size(settings: WorkerSettings) -> int:
    return settings.connection_pool_size or settings.max_concurrent_queries


def load_init_scripts(path: str | None) -> list[str]:
    if not path:
        return []
    content = Path(path).read_text()
    return [stmt.strip() for stmt in content.split(";") if stmt.strip()]
