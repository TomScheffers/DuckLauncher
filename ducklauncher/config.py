import os
import re
from pathlib import Path
from uuid import UUID, uuid4

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_VAR = re.compile(r"\$\{([^}:]+)(?::-([^}]*))?\}")

WORKER_DATA_ROOT = Path("/tmp/ducklauncher/workers")


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
    result_dir: Path = Path("/tmp/ducklauncher/results")


def default_worker_id_path(port: int) -> Path:
    return Path.home() / ".ducklauncher" / "workers" / str(port) / "worker_id"


def resolve_local_worker_id(settings: WorkerSettings) -> UUID:
    if settings.worker_id:
        return UUID(settings.worker_id)
    if settings.worker_id_path.exists():
        return UUID(settings.worker_id_path.read_text().strip())
    return uuid4()


def resolve_worker_storage(settings: WorkerSettings, worker_id: UUID) -> tuple[str, Path]:
    worker_dir = WORKER_DATA_ROOT / str(worker_id)
    worker_dir.mkdir(parents=True, exist_ok=True)

    if settings.duckdb_path == ":memory:":
        duckdb_path = ":memory:"
    else:
        duckdb_path = str(worker_dir / Path(settings.duckdb_path).name)

    if settings.result_dir == Path("/tmp/ducklauncher/results"):
        result_dir = worker_dir / "results"
    else:
        result_dir = settings.result_dir / str(worker_id)
    result_dir.mkdir(parents=True, exist_ok=True)
    return duckdb_path, result_dir


def worker_connection_pool_size(settings: WorkerSettings) -> int:
    return settings.connection_pool_size or settings.max_concurrent_queries


def default_init_scripts_path() -> Path | None:
    path = Path("init.sql")
    if path.is_file():
        return path.resolve()
    return None


def resolve_init_scripts_path(explicit: str | Path | None = None) -> str | None:
    if explicit is not None:
        return str(explicit)
    default = default_init_scripts_path()
    return str(default) if default is not None else None


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _env_default(name: str, inline_default: str | None) -> str:
    if name in os.environ:
        return os.environ[name]
    if name == "ICEBERG_OAUTH_URI":
        endpoint = os.environ.get("ICEBERG_ENDPOINT", inline_default or "")
        if not endpoint:
            raise KeyError(name)
        return f"{endpoint.rstrip('/')}/v1/oauth/tokens"
    if inline_default is not None:
        return inline_default
    raise KeyError(name)


def expand_init_sql(content: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        name = match.group(1)
        inline_default = match.group(2)
        try:
            value = _env_default(name, inline_default)
        except KeyError:
            return match.group(0)
        return _sql_escape(value)

    return _ENV_VAR.sub(replacer, content)


def _executable_sql(statement: str) -> bool:
    for line in statement.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return True
    return False


def cache_httpfs_enabled() -> bool:
    return os.environ.get("CACHE_HTTPFS_ENABLED", "1").lower() not in ("0", "false", "no")


def load_init_scripts(path: str | None) -> list[str]:
    resolved = resolve_init_scripts_path(path)
    if not resolved:
        return []
    content = expand_init_sql(Path(resolved).read_text())
    statements = [stmt.strip() for stmt in content.split(";") if _executable_sql(stmt)]
    if not cache_httpfs_enabled():
        statements = [stmt for stmt in statements if "cache_httpfs" not in stmt.lower()]
    return statements
