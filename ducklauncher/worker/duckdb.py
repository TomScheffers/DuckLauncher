import logging
import queue
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import duckdb

logger = logging.getLogger(__name__)

_RESULT_QUERY = re.compile(r"^\s*(with|select|describe|show|explain|values)\b", re.IGNORECASE)


@dataclass
class QueryExecutionResult:
    result_row_count: int | None = None


class DuckDBExecutor:
    def __init__(self, database_path: str, pool_size: int, result_dir: Path) -> None:
        self._database_path = database_path
        self._pool_size = pool_size
        self._result_dir = result_dir
        self._result_dir.mkdir(parents=True, exist_ok=True)
        self._admin_conn = duckdb.connect(database_path)
        self._available: queue.Queue[duckdb.DuckDBPyConnection] = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._active: dict[UUID, duckdb.DuckDBPyConnection] = {}

    def warm_pool(self) -> None:
        logger.info("Warming DuckDB connection pool (%d connections)", self._pool_size)
        for _ in range(self._pool_size):
            self._available.put(duckdb.connect(self._database_path))

    def run_init_scripts(self, scripts: list[str]) -> None:
        for script in scripts:
            logger.info("Running init script")
            self._admin_conn.execute(script)

    def result_path(self, query_id: UUID) -> Path:
        return self._result_dir / f"{query_id}.parquet"

    def _acquire(self) -> duckdb.DuckDBPyConnection:
        return self._available.get()

    def _release(self, conn: duckdb.DuckDBPyConnection, discard: bool = False) -> None:
        if discard:
            try:
                conn.close()
            except Exception:
                pass
            conn = duckdb.connect(self._database_path)
        self._available.put(conn)

    def _returns_results(self, query: str) -> bool:
        stripped = query.strip().rstrip(";").strip()
        if ";" in stripped:
            return False
        return bool(_RESULT_QUERY.match(stripped))

    def execute(self, query_id: UUID, query: str) -> QueryExecutionResult:
        conn = self._acquire()
        discard = False
        with self._lock:
            self._active[query_id] = conn
        try:
            if self._returns_results(query):
                result_path = self.result_path(query_id)
                conn.execute(
                    f"COPY ({query}) TO '{result_path}' (FORMAT PARQUET)"
                )
                row_count = conn.execute(
                    "SELECT count(*) FROM read_parquet(?)",
                    [str(result_path)],
                ).fetchone()[0]
                return QueryExecutionResult(result_row_count=row_count)
            conn.execute(query)
            return QueryExecutionResult()
        except duckdb.InterruptException as exc:
            discard = True
            self._remove_result(query_id)
            raise QueryCancelledError("Query was cancelled") from exc
        except Exception:
            self._remove_result(query_id)
            raise
        finally:
            with self._lock:
                self._active.pop(query_id, None)
            self._release(conn, discard=discard)

    def _remove_result(self, query_id: UUID) -> None:
        path = self.result_path(query_id)
        if path.exists():
            path.unlink()

    def read_result_page(self, query_id: UUID, offset: int, limit: int) -> dict:
        path = self.result_path(query_id)
        if not path.exists():
            raise FileNotFoundError(f"No result file for query {query_id}")
        total_rows = self._admin_conn.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(path)],
        ).fetchone()[0]
        preview = self._admin_conn.execute(
            "SELECT * FROM read_parquet(?) LIMIT 0",
            [str(path)],
        )
        columns = [col[0] for col in preview.description]
        rows = self._admin_conn.execute(
            "SELECT * FROM read_parquet(?) LIMIT ? OFFSET ?",
            [str(path), limit, offset],
        ).fetchall()
        return {
            "total_rows": total_rows,
            "columns": columns,
            "rows": [list(row) for row in rows],
        }

    def cancel(self, query_id: UUID) -> bool:
        with self._lock:
            conn = self._active.get(query_id)
        if conn is None:
            return False
        logger.info("Interrupting DuckDB query %s", query_id)
        conn.interrupt()
        return True

    def cancel_all(self) -> list[UUID]:
        with self._lock:
            query_ids = list(self._active.keys())
        for query_id in query_ids:
            self.cancel(query_id)
        return query_ids

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    def close(self) -> None:
        with self._lock:
            for conn in self._active.values():
                try:
                    conn.close()
                except Exception:
                    pass
            self._active.clear()
            while True:
                try:
                    conn = self._available.get_nowait()
                except queue.Empty:
                    break
                try:
                    conn.close()
                except Exception:
                    pass
        self._admin_conn.close()


class QueryCancelledError(Exception):
    pass
