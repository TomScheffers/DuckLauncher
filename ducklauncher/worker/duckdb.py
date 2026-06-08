import logging
import queue
import threading
from uuid import UUID

import duckdb

logger = logging.getLogger(__name__)


class DuckDBExecutor:
    def __init__(self, database_path: str, *, pool_size: int) -> None:
        self._database_path = database_path
        self._pool_size = pool_size
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

    def _acquire(self) -> duckdb.DuckDBPyConnection:
        return self._available.get()

    def _release(self, conn: duckdb.DuckDBPyConnection, *, discard: bool = False) -> None:
        if discard:
            try:
                conn.close()
            except Exception:
                pass
            conn = duckdb.connect(self._database_path)
        self._available.put(conn)

    def execute(self, query_id: UUID, query: str) -> None:
        conn = self._acquire()
        discard = False
        with self._lock:
            self._active[query_id] = conn
        try:
            conn.execute(query)
        except duckdb.InterruptException as exc:
            discard = True
            raise QueryCancelledError("Query was cancelled") from exc
        finally:
            with self._lock:
                self._active.pop(query_id, None)
            self._release(conn, discard=discard)

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
