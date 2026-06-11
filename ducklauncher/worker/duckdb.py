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
        self._admin_lock = threading.Lock()
        self._available: queue.Queue[duckdb.DuckDBPyConnection] = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._active: dict[UUID, duckdb.DuckDBPyConnection] = {}

    def warm_pool(self) -> None:
        logger.info("Warming DuckDB connection pool (%d connections)", self._pool_size)
        for _ in range(self._pool_size):
            self._available.put(duckdb.connect(self._database_path))

    def run_init_scripts(self, scripts: list[str]) -> None:
        with self._admin_lock:
            for script in scripts:
                logger.info("Running init script")
                self._admin_conn.execute(script)

    def _fetch_scalar(self, sql: str, params: list | None = None) -> object:
        if params is None:
            relation = self._admin_conn.execute(sql)
        else:
            relation = self._admin_conn.execute(sql, params)
        row = relation.fetchone()
        if row is None:
            raise ValueError("Query returned no rows")
        return row[0]

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

    @staticmethod
    def _quote_identifier(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def introspect_catalog(self) -> dict:
        with self._admin_lock:
            rows = self._admin_conn.execute(
                """
                SELECT
                    table_catalog AS database_name,
                    table_schema AS schema_name,
                    table_name,
                    column_name,
                    data_type,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY
                    database_name,
                    schema_name,
                    table_name,
                    ordinal_position
                """
            ).fetchall()

        databases: dict[str, dict[str, dict[str, list[dict[str, str | int]]]]] = {}
        database_order: list[str] = []
        schema_order: dict[str, list[str]] = {}
        table_order: dict[tuple[str, str], list[str]] = {}

        for database_name, schema_name, table_name, column_name, data_type, ordinal_position in rows:
            if database_name not in databases:
                databases[database_name] = {}
                database_order.append(database_name)
            db = databases[database_name]

            if schema_name not in db:
                db[schema_name] = {}
                schema_order.setdefault(database_name, []).append(schema_name)

            schema = db[schema_name]
            table_key = (database_name, schema_name)
            if table_name not in schema:
                schema[table_name] = []
                table_order.setdefault(table_key, []).append(table_name)

            schema[table_name].append(
                {"name": column_name, "type": data_type, "ordinal_position": ordinal_position}
            )

        return {
            "databases": [
                {
                    "name": database_name,
                    "schemas": [
                        {
                            "name": schema_name,
                            "tables": [
                                {
                                    "name": table_name,
                                    "columns": [
                                        {"name": column["name"], "type": column["type"]}
                                        for column in sorted(
                                            databases[database_name][schema_name][table_name],
                                            key=lambda column: column["ordinal_position"],
                                        )
                                    ],
                                }
                                for table_name in table_order.get((database_name, schema_name), [])
                            ],
                        }
                        for schema_name in schema_order.get(database_name, [])
                    ],
                }
                for database_name in database_order
            ]
        }

    def read_result_page(self, query_id: UUID, offset: int, limit: int) -> dict:
        path = self.result_path(query_id)
        if not path.exists():
            raise FileNotFoundError(f"No result file for query {query_id}")
        path_str = str(path)
        with self._admin_lock:
            try:
                total_rows = self._fetch_scalar(
                    "SELECT count(*) FROM read_parquet(?)",
                    [path_str],
                )
                preview = self._admin_conn.execute(
                    "SELECT * FROM read_parquet(?) LIMIT 0",
                    [path_str],
                )
                columns = [col[0] for col in preview.description]
                quoted_columns = ", ".join(self._quote_identifier(column) for column in columns)
                rows = self._admin_conn.execute(
                    f"""
                    SELECT {quoted_columns}
                    FROM (
                        SELECT
                            {quoted_columns},
                            row_number() OVER (ORDER BY (SELECT 1)) AS __ducklauncher_row__
                        FROM read_parquet(?)
                    )
                    ORDER BY __ducklauncher_row__
                    LIMIT ? OFFSET ?
                    """,
                    [path_str, limit, offset],
                ).fetchall()
            except duckdb.Error as exc:
                raise ValueError(f"Failed to read result file for query {query_id}: {exc}") from exc
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
