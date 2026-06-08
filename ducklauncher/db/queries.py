from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg

from ducklauncher.db.notify import notify_query_status


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def register_worker(
    pool: asyncpg.Pool,
    *,
    worker_id: UUID | None,
    endpoint: str,
    cpus: int,
    memory: int,
    disk_space: int,
    max_concurrent_queries: int,
) -> UUID:
    resolved_id = worker_id or uuid4()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO workers (
                worker_id, endpoint, status, cpus, memory, disk_space, max_concurrent_queries
            )
            VALUES ($1, $2, 'running', $3, $4, $5, $6)
            ON CONFLICT (worker_id) DO UPDATE SET
                endpoint = EXCLUDED.endpoint,
                status = 'running',
                cpus = EXCLUDED.cpus,
                memory = EXCLUDED.memory,
                disk_space = EXCLUDED.disk_space,
                max_concurrent_queries = EXCLUDED.max_concurrent_queries,
                last_heartbeat_at = now()
            """,
            resolved_id,
            endpoint,
            cpus,
            memory,
            disk_space,
            max_concurrent_queries,
        )
    return resolved_id


async def heartbeat_worker(
    pool: asyncpg.Pool,
    worker_id: UUID,
    cpus: int | None = None,
    memory: int | None = None,
    disk_space: int | None = None,
    status: str | None = None,
    memory_used_mb: int | None = None,
    cpu_usage: float | None = None,
) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE workers
            SET last_heartbeat_at = now(),
                cpus = COALESCE($2, cpus),
                memory = COALESCE($3, memory),
                disk_space = COALESCE($4, disk_space),
                status = COALESCE($5, status),
                memory_used_mb = COALESCE($6, memory_used_mb),
                cpu_usage = COALESCE($7, cpu_usage)
            WHERE worker_id = $1
            """,
            worker_id,
            cpus,
            memory,
            disk_space,
            status,
            memory_used_mb,
            cpu_usage,
        )
    return result.endswith("1")


async def list_workers(pool: asyncpg.Pool) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT
                w.worker_id,
                w.endpoint,
                w.status,
                w.cpus,
                w.memory,
                w.disk_space,
                w.max_concurrent_queries,
                w.memory_used_mb,
                w.cpu_usage,
                w.started_at,
                w.last_heartbeat_at,
                COUNT(q.query_id) FILTER (WHERE q.status = 'running') AS running_queries
            FROM workers w
            LEFT JOIN queries q ON q.worker_id = w.worker_id
            WHERE w.status IN ('running', 'shutting_down')
            GROUP BY w.worker_id
            ORDER BY w.started_at
            """
        )


async def shutdown_worker(pool: asyncpg.Pool, worker_id: UUID) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE workers
            SET status = 'shutting_down', last_heartbeat_at = now()
            WHERE worker_id = $1 AND status = 'running'
            """,
            worker_id,
        )
    return result.endswith("1")


async def create_query(
    pool: asyncpg.Pool,
    *,
    query: str,
    cpus: int | None,
    memory: int | None,
    disk_space: int | None,
) -> UUID:
    async with pool.acquire() as conn:
        query_id = await conn.fetchval(
            """
            INSERT INTO queries (query, cpus, memory, disk_space)
            VALUES ($1, $2, $3, $4)
            RETURNING query_id
            """,
            query,
            cpus,
            memory,
            disk_space,
        )
    return query_id


async def get_query(pool: asyncpg.Pool, query_id: UUID) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT query_id, worker_id, status, query, error, cpus, memory, disk_space,
                   created_at, started_at, completed_at, result_row_count
            FROM queries
            WHERE query_id = $1
            """,
            query_id,
        )


async def complete_query(
    pool: asyncpg.Pool,
    query_id: UUID,
    status: str,
    error: str | None = None,
    result_row_count: int | None = None,
) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE queries
            SET status = $2,
                error = COALESCE($3, error),
                result_row_count = COALESCE($4, result_row_count),
                completed_at = COALESCE(completed_at, now())
            WHERE query_id = $1 AND status IN ('running', 'pending')
            """,
            query_id,
            status,
            error,
            result_row_count,
        )
        if result.endswith("1"):
            await notify_query_status(
                conn,
                query_id,
                status,
                error=error,
                result_row_count=result_row_count,
            )
            return True
        existing = await conn.fetchval(
            "SELECT status FROM queries WHERE query_id = $1",
            query_id,
        )
        if existing == status:
            await notify_query_status(
                conn,
                query_id,
                status,
                error=error,
                result_row_count=result_row_count,
            )
    return existing == status


async def mark_query_cancelled(pool: asyncpg.Pool, query_id: UUID, error: str | None = None) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE queries
            SET status = 'cancelled',
                error = COALESCE($2, error),
                completed_at = now()
            WHERE query_id = $1 AND status IN ('pending', 'running')
            RETURNING query_id, worker_id, status, error
            """,
            query_id,
            error,
        )
        if row is not None:
            await notify_query_status(conn, query_id, "cancelled", error=row["error"])
        return row


async def revert_query_to_pending(pool: asyncpg.Pool, query_id: UUID) -> None:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE queries
            SET status = 'pending',
                worker_id = NULL,
                started_at = NULL
            WHERE query_id = $1 AND status = 'running'
            """,
            query_id,
        )
        if result.endswith("1"):
            await notify_query_status(conn, query_id, "pending")


_WORKER_FOR_QUERY_SQL = """
    WITH worker_load AS (
        SELECT
            worker_id,
            COUNT(*) AS queries,
            COALESCE(SUM(cpus), 0) AS cpus,
            COALESCE(SUM(memory), 0) AS memory,
            COALESCE(SUM(disk_space), 0) AS disk_space
        FROM queries
        WHERE status = 'running'
        GROUP BY worker_id
    )
    SELECT w.worker_id, w.endpoint, w.cpus, w.memory, w.disk_space
    FROM workers w
    LEFT JOIN worker_load wl ON wl.worker_id = w.worker_id
    WHERE w.status = 'running'
      AND w.last_heartbeat_at >= $1
      AND ($2::int IS NULL OR COALESCE(wl.cpus, 0) + $2 <= w.cpus)
      AND ($3::int IS NULL OR COALESCE(wl.memory, 0) + $3 <= w.memory)
      AND ($4::int IS NULL OR COALESCE(wl.disk_space, 0) + $4 <= w.disk_space)
      AND COALESCE(wl.queries, 0) + 1 <= w.max_concurrent_queries
    ORDER BY COALESCE(wl.queries, 0), w.last_heartbeat_at DESC
    LIMIT 1
"""


async def _claim_query(
    conn: asyncpg.Connection,
    pending: asyncpg.Record,
    *,
    stale_cutoff: datetime,
) -> asyncpg.Record | None:
    worker = await conn.fetchrow(
        _WORKER_FOR_QUERY_SQL,
        stale_cutoff,
        pending["cpus"],
        pending["memory"],
        pending["disk_space"],
    )
    if worker is None:
        return None

    await conn.execute(
        """
        UPDATE queries
        SET status = 'running',
            worker_id = $2,
            started_at = now()
        WHERE query_id = $1
        """,
        pending["query_id"],
        worker["worker_id"],
    )
    await notify_query_status(conn, pending["query_id"], "running")
    return await conn.fetchrow(
        """
        SELECT q.query_id, q.query, q.cpus, q.memory, q.disk_space,
               w.worker_id, w.endpoint
        FROM queries q
        JOIN workers w ON w.worker_id = q.worker_id
        WHERE q.query_id = $1
        """,
        pending["query_id"],
    )


async def claim_pending_query(
    pool: asyncpg.Pool,
    *,
    worker_stale_sec: int,
) -> asyncpg.Record | None:
    stale_cutoff = _utcnow() - timedelta(seconds=worker_stale_sec)
    async with pool.acquire() as conn:
        async with conn.transaction():
            pending = await conn.fetchrow(
                """
                SELECT query_id, query, cpus, memory, disk_space
                FROM queries
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            if pending is None:
                return None
            return await _claim_query(conn, pending, stale_cutoff=stale_cutoff)


async def claim_query_by_id(
    pool: asyncpg.Pool,
    *,
    query_id: UUID,
    worker_stale_sec: int,
) -> asyncpg.Record | None:
    stale_cutoff = _utcnow() - timedelta(seconds=worker_stale_sec)
    async with pool.acquire() as conn:
        async with conn.transaction():
            pending = await conn.fetchrow(
                """
                SELECT query_id, query, cpus, memory, disk_space
                FROM queries
                WHERE query_id = $1 AND status = 'pending'
                FOR UPDATE SKIP LOCKED
                """,
                query_id,
            )
            if pending is None:
                return None
            return await _claim_query(conn, pending, stale_cutoff=stale_cutoff)


async def sweep_stale_workers(pool: asyncpg.Pool, worker_stale_sec: int) -> None:
    stale_cutoff = _utcnow() - timedelta(seconds=worker_stale_sec)
    async with pool.acquire() as conn:
        async with conn.transaction():
            stale_workers = await conn.fetch(
                """
                UPDATE workers
                SET status = 'stopped'
                WHERE status IN ('running', 'shutting_down')
                  AND last_heartbeat_at < $1
                RETURNING worker_id
                """,
                stale_cutoff,
            )
            if not stale_workers:
                return
            worker_ids = [row["worker_id"] for row in stale_workers]
            reverted = await conn.fetch(
                """
                UPDATE queries
                SET status = 'pending',
                    worker_id = NULL,
                    started_at = NULL,
                    error = 'Worker became unreachable'
                WHERE worker_id = ANY($1::uuid[]) AND status = 'running'
                RETURNING query_id, error
                """,
                worker_ids,
            )
            for row in reverted:
                await notify_query_status(
                    conn,
                    row["query_id"],
                    "pending",
                    error=row["error"],
                )
