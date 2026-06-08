import json
from uuid import UUID

import asyncpg

QUERY_STATUS_CHANNEL = "query_status"


async def notify_query_status(
    conn: asyncpg.Connection,
    query_id: UUID,
    status: str,
    error: str | None = None,
    result_row_count: int | None = None,
) -> None:
    payload: dict[str, object] = {
        "query_id": str(query_id),
        "status": status,
    }
    if error is not None:
        payload["error"] = error
    if result_row_count is not None:
        payload["result_row_count"] = result_row_count
    await conn.execute(
        "SELECT pg_notify($1, $2)",
        QUERY_STATUS_CHANNEL,
        json.dumps(payload),
    )
