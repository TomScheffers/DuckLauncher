import asyncio
import json
import logging
from collections import defaultdict
from uuid import UUID

import asyncpg

from ducklauncher.db.notify import QUERY_STATUS_CHANNEL

logger = logging.getLogger(__name__)

TERMINAL_QUERY_STATUSES = frozenset({"completed", "failed", "cancelled"})


class QueryEventHub:
    def __init__(self) -> None:
        self._subscribers: dict[UUID, list[asyncio.Queue]] = defaultdict(list)

    def subscribe(self, query_id: UUID) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers[query_id].append(queue)
        return queue

    def unsubscribe(self, query_id: UUID, queue: asyncio.Queue) -> None:
        subscribers = self._subscribers.get(query_id)
        if not subscribers:
            return
        try:
            subscribers.remove(queue)
        except ValueError:
            return
        if not subscribers:
            self._subscribers.pop(query_id, None)

    def dispatch(self, payload: dict) -> None:
        try:
            query_id = UUID(str(payload["query_id"]))
        except (KeyError, ValueError, TypeError):
            logger.warning("Ignoring invalid query notify payload: %s", payload)
            return
        for queue in list(self._subscribers.get(query_id, [])):
            queue.put_nowait(payload)


async def postgres_listen_loop(
    database_url: str,
    hub: QueryEventHub,
    stop_event: asyncio.Event,
) -> None:
    conn = await asyncpg.connect(database_url)

    def on_notify(
        _connection: asyncpg.Connection,
        _pid: int,
        _channel: str,
        payload: str,
    ) -> None:
        try:
            hub.dispatch(json.loads(payload))
        except json.JSONDecodeError:
            logger.warning("Ignoring non-JSON query notify payload: %s", payload)

    await conn.add_listener(QUERY_STATUS_CHANNEL, on_notify)
    logger.info("Listening for PostgreSQL notifications on %s", QUERY_STATUS_CHANNEL)
    try:
        await stop_event.wait()
    finally:
        await conn.remove_listener(QUERY_STATUS_CHANNEL, on_notify)
        await conn.close()


def query_event_payload(row: asyncpg.Record) -> dict:
    return {
        "query_id": str(row["query_id"]),
        "status": row["status"],
        "error": row["error"],
        "result_row_count": row["result_row_count"],
    }


def format_sse(event: dict) -> str:
    return f"event: status\ndata: {json.dumps(event)}\n\n"
