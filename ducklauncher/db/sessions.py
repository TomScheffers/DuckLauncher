from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def create_session(
    pool: asyncpg.Pool,
    user_id: UUID,
    ttl_hours: int,
) -> UUID:
    session_id = uuid4()
    expires_at = _utcnow() + timedelta(hours=ttl_hours)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO sessions (session_id, user_id, expires_at)
            VALUES ($1, $2, $3)
            """,
            session_id,
            user_id,
            expires_at,
        )
    return session_id


async def get_session_user(pool: asyncpg.Pool, session_id: UUID) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT u.user_id, u.sub, u.email, u.name, u.created_at, u.last_login_at
            FROM sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.session_id = $1 AND s.expires_at > now()
            """,
            session_id,
        )
        if row is None:
            await conn.execute(
                "DELETE FROM sessions WHERE session_id = $1 AND expires_at <= now()",
                session_id,
            )
        return row


async def delete_session(pool: asyncpg.Pool, session_id: UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM sessions WHERE session_id = $1", session_id)
