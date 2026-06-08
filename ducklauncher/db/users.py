from uuid import UUID

import asyncpg


async def upsert_user(
    pool: asyncpg.Pool,
    sub: str,
    email: str | None,
    name: str | None,
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO users (sub, email, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (sub) DO UPDATE SET
                email = COALESCE(EXCLUDED.email, users.email),
                name = COALESCE(EXCLUDED.name, users.name),
                last_login_at = now()
            RETURNING user_id, sub, email, name, created_at, last_login_at
            """,
            sub,
            email,
            name,
        )


async def get_user(pool: asyncpg.Pool, user_id: UUID) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT user_id, sub, email, name, created_at, last_login_at
            FROM users
            WHERE user_id = $1
            """,
            user_id,
        )
