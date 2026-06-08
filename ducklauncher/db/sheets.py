from uuid import UUID

import asyncpg


async def list_sheets(pool: asyncpg.Pool, user_id: UUID) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT sheet_id, user_id, name, sql, created_at, updated_at
            FROM sheets
            WHERE user_id = $1
            ORDER BY updated_at DESC
            """,
            user_id,
        )


async def get_sheet(pool: asyncpg.Pool, sheet_id: UUID, user_id: UUID) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT sheet_id, user_id, name, sql, created_at, updated_at
            FROM sheets
            WHERE sheet_id = $1 AND user_id = $2
            """,
            sheet_id,
            user_id,
        )


async def create_sheet(
    pool: asyncpg.Pool,
    user_id: UUID,
    name: str,
    sql: str,
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            INSERT INTO sheets (user_id, name, sql)
            VALUES ($1, $2, $3)
            RETURNING sheet_id, user_id, name, sql, created_at, updated_at
            """,
            user_id,
            name,
            sql,
        )


async def update_sheet(
    pool: asyncpg.Pool,
    sheet_id: UUID,
    user_id: UUID,
    name: str | None,
    sql: str | None,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            UPDATE sheets
            SET name = COALESCE($3, name),
                sql = COALESCE($4, sql),
                updated_at = now()
            WHERE sheet_id = $1 AND user_id = $2
            RETURNING sheet_id, user_id, name, sql, created_at, updated_at
            """,
            sheet_id,
            user_id,
            name,
            sql,
        )


async def delete_sheet(pool: asyncpg.Pool, sheet_id: UUID, user_id: UUID) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM sheets WHERE sheet_id = $1 AND user_id = $2",
            sheet_id,
            user_id,
        )
    return result.endswith("1")
