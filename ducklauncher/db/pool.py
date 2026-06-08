from importlib import resources

import asyncpg


def _migrations_sql() -> str:
    return resources.files("ducklauncher.utils").joinpath("migrations.sql").read_text()


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url, min_size=1, max_size=10)


async def run_migrations(pool: asyncpg.Pool) -> None:
    sql = _migrations_sql()
    async with pool.acquire() as conn:
        await conn.execute(sql)


async def init_database(database_url: str) -> None:
    pool = await create_pool(database_url)
    try:
        await run_migrations(pool)
    finally:
        await pool.close()
