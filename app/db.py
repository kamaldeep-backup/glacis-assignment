from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from app.config import Settings, get_settings


class Database:
    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool has not been initialized")
        return self._pool

    async def connect(self) -> None:
        if self._pool is not None:
            return

        self._pool = await asyncpg.create_pool(
            dsn=self._settings.database_url,
            min_size=self._settings.database_min_pool_size,
            max_size=self._settings.database_max_pool_size,
        )

    async def close(self) -> None:
        if self._pool is None:
            return

        await self._pool.close()
        self._pool = None

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection:
            yield connection


database = Database()
