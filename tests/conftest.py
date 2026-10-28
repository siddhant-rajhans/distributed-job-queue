from __future__ import annotations

import os

import pytest_asyncio
import asyncpg

from jobqueue.db import SCHEMA_SQL
from jobqueue.queue import JobQueue

TEST_DB_DSN = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue_test",
)


async def _create_pool(dsn: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=20)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    return pool


@pytest_asyncio.fixture
async def pool():
    p = await _create_pool(TEST_DB_DSN)
    yield p
    async with p.acquire() as conn:
        await conn.execute("DELETE FROM jobs")
    await p.close()


@pytest_asyncio.fixture
async def jq(pool) -> JobQueue:
    return JobQueue(pool)
