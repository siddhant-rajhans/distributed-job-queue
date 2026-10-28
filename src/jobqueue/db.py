from __future__ import annotations

import asyncpg

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT NOT NULL DEFAULT 'default',
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'done', 'failed', 'dead')),
    priority        INT NOT NULL DEFAULT 0,
    scheduled_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    last_heartbeat  TIMESTAMPTZ,
    attempts        INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    error           TEXT,
    worker_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_dequeue
    ON jobs (priority DESC, scheduled_at ASC)
    WHERE status = 'queued';

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (queue);
"""


async def create_pool(dsn: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=20)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    return pool
