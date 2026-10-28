from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from jobqueue.models import Job, JobStatus

logger = logging.getLogger(__name__)

ENQUEUE_SQL = """
INSERT INTO jobs (queue, payload, priority, scheduled_at, max_retries)
VALUES ($1, $2::jsonb, $3, COALESCE($4, NOW()), $5)
RETURNING id
"""

DEQUEUE_SQL = """
UPDATE jobs SET
    status = 'running',
    started_at = NOW(),
    last_heartbeat = NOW(),
    attempts = attempts + 1,
    worker_id = $2
WHERE id = (
    SELECT id FROM jobs
    WHERE queue = $1
      AND status = 'queued'
      AND scheduled_at <= NOW()
    ORDER BY priority DESC, scheduled_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, queue, payload, status, priority, scheduled_at, started_at,
          completed_at, last_heartbeat, attempts, max_retries, error, worker_id
"""

COMPLETE_SQL = """
UPDATE jobs SET status = 'done', completed_at = NOW()
WHERE id = $1 AND status = 'running'
RETURNING id
"""

FAIL_SQL = """
UPDATE jobs SET
    status = CASE
        WHEN attempts >= max_retries THEN 'dead'
        ELSE 'failed'
    END,
    error = $2,
    completed_at = CASE WHEN attempts >= max_retries THEN NOW() ELSE NULL END
WHERE id = $1 AND status = 'running'
RETURNING id, status
"""

REQUEUE_FAILED_SQL = """
UPDATE jobs SET
    status = 'queued',
    scheduled_at = NOW() + ($2 || ' seconds')::interval,
    worker_id = NULL,
    started_at = NULL,
    last_heartbeat = NULL
WHERE id = $1 AND status = 'failed'
RETURNING id
"""

HEARTBEAT_SQL = """
UPDATE jobs SET last_heartbeat = NOW()
WHERE id = $1 AND status = 'running'
"""

RECLAIM_SQL = """
UPDATE jobs SET
    status = CASE
        WHEN attempts >= max_retries THEN 'dead'
        ELSE 'queued'
    END,
    worker_id = NULL,
    started_at = NULL,
    last_heartbeat = NULL,
    error = COALESCE(error, 'reclaimed: worker heartbeat expired')
WHERE status = 'running'
  AND last_heartbeat < NOW() - ($1 || ' seconds')::interval
RETURNING id, status
"""

GET_JOB_SQL = """
SELECT id, queue, payload, status, priority, scheduled_at, started_at,
       completed_at, last_heartbeat, attempts, max_retries, error, worker_id
FROM jobs WHERE id = $1
"""

STATS_SQL = """
SELECT status, COUNT(*) as count
FROM jobs WHERE queue = $1
GROUP BY status
"""


def _row_to_job(row: asyncpg.Record) -> Job:
    return Job(
        id=row["id"],
        queue=row["queue"],
        payload=json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"],
        status=JobStatus(row["status"]),
        priority=row["priority"],
        scheduled_at=row["scheduled_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        last_heartbeat=row["last_heartbeat"],
        attempts=row["attempts"],
        max_retries=row["max_retries"],
        error=row["error"],
        worker_id=row["worker_id"],
    )


class JobQueue:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def enqueue(
        self,
        payload: dict[str, Any],
        queue: str = "default",
        priority: int = 0,
        scheduled_at: datetime | None = None,
        max_retries: int = 3,
    ) -> int:
        async with self._pool.acquire() as conn:
            job_id = await conn.fetchval(
                ENQUEUE_SQL,
                queue,
                json.dumps(payload),
                priority,
                scheduled_at,
                max_retries,
            )
            logger.info("Enqueued job %d on queue=%s priority=%d", job_id, queue, priority)
            return job_id

    async def dequeue(self, queue: str, worker_id: str) -> Job | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(DEQUEUE_SQL, queue, worker_id)
            if row is None:
                return None
            job = _row_to_job(row)
            logger.info("Dequeued job %d for worker=%s", job.id, worker_id)
            return job

    async def complete(self, job_id: int) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(COMPLETE_SQL, job_id)
            if row:
                logger.info("Completed job %d", job_id)
            return row is not None

    async def fail(self, job_id: int, error: str) -> str:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(FAIL_SQL, job_id, error)
            if row is None:
                return "not_found"
            status = row["status"]
            logger.info("Failed job %d -> %s: %s", job_id, status, error)
            return status

    async def requeue_failed(self, job_id: int, backoff_seconds: float) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(REQUEUE_FAILED_SQL, job_id, str(backoff_seconds))
            return row is not None

    async def heartbeat(self, job_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(HEARTBEAT_SQL, job_id)

    async def reclaim_stale(self, timeout_seconds: float) -> list[tuple[int, str]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(RECLAIM_SQL, str(timeout_seconds))
            for row in rows:
                logger.info("Reclaimed job %d -> %s", row["id"], row["status"])
            return [(row["id"], row["status"]) for row in rows]

    async def get_job(self, job_id: int) -> Job | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(GET_JOB_SQL, job_id)
            return _row_to_job(row) if row else None

    async def stats(self, queue: str = "default") -> dict[str, int]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(STATS_SQL, queue)
            return {row["status"]: row["count"] for row in rows}
