from __future__ import annotations

import asyncio
import logging
import signal
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from jobqueue.config import settings
from jobqueue.models import Job
from jobqueue.queue import JobQueue

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Awaitable[None]]


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        handler: Handler,
        queue_name: str = "default",
        worker_id: str | None = None,
    ):
        self._queue = queue
        self._handler = handler
        self._queue_name = queue_name
        self._worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._running = False
        self._current_job: Job | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self.jobs_processed = 0
        self.jobs_failed = 0

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def start(self) -> None:
        self._running = True
        logger.info("Worker %s started on queue=%s", self._worker_id, self._queue_name)

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._shutdown)
            except NotImplementedError:
                pass

        while self._running:
            job = await self._queue.dequeue(self._queue_name, self._worker_id)
            if job is None:
                await asyncio.sleep(settings.poll_interval_seconds)
                continue

            self._current_job = job
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(job.id))

            try:
                await self._handler(job.payload)
                await self._queue.complete(job.id)
                self.jobs_processed += 1
                logger.info("Job %d completed by %s", job.id, self._worker_id)
            except Exception as exc:
                new_status = await self._queue.fail(job.id, str(exc))
                self.jobs_failed += 1
                if new_status == "failed":
                    backoff = settings.base_backoff_seconds * (2 ** (job.attempts - 1))
                    await self._queue.requeue_failed(job.id, backoff)
                    logger.warning(
                        "Job %d failed (attempt %d/%d), requeued with %.1fs backoff: %s",
                        job.id, job.attempts, job.max_retries, backoff, exc,
                    )
                else:
                    logger.error(
                        "Job %d moved to dead-letter after %d attempts: %s",
                        job.id, job.attempts, exc,
                    )
            finally:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
                self._current_job = None
                self._heartbeat_task = None

        logger.info("Worker %s stopped", self._worker_id)

    def _shutdown(self) -> None:
        logger.info("Worker %s shutting down", self._worker_id)
        self._running = False

    async def stop(self) -> None:
        self._running = False

    async def _heartbeat_loop(self, job_id: int) -> None:
        while True:
            await asyncio.sleep(settings.heartbeat_interval_seconds)
            try:
                await self._queue.heartbeat(job_id)
            except Exception:
                logger.exception("Heartbeat failed for job %d", job_id)
