from __future__ import annotations

import asyncio

import pytest

from jobqueue.models import JobStatus
from jobqueue.worker import Worker


@pytest.mark.asyncio
async def test_worker_processes_jobs(jq):
    processed = []

    async def handler(payload):
        processed.append(payload)

    for i in range(5):
        await jq.enqueue({"i": i})

    worker = Worker(jq, handler, worker_id="test-worker")
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(1.5)
    await worker.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(processed) == 5
    assert worker.jobs_processed == 5
    assert worker.jobs_failed == 0


@pytest.mark.asyncio
async def test_worker_retries_on_failure(jq):
    attempts = []

    async def flaky_handler(payload):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("transient error")

    await jq.enqueue({"x": 1}, max_retries=3)

    worker = Worker(jq, flaky_handler, worker_id="retry-worker")
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(6.0)
    await worker.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(attempts) == 3
    assert worker.jobs_processed == 1


@pytest.mark.asyncio
async def test_worker_dead_letters_persistent_failure(jq):
    async def always_fail(payload):
        raise RuntimeError("permanent error")

    await jq.enqueue({"x": 1}, max_retries=2)

    worker = Worker(jq, always_fail, worker_id="fail-worker")
    task = asyncio.create_task(worker.start())
    await asyncio.sleep(3.0)
    await worker.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    stats = await jq.stats("default")
    assert stats.get("dead", 0) >= 1
