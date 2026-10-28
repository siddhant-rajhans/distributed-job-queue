from __future__ import annotations

import pytest

from jobqueue.models import JobStatus


@pytest.mark.asyncio
async def test_enqueue_dequeue(jq):
    job_id = await jq.enqueue({"task": "send_email", "to": "a@b.com"})
    assert job_id > 0

    job = await jq.dequeue("default", "worker-1")
    assert job is not None
    assert job.id == job_id
    assert job.status == JobStatus.RUNNING
    assert job.payload["task"] == "send_email"


@pytest.mark.asyncio
async def test_priority_ordering(jq):
    low = await jq.enqueue({"p": "low"}, priority=0)
    high = await jq.enqueue({"p": "high"}, priority=10)
    mid = await jq.enqueue({"p": "mid"}, priority=5)

    j1 = await jq.dequeue("default", "w")
    j2 = await jq.dequeue("default", "w")
    j3 = await jq.dequeue("default", "w")

    assert j1.id == high
    assert j2.id == mid
    assert j3.id == low


@pytest.mark.asyncio
async def test_complete_marks_done(jq):
    job_id = await jq.enqueue({"x": 1})
    await jq.dequeue("default", "w")
    await jq.complete(job_id)

    job = await jq.get_job(job_id)
    assert job.status == JobStatus.DONE
    assert job.completed_at is not None


@pytest.mark.asyncio
async def test_fail_and_requeue(jq):
    job_id = await jq.enqueue({"x": 1}, max_retries=3)
    await jq.dequeue("default", "w")
    status = await jq.fail(job_id, "boom")
    assert status == "failed"

    await jq.requeue_failed(job_id, backoff_seconds=0)
    job = await jq.get_job(job_id)
    assert job.status == JobStatus.QUEUED


@pytest.mark.asyncio
async def test_dead_letter_after_max_retries(jq):
    job_id = await jq.enqueue({"x": 1}, max_retries=2)

    for attempt in range(2):
        job = await jq.dequeue("default", "w")
        assert job is not None
        status = await jq.fail(job.id, f"error-{attempt}")
        if status == "failed":
            await jq.requeue_failed(job.id, backoff_seconds=0)

    job = await jq.get_job(job_id)
    assert job.status == JobStatus.DEAD
    assert job.attempts == 2


@pytest.mark.asyncio
async def test_empty_queue_returns_none(jq):
    job = await jq.dequeue("default", "w")
    assert job is None


@pytest.mark.asyncio
async def test_queue_isolation(jq):
    await jq.enqueue({"q": "a"}, queue="alpha")
    await jq.enqueue({"q": "b"}, queue="beta")

    ja = await jq.dequeue("alpha", "w")
    jb = await jq.dequeue("beta", "w")
    assert ja.payload["q"] == "a"
    assert jb.payload["q"] == "b"

    assert await jq.dequeue("alpha", "w") is None


@pytest.mark.asyncio
async def test_stats(jq):
    await jq.enqueue({"x": 1})
    await jq.enqueue({"x": 2})
    job = await jq.dequeue("default", "w")
    await jq.complete(job.id)

    s = await jq.stats("default")
    assert s.get("queued", 0) == 1
    assert s.get("done", 0) == 1
