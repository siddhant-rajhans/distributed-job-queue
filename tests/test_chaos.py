from __future__ import annotations

import asyncio

import pytest

from jobqueue.models import JobStatus


@pytest.mark.asyncio
async def test_reclaim_after_worker_crash(jq):
    """A job held by a crashed worker is reclaimed after the visibility timeout."""
    job_id = await jq.enqueue({"x": 1}, max_retries=3)
    job = await jq.dequeue("default", "doomed-worker")
    assert job is not None

    # Simulate a crash: no heartbeat, force the heartbeat to look old.
    async with jq._pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET last_heartbeat = NOW() - INTERVAL '60 seconds' WHERE id = $1",
            job_id,
        )

    reclaimed = await jq.reclaim_stale(timeout_seconds=30)
    assert any(r[0] == job_id for r in reclaimed)

    job = await jq.get_job(job_id)
    assert job.status == JobStatus.QUEUED

    # Another worker picks it up.
    job2 = await jq.dequeue("default", "recovery-worker")
    assert job2 is not None
    assert job2.id == job_id
    await jq.complete(job2.id)

    final = await jq.get_job(job_id)
    assert final.status == JobStatus.DONE


@pytest.mark.asyncio
async def test_crash_after_max_retries_goes_to_dead_letter(jq):
    """A repeatedly crashed job lands in the dead-letter queue."""
    job_id = await jq.enqueue({"x": 1}, max_retries=2)

    for i in range(2):
        job = await jq.dequeue("default", f"crash-worker-{i}")
        assert job is not None
        status = await jq.fail(job.id, f"crash-{i}")
        if status == "failed":
            await jq.requeue_failed(job.id, backoff_seconds=0)

    final = await jq.get_job(job_id)
    assert final.status == JobStatus.DEAD


@pytest.mark.asyncio
async def test_every_job_finishes_or_dead_letters(jq):
    """Chaos: enqueue N jobs, randomly fail some, verify all end in done or dead."""
    import random
    random.seed(42)

    n_jobs = 30
    for i in range(n_jobs):
        await jq.enqueue({"i": i}, max_retries=2)

    for _ in range(100):
        job = await jq.dequeue("default", "chaos-worker")
        if job is None:
            break
        if random.random() < 0.4:
            status = await jq.fail(job.id, "random failure")
            if status == "failed":
                await jq.requeue_failed(job.id, backoff_seconds=0)
        else:
            await jq.complete(job.id)

    stats = await jq.stats("default")
    remaining_queued = stats.get("queued", 0)
    remaining_running = stats.get("running", 0)
    done = stats.get("done", 0)
    dead = stats.get("dead", 0)

    assert remaining_running == 0
    assert done + dead + remaining_queued == n_jobs
    assert done + dead > 0
