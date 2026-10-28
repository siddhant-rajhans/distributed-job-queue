from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_no_double_processing(jq):
    """Multiple workers pulling concurrently must never get the same job."""
    n_jobs = 50
    for i in range(n_jobs):
        await jq.enqueue({"i": i})

    claimed: list[int] = []
    lock = asyncio.Lock()

    async def worker(wid: str):
        while True:
            job = await jq.dequeue("default", wid)
            if job is None:
                break
            async with lock:
                claimed.append(job.id)
            await jq.complete(job.id)

    await asyncio.gather(*(worker(f"w-{i}") for i in range(8)))

    assert len(claimed) == n_jobs
    assert len(set(claimed)) == n_jobs, "Duplicate job processing detected"


@pytest.mark.asyncio
async def test_concurrent_enqueue_dequeue(jq):
    """Producers and consumers running simultaneously."""
    n = 100
    produced = []
    consumed = []
    lock = asyncio.Lock()

    async def producer():
        for i in range(n):
            jid = await jq.enqueue({"i": i})
            produced.append(jid)

    async def consumer(wid: str):
        while True:
            job = await jq.dequeue("default", wid)
            if job is None:
                await asyncio.sleep(0.05)
                job = await jq.dequeue("default", wid)
                if job is None:
                    break
            async with lock:
                consumed.append(job.id)
            await jq.complete(job.id)

    await asyncio.gather(
        producer(),
        *(consumer(f"c-{i}") for i in range(4)),
    )

    assert len(consumed) == n
    assert len(set(consumed)) == n
