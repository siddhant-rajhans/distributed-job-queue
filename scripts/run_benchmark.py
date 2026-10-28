"""Benchmark: measures jobs/s with varying worker counts and verifies no jobs lost.

Run against a Postgres instance:
  python scripts/run_benchmark.py --url postgresql://...
"""
from __future__ import annotations

import asyncio
import os
import random
import time

from jobqueue.db import create_pool
from jobqueue.queue import JobQueue
from jobqueue.worker import Worker


async def run_benchmark(dsn: str, n_jobs: int, n_workers: int):
    pool = await create_pool(dsn)
    jq = JobQueue(pool)

    # Enqueue
    for i in range(n_jobs):
        await jq.enqueue({"i": i}, priority=random.randint(0, 10))

    processed = []
    lock = asyncio.Lock()

    async def handler(payload):
        async with lock:
            processed.append(payload["i"])

    workers = [
        Worker(jq, handler, worker_id=f"bench-{i}")
        for i in range(n_workers)
    ]

    t0 = time.monotonic()
    tasks = [asyncio.create_task(w.start()) for w in workers]

    while len(processed) < n_jobs:
        await asyncio.sleep(0.1)
        elapsed = time.monotonic() - t0
        if elapsed > 60:
            break

    wall_time = time.monotonic() - t0

    for w in workers:
        await w.stop()
    for t in tasks:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    # Check for lost jobs
    stats = await jq.stats("default")
    done = stats.get("done", 0)
    lost = n_jobs - done

    print(f"Workers: {n_workers}")
    print(f"Jobs: {done}/{n_jobs} completed")
    print(f"Wall clock: {wall_time:.3f}s")
    print(f"Throughput: {done / wall_time:.1f} jobs/s")
    print(f"Jobs lost: {lost}")
    print(f"Duplicates: {len(processed) - len(set(processed))}")
    print()

    # Clean up for next run
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM jobs")

    await pool.close()
    return {"workers": n_workers, "jobs_per_sec": done / wall_time, "lost": lost}


async def main():
    dsn = os.environ.get(
        "DATABASE_URL",
        "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue_test",
    )
    n_jobs = int(os.environ.get("BENCHMARK_JOBS", "500"))

    print("=== Distributed Job Queue Benchmark ===\n")
    results = []
    for n_workers in [1, 4, 16]:
        r = await run_benchmark(dsn, n_jobs, n_workers)
        results.append(r)

    print("=== Summary ===")
    print(f"{'Workers':<10} {'Jobs/s':<12} {'Lost':<6}")
    for r in results:
        print(f"{r['workers']:<10} {r['jobs_per_sec']:<12.1f} {r['lost']:<6}")


if __name__ == "__main__":
    asyncio.run(main())
