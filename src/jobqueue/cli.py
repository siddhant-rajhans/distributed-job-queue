from __future__ import annotations

import asyncio
import json

import click
from rich.console import Console
from rich.table import Table

from jobqueue.config import settings
from jobqueue.db import create_pool
from jobqueue.queue import JobQueue

console = Console()


@click.group()
def cli():
    """Distributed Job Queue CLI."""
    pass


@cli.command()
@click.option("--queue", default="default")
@click.option("--payload", required=True, help="JSON payload")
@click.option("--priority", default=0)
@click.option("--max-retries", default=3)
def enqueue(queue: str, payload: str, priority: int, max_retries: int):
    """Enqueue a new job."""
    async def _run():
        pool = await create_pool(settings.database_url)
        jq = JobQueue(pool)
        job_id = await jq.enqueue(
            json.loads(payload), queue=queue, priority=priority, max_retries=max_retries,
        )
        console.print(f"Enqueued job [bold]{job_id}[/bold] on queue={queue}")
        await pool.close()

    asyncio.run(_run())


@cli.command()
@click.option("--queue", default="default")
def stats(queue: str):
    """Show queue statistics."""
    async def _run():
        pool = await create_pool(settings.database_url)
        jq = JobQueue(pool)
        data = await jq.stats(queue)
        table = Table(title=f"Queue: {queue}")
        table.add_column("Status")
        table.add_column("Count", justify="right")
        for status, count in sorted(data.items()):
            table.add_row(status, str(count))
        console.print(table)
        await pool.close()

    asyncio.run(_run())


@cli.command()
@click.argument("job_id", type=int)
def status(job_id: int):
    """Show job status."""
    async def _run():
        pool = await create_pool(settings.database_url)
        jq = JobQueue(pool)
        job = await jq.get_job(job_id)
        if job is None:
            console.print(f"[red]Job {job_id} not found[/red]")
        else:
            console.print(f"Job [bold]{job.id}[/bold]")
            console.print(f"  Status:   {job.status.value}")
            console.print(f"  Queue:    {job.queue}")
            console.print(f"  Priority: {job.priority}")
            console.print(f"  Attempts: {job.attempts}/{job.max_retries}")
            if job.error:
                console.print(f"  Error:    {job.error}")
        await pool.close()

    asyncio.run(_run())


@cli.command()
@click.option("--timeout", default=30.0, help="Visibility timeout in seconds")
def reclaim(timeout: float):
    """Reclaim jobs from dead workers."""
    async def _run():
        pool = await create_pool(settings.database_url)
        jq = JobQueue(pool)
        reclaimed = await jq.reclaim_stale(timeout)
        for job_id, new_status in reclaimed:
            console.print(f"Reclaimed job {job_id} -> {new_status}")
        if not reclaimed:
            console.print("No stale jobs to reclaim")
        await pool.close()

    asyncio.run(_run())


if __name__ == "__main__":
    cli()
