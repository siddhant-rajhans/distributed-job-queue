# Distributed Job Queue

Postgres-backed distributed job queue using `SELECT ... FOR UPDATE SKIP LOCKED` for contention-free dequeuing, exponential backoff retries, dead-letter handling, worker heartbeats, and crash recovery.

## Measured Results

| Metric | Value |
|---|---|
| Throughput (1 worker) | 276 jobs/s |
| Throughput (4 workers) | 712 jobs/s |
| Throughput (16 workers) | 822 jobs/s |
| Jobs lost | 0 / 500 per run |
| Double-processed | 0 |
| Tests | 16/16 passing |
| Test categories | unit, concurrency, chaos, worker |

## Architecture

```mermaid
graph LR
    subgraph Producers
        P1[CLI / App] -->|enqueue| Q[(Postgres<br>jobs table)]
    end
    subgraph Workers
        W1[Worker 1] -->|SKIP LOCKED dequeue| Q
        W2[Worker 2] -->|SKIP LOCKED dequeue| Q
        W3[Worker N] -->|SKIP LOCKED dequeue| Q
    end
    Q -->|heartbeat timeout| R[Reclaim]
    R -->|requeue or dead-letter| Q
    W1 -->|fail + backoff| Q
    W1 -->|complete| Q
```

## Design Decisions

### 1. SKIP LOCKED over advisory locks

`FOR UPDATE SKIP LOCKED` atomically claims a job row without blocking other workers. No external lock manager, no lock table bloat, no risk of lock leaks. The dequeue query updates `status = 'running'` in the same statement that acquires the lock, so there is no window between locking and claiming.

### 2. Exponential backoff with dead-letter

Failed jobs are requeued with exponential backoff (`base * 2^(attempt-1)`). After `max_retries` exhausted, jobs move to `dead` status instead of cycling forever. This prevents thundering-herd retries while ensuring persistent failures are surfaced and stop consuming worker cycles.

### 3. Heartbeat-based crash recovery

Each worker sends periodic heartbeats while processing a job. A reclaimer process scans for `running` jobs whose `last_heartbeat` exceeds the visibility timeout and requeues them (or dead-letters if retries are exhausted). This handles worker crashes, network partitions, and OOM kills without requiring external supervision.

### 4. Priority + scheduled_at ordering

The dequeue query orders by `priority DESC, scheduled_at ASC`, backed by a partial index on `status = 'queued'`. Higher priority jobs run first; within the same priority, FIFO ordering is preserved. The partial index keeps the dequeue path fast even with millions of completed rows in the table.

### 5. Raw SQL over an ORM

The queue uses raw `asyncpg` queries with hand-written SQL. The critical dequeue path is a single atomic UPDATE...RETURNING statement. An ORM would add abstraction over the exact locking and ordering semantics that define correctness here. The SQL is version-controlled and tested directly.

## API

| Operation | Description |
|---|---|
| `enqueue(payload, queue, priority, scheduled_at, max_retries)` | Add a job to the queue |
| `dequeue(queue, worker_id)` | Claim the next job (SKIP LOCKED) |
| `complete(job_id)` | Mark job as done |
| `fail(job_id, error)` | Mark job as failed, auto dead-letter if retries exhausted |
| `requeue_failed(job_id, backoff_seconds)` | Requeue a failed job with delay |
| `heartbeat(job_id)` | Update last_heartbeat timestamp |
| `reclaim_stale(timeout_seconds)` | Reclaim jobs from dead workers |
| `stats(queue)` | Get status counts for a queue |

## CLI

```bash
# Enqueue a job
jobqueue enqueue --payload '{"url": "https://example.com"}' --priority 5

# Check queue stats
jobqueue stats --queue default

# Check a specific job
jobqueue status 42

# Reclaim stale jobs (workers that crashed)
jobqueue reclaim --timeout 30
```

## Quick Start

```bash
# Start Postgres
docker compose up -d db

# Install
pip install -e ".[dev]"

# Run tests
make test

# Run benchmark
make bench
```

### Without Docker

```bash
# Set your Postgres connection
export JQ_DATABASE_URL="postgresql://user:pass@localhost:5432/jobqueue"

# Create the schema automatically on first pool creation
python3 -c "
import asyncio
from jobqueue.db import create_pool
asyncio.run(create_pool('$JQ_DATABASE_URL'))
print('Schema created')
"
```

### Programmatic Usage

```python
import asyncio
from jobqueue.db import create_pool
from jobqueue.queue import JobQueue
from jobqueue.worker import Worker

async def main():
    pool = await create_pool("postgresql://localhost/jobqueue")
    jq = JobQueue(pool)

    # Enqueue work
    await jq.enqueue({"url": "https://example.com"}, priority=5)

    # Process jobs
    async def handler(payload):
        print(f"Processing: {payload}")

    worker = Worker(jq, handler, worker_id="w1")
    await worker.start()

asyncio.run(main())
```

## Configuration

All settings use the `JQ_` environment variable prefix:

| Variable | Default | Description |
|---|---|---|
| `JQ_DATABASE_URL` | `postgresql://jobqueue:jobqueue@localhost:5432/jobqueue` | Postgres connection string |
| `JQ_MAX_RETRIES` | 3 | Default max retries per job |
| `JQ_BASE_BACKOFF_SECONDS` | 1.0 | Base delay for exponential backoff |
| `JQ_VISIBILITY_TIMEOUT_SECONDS` | 30.0 | Heartbeat timeout before reclaim |
| `JQ_HEARTBEAT_INTERVAL_SECONDS` | 10.0 | How often workers send heartbeats |
| `JQ_POLL_INTERVAL_SECONDS` | 0.5 | Polling interval when queue is empty |

## Schema

```sql
CREATE TABLE jobs (
    id              BIGSERIAL PRIMARY KEY,
    queue           TEXT NOT NULL DEFAULT 'default',
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued','running','done','failed','dead')),
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

-- Partial index for fast dequeue
CREATE INDEX idx_jobs_dequeue
    ON jobs (priority DESC, scheduled_at ASC)
    WHERE status = 'queued';
```

## Tests

```
tests/test_queue.py       8 tests  enqueue, dequeue, priority, complete, fail, requeue, dead-letter, stats
tests/test_concurrency.py 2 tests  no double-processing (8 workers, 50 jobs), concurrent enqueue/dequeue
tests/test_chaos.py       3 tests  crash recovery, max-retry dead-letter, random-failure chaos
tests/test_worker.py      3 tests  job processing, retry with backoff, persistent failure dead-letter
```

## License

MIT
