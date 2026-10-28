from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: int
    queue: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int = 0
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_heartbeat: datetime | None = None
    attempts: int = 0
    max_retries: int = 3
    error: str | None = None
    worker_id: str | None = None
