from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://jobqueue:jobqueue@localhost:5432/jobqueue"
    max_retries: int = 3
    base_backoff_seconds: float = 1.0
    visibility_timeout_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    poll_interval_seconds: float = 0.5

    model_config = {"env_prefix": "JQ_", "case_sensitive": False}


settings = Settings()
