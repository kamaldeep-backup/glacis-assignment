from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    database_url: str
    database_min_pool_size: int = 1
    database_max_pool_size: int = 10
    worker_poll_interval_seconds: float = 1.0
    job_max_attempts: int = 3
    llm_model: str = "openai:gpt-4.1-mini"


def get_settings() -> Settings:
    return Settings(
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql://webhook:webhook@localhost:5432/webhook_ingestion",
        ),
        database_min_pool_size=int(os.getenv("DATABASE_MIN_POOL_SIZE", "1")),
        database_max_pool_size=int(os.getenv("DATABASE_MAX_POOL_SIZE", "10")),
        worker_poll_interval_seconds=float(
            os.getenv("WORKER_POLL_INTERVAL_SECONDS", "1")
        ),
        job_max_attempts=int(os.getenv("JOB_MAX_ATTEMPTS", "3")),
        llm_model=os.getenv("LLM_MODEL", "openai:gpt-4.1-mini"),
    )
