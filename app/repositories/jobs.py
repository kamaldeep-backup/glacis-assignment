from dataclasses import dataclass
import json
from typing import Any
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class ProcessingJob:
    id: UUID
    raw_event_id: UUID
    queue_name: str
    attempts: int
    max_attempts: int


async def create_processing_job(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    queue_name: str,
    max_attempts: int,
) -> UUID:
    return await connection.fetchval(
        """
        INSERT INTO processing_jobs (raw_event_id, queue_name, max_attempts)
        VALUES ($1, $2, $3)
        RETURNING id
        """,
        raw_event_id,
        queue_name,
        max_attempts,
    )


async def claim_next_job(
    connection: asyncpg.Connection,
    *,
    queue_name: str,
    worker_id: str,
) -> ProcessingJob | None:
    row = await connection.fetchrow(
        """
        SELECT id, raw_event_id, queue_name, attempts, max_attempts
        FROM processing_jobs
        WHERE queue_name = $1
          AND status = 'PENDING'
          AND run_after <= now()
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        """,
        queue_name,
    )
    if row is None:
        return None

    await connection.execute(
        """
        UPDATE processing_jobs
        SET status = 'PROCESSING',
            locked_at = now(),
            locked_by = $2,
            updated_at = now()
        WHERE id = $1
        """,
        row["id"],
        worker_id,
    )

    return ProcessingJob(
        id=row["id"],
        raw_event_id=row["raw_event_id"],
        queue_name=row["queue_name"],
        attempts=row["attempts"],
        max_attempts=row["max_attempts"],
    )


async def complete_processing_job(
    connection: asyncpg.Connection,
    *,
    job_id: UUID,
) -> None:
    await connection.execute(
        """
        UPDATE processing_jobs
        SET status = 'COMPLETED',
            last_error = NULL,
            locked_at = NULL,
            locked_by = NULL,
            updated_at = now()
        WHERE id = $1
        """,
        job_id,
    )


async def fail_processing_job(
    connection: asyncpg.Connection,
    *,
    job: ProcessingJob,
    stage: str,
    error_message: str,
    backoff_seconds: float,
    llm_response: Any | None = None,
) -> bool:
    next_attempts = job.attempts + 1
    terminal = next_attempts >= job.max_attempts

    if terminal:
        await connection.execute(
            """
            UPDATE processing_jobs
            SET status = 'FAILED',
                attempts = $2,
                last_error = $3,
                locked_at = NULL,
                locked_by = NULL,
                updated_at = now()
            WHERE id = $1
            """,
            job.id,
            next_attempts,
            error_message,
        )
        await create_failed_event(
            connection,
            raw_event_id=job.raw_event_id,
            job_id=job.id,
            stage=stage,
            error_message=error_message,
            attempts=next_attempts,
            llm_response=llm_response,
        )
        return True

    await connection.execute(
        """
        UPDATE processing_jobs
        SET status = 'PENDING',
            attempts = $2,
            run_after = now() + ($3::double precision * interval '1 second'),
            last_error = $4,
            locked_at = NULL,
            locked_by = NULL,
            updated_at = now()
        WHERE id = $1
        """,
        job.id,
        next_attempts,
        backoff_seconds,
        error_message,
    )
    return False


async def create_failed_event(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    job_id: UUID,
    stage: str,
    error_message: str,
    attempts: int,
    llm_response: Any | None = None,
) -> UUID:
    serialized_llm_response = (
        json.dumps(llm_response, default=str) if llm_response is not None else None
    )
    return await connection.fetchval(
        """
        INSERT INTO failed_events (
            raw_event_id,
            job_id,
            stage,
            error_message,
            llm_response,
            attempts
        )
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
        RETURNING id
        """,
        raw_event_id,
        job_id,
        stage,
        error_message,
        serialized_llm_response,
        attempts,
    )
