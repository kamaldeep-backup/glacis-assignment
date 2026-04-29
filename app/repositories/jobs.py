from uuid import UUID

import asyncpg


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
