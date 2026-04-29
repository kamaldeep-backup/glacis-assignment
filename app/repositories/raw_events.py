import json
from typing import Any
from uuid import UUID

import asyncpg


async def create_raw_event(
    connection: asyncpg.Connection,
    payload: Any,
) -> UUID:
    return await connection.fetchval(
        """
        INSERT INTO raw_events (payload)
        VALUES ($1::jsonb)
        RETURNING id
        """,
        json.dumps(payload),
    )


async def get_raw_event_for_update(
    connection: asyncpg.Connection,
    raw_event_id: UUID,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        """
        SELECT id, payload, status, idempotency_key
        FROM raw_events
        WHERE id = $1
        FOR UPDATE
        """,
        raw_event_id,
    )


async def mark_raw_event_queued_for_normalization(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    idempotency_key: str,
) -> None:
    await connection.execute(
        """
        UPDATE raw_events
        SET status = 'QUEUED_FOR_NORMALIZATION',
            idempotency_key = $2,
            processed_at = now(),
            error_message = NULL
        WHERE id = $1
        """,
        raw_event_id,
        idempotency_key,
    )


async def mark_raw_event_duplicate(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    error_message: str,
) -> None:
    await connection.execute(
        """
        UPDATE raw_events
        SET status = 'DUPLICATE',
            processed_at = now(),
            error_message = $2
        WHERE id = $1
        """,
        raw_event_id,
        error_message,
    )


async def mark_raw_event_failed(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    error_message: str,
) -> None:
    await connection.execute(
        """
        UPDATE raw_events
        SET status = 'FAILED',
            processed_at = now(),
            error_message = $2
        WHERE id = $1
        """,
        raw_event_id,
        error_message,
    )


async def mark_raw_event_normalized(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
) -> None:
    await connection.execute(
        """
        UPDATE raw_events
        SET status = 'NORMALIZED',
            processed_at = now(),
            error_message = NULL
        WHERE id = $1
        """,
        raw_event_id,
    )


async def mark_raw_event_unclassified(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    reason: str,
) -> None:
    await connection.execute(
        """
        UPDATE raw_events
        SET status = 'UNCLASSIFIED',
            processed_at = now(),
            error_message = $2
        WHERE id = $1
        """,
        raw_event_id,
        reason,
    )
