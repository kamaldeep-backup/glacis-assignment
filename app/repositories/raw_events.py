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
