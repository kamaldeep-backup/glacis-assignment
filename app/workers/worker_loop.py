import asyncio
import logging
import os
import socket
from collections.abc import Awaitable, Callable
from uuid import uuid4

import asyncpg

from app.config import Settings, get_settings
from app.db import Database, database
from app.repositories.jobs import ProcessingJob, claim_next_job


logger = logging.getLogger(__name__)
JobHandler = Callable[[asyncpg.Connection, ProcessingJob], Awaitable[None]]


def build_worker_id(queue_name: str) -> str:
    return f"{queue_name}:{socket.gethostname()}:{os.getpid()}:{uuid4()}"


async def process_one_job(
    *,
    db: Database,
    queue_name: str,
    worker_id: str,
    handle_job: JobHandler,
) -> bool:
    async with db.acquire() as connection:
        async with connection.transaction():
            job = await claim_next_job(
                connection,
                queue_name=queue_name,
                worker_id=worker_id,
            )
            if job is None:
                return False

            await handle_job(connection, job)
            return True


async def run_worker_loop(
    *,
    queue_name: str,
    handle_job: JobHandler,
    db: Database = database,
    settings: Settings | None = None,
) -> None:
    settings = settings or get_settings()
    worker_id = build_worker_id(queue_name)
    logging.basicConfig(level=logging.INFO)

    await db.connect()
    logger.info("started %s worker %s", queue_name, worker_id)
    try:
        while True:
            processed = await process_one_job(
                db=db,
                queue_name=queue_name,
                worker_id=worker_id,
                handle_job=handle_job,
            )
            if not processed:
                await asyncio.sleep(settings.worker_poll_interval_seconds)
    finally:
        await db.close()
