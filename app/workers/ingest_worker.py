import asyncio
import logging

import asyncpg

from app.config import Settings, get_settings
from app.repositories.jobs import (
    ProcessingJob,
    complete_processing_job,
    create_processing_job,
    fail_processing_job,
)
from app.repositories.raw_events import (
    get_raw_event_for_update,
    mark_raw_event_duplicate,
    mark_raw_event_failed,
    mark_raw_event_queued_for_normalization,
)
from app.services.idempotency import compute_idempotency_key
from app.workers.worker_loop import run_worker_loop


logger = logging.getLogger(__name__)
INGRESS_QUEUE = "ingress"
NORMALIZATION_QUEUE = "normalization"


def retry_backoff_seconds(attempts: int) -> float:
    return float(min(2**attempts, 60))


async def handle_ingress_job(
    connection: asyncpg.Connection,
    job: ProcessingJob,
    settings: Settings | None = None,
) -> None:
    settings = settings or get_settings()

    try:
        raw_event = await get_raw_event_for_update(connection, job.raw_event_id)
        if raw_event is None:
            raise ValueError(f"Raw event {job.raw_event_id} does not exist")

        idempotency_key = compute_idempotency_key(raw_event["payload"])
        try:
            async with connection.transaction():
                await mark_raw_event_queued_for_normalization(
                    connection,
                    raw_event_id=job.raw_event_id,
                    idempotency_key=idempotency_key,
                )
        except asyncpg.UniqueViolationError:
            await mark_raw_event_duplicate(
                connection,
                raw_event_id=job.raw_event_id,
                error_message="Duplicate payload",
            )
            await complete_processing_job(connection, job_id=job.id)
            logger.info("marked raw event %s as duplicate", job.raw_event_id)
            return

        await create_processing_job(
            connection,
            raw_event_id=job.raw_event_id,
            queue_name=NORMALIZATION_QUEUE,
            max_attempts=settings.job_max_attempts,
        )
        await complete_processing_job(connection, job_id=job.id)
        logger.info("queued raw event %s for normalization", job.raw_event_id)
    except Exception as exc:
        error_message = str(exc)
        terminal = await fail_processing_job(
            connection,
            job=job,
            stage=INGRESS_QUEUE,
            error_message=error_message,
            backoff_seconds=retry_backoff_seconds(job.attempts),
        )
        if terminal:
            await mark_raw_event_failed(
                connection,
                raw_event_id=job.raw_event_id,
                error_message=error_message,
            )
        logger.exception("failed to process ingress job %s", job.id)


async def main() -> None:
    async def handle_job(
        connection: asyncpg.Connection,
        job: ProcessingJob,
    ) -> None:
        await handle_ingress_job(connection, job)

    await run_worker_loop(queue_name=INGRESS_QUEUE, handle_job=handle_job)


if __name__ == "__main__":
    asyncio.run(main())
