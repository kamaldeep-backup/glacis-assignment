import asyncio
import logging
from typing import Any

import asyncpg

from app.domain.schemas import Invoice, ShipmentUpdate, Unclassified
from app.repositories.jobs import (
    ProcessingJob,
    complete_processing_job,
    fail_processing_job,
)
from app.repositories.normalized_records import create_invoice, create_shipment_update
from app.repositories.raw_events import (
    get_raw_event_for_update,
    mark_raw_event_failed,
    mark_raw_event_normalized,
    mark_raw_event_unclassified,
)
from app.services.normalizer import PydanticAIWebhookNormalizer, WebhookNormalizer
from app.workers.ingest_worker import retry_backoff_seconds
from app.workers.worker_loop import run_worker_loop


logger = logging.getLogger(__name__)
NORMALIZATION_QUEUE = "normalization"
TERMINAL_RAW_STATUSES = {"NORMALIZED", "UNCLASSIFIED", "FAILED", "DUPLICATE"}


def extract_llm_response(error: Exception) -> Any | None:
    for attribute in ("llm_response", "model_response", "response"):
        value = getattr(error, attribute, None)
        if value is not None:
            return value
    return None


async def handle_normalization_job(
    connection: asyncpg.Connection,
    job: ProcessingJob,
    normalizer: WebhookNormalizer | None = None,
) -> None:
    normalizer = normalizer or PydanticAIWebhookNormalizer()

    try:
        raw_event = await get_raw_event_for_update(connection, job.raw_event_id)
        if raw_event is None:
            raise ValueError(f"Raw event {job.raw_event_id} does not exist")

        if raw_event["status"] in TERMINAL_RAW_STATUSES:
            await complete_processing_job(connection, job_id=job.id)
            logger.info(
                "completed normalization job %s for terminal raw event %s",
                job.id,
                job.raw_event_id,
            )
            return

        if raw_event["status"] != "QUEUED_FOR_NORMALIZATION":
            raise ValueError(
                "Raw event "
                f"{job.raw_event_id} is not queued for normalization "
                f"(status={raw_event['status']})"
            )

        normalized = await normalizer.normalize(raw_event["payload"])

        if isinstance(normalized, ShipmentUpdate):
            await create_shipment_update(
                connection,
                raw_event_id=job.raw_event_id,
                shipment=normalized,
            )
            await mark_raw_event_normalized(
                connection,
                raw_event_id=job.raw_event_id,
            )
            logger.info("normalized raw event %s as shipment", job.raw_event_id)
        elif isinstance(normalized, Invoice):
            await create_invoice(
                connection,
                raw_event_id=job.raw_event_id,
                invoice=normalized,
            )
            await mark_raw_event_normalized(
                connection,
                raw_event_id=job.raw_event_id,
            )
            logger.info("normalized raw event %s as invoice", job.raw_event_id)
        elif isinstance(normalized, Unclassified):
            await mark_raw_event_unclassified(
                connection,
                raw_event_id=job.raw_event_id,
                reason=normalized.reason,
            )
            logger.info("marked raw event %s as unclassified", job.raw_event_id)
        else:
            raise TypeError(f"Unexpected normalized output: {type(normalized)!r}")

        await complete_processing_job(connection, job_id=job.id)
    except Exception as exc:
        error_message = str(exc)
        terminal = await fail_processing_job(
            connection,
            job=job,
            stage=NORMALIZATION_QUEUE,
            error_message=error_message,
            backoff_seconds=retry_backoff_seconds(job.attempts),
            llm_response=extract_llm_response(exc),
        )
        if terminal:
            await mark_raw_event_failed(
                connection,
                raw_event_id=job.raw_event_id,
                error_message=error_message,
            )
        logger.exception("failed to process normalization job %s", job.id)


async def main() -> None:
    normalizer = PydanticAIWebhookNormalizer()

    async def handle_job(
        connection: asyncpg.Connection,
        job: ProcessingJob,
    ) -> None:
        await handle_normalization_job(connection, job, normalizer)

    await run_worker_loop(queue_name=NORMALIZATION_QUEUE, handle_job=handle_job)


if __name__ == "__main__":
    asyncio.run(main())
