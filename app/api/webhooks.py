from json import JSONDecodeError

from fastapi import APIRouter, HTTPException, Request, status

from app.config import Settings, get_settings
from app.db import Database, database
from app.repositories.jobs import create_processing_job
from app.repositories.raw_events import create_raw_event


router = APIRouter()


def get_database() -> Database:
    return database


def get_app_settings() -> Settings:
    return get_settings()


@router.post("/webhooks", status_code=status.HTTP_202_ACCEPTED)
async def accept_webhook(request: Request) -> dict[str, str]:
    try:
        payload = await request.json()
    except JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body must be valid JSON",
        ) from exc

    db = get_database()
    settings = get_app_settings()

    async with db.acquire() as connection:
        async with connection.transaction():
            raw_event_id = await create_raw_event(connection, payload)
            await create_processing_job(
                connection,
                raw_event_id=raw_event_id,
                queue_name="ingress",
                max_attempts=settings.job_max_attempts,
            )

    return {"rawEventId": str(raw_event_id), "status": "RECEIVED"}
