import unittest
from types import SimpleNamespace
from uuid import UUID

import asyncpg

from app.repositories.jobs import ProcessingJob
from app.workers.ingest_worker import handle_ingress_job, retry_backoff_seconds


RAW_EVENT_ID = UUID("00000000-0000-0000-0000-000000000030")
JOB_ID = UUID("00000000-0000-0000-0000-000000000031")


def compact_sql(query: str) -> str:
    return " ".join(query.split())


class FakeTransaction:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> "FakeTransaction":
        self.entered = True
        return self

    async def __aexit__(self, *args: object) -> bool:
        self.exited = True
        return False


class DuplicateIdempotencyConnection:
    def __init__(self) -> None:
        self.raw_event = {
            "id": RAW_EVENT_ID,
            "payload": {"carrier": "FastShip", "tracking": "1Z999"},
            "status": "RECEIVED",
            "idempotency_key": None,
        }
        self.transaction_context = FakeTransaction()
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> FakeTransaction:
        return self.transaction_context

    async def fetchrow(self, query: str, *args: object) -> dict[str, object]:
        self.fetchrow_calls.append((compact_sql(query), args))
        return self.raw_event

    async def execute(self, query: str, *args: object) -> str:
        normalized_query = compact_sql(query)
        self.execute_calls.append((normalized_query, args))
        if "SET status = 'QUEUED_FOR_NORMALIZATION'" in normalized_query:
            raise asyncpg.UniqueViolationError("duplicate key value violates unique constraint")
        return "UPDATE 1"

    async def fetchval(self, query: str, *args: object) -> UUID:
        self.fetchval_calls.append((compact_sql(query), args))
        raise AssertionError("Duplicate ingress jobs must not enqueue normalization work")


class IngestWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_backoff_is_exponential_and_capped(self) -> None:
        self.assertEqual(retry_backoff_seconds(0), 1.0)
        self.assertEqual(retry_backoff_seconds(3), 8.0)
        self.assertEqual(retry_backoff_seconds(99), 60.0)

    async def test_concurrent_duplicate_marks_event_duplicate_and_completes_job(self) -> None:
        connection = DuplicateIdempotencyConnection()
        job = ProcessingJob(
            id=JOB_ID,
            raw_event_id=RAW_EVENT_ID,
            queue_name="ingress",
            attempts=0,
            max_attempts=3,
        )

        await handle_ingress_job(
            connection,
            job,
            settings=SimpleNamespace(job_max_attempts=3),
        )

        self.assertTrue(connection.transaction_context.entered)
        self.assertTrue(connection.transaction_context.exited)
        self.assertEqual(connection.fetchval_calls, [])

        queries = [query for query, _args in connection.execute_calls]
        self.assertTrue(
            any("SET status = 'QUEUED_FOR_NORMALIZATION'" in query for query in queries)
        )
        self.assertTrue(any("SET status = 'DUPLICATE'" in query for query in queries))
        self.assertTrue(any("SET status = 'COMPLETED'" in query for query in queries))
        self.assertFalse(any("SET status = 'FAILED'" in query for query in queries))


if __name__ == "__main__":
    unittest.main()
