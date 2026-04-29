import json
import unittest
from json import JSONDecodeError
from types import SimpleNamespace
from uuid import UUID

from fastapi import HTTPException

from app.api import webhooks


RAW_EVENT_ID = UUID("00000000-0000-0000-0000-000000000001")
JOB_ID = UUID("00000000-0000-0000-0000-000000000002")


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


class FakeConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_context = FakeTransaction()

    def transaction(self) -> FakeTransaction:
        return self.transaction_context

    async def fetchval(self, query: str, *args: object) -> UUID:
        self.calls.append((" ".join(query.split()), args))
        if "raw_events" in query:
            return RAW_EVENT_ID
        if "processing_jobs" in query:
            return JOB_ID
        raise AssertionError(f"Unexpected query: {query}")


class FakeAcquire:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.entered = False
        self.exited = False

    async def __aenter__(self) -> FakeConnection:
        self.entered = True
        return self.connection

    async def __aexit__(self, *args: object) -> bool:
        self.exited = True
        return False


class FakeDatabase:
    def __init__(self) -> None:
        self.connection = FakeConnection()
        self.acquire_context = FakeAcquire(self.connection)

    def acquire(self) -> FakeAcquire:
        return self.acquire_context


class JsonRequest:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    async def json(self) -> object:
        return self.payload


class InvalidJsonRequest:
    async def json(self) -> object:
        raise JSONDecodeError("Expecting value", "", 0)


class AcceptWebhookTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.original_get_database = webhooks.get_database
        self.original_get_app_settings = webhooks.get_app_settings

    async def asyncTearDown(self) -> None:
        webhooks.get_database = self.original_get_database
        webhooks.get_app_settings = self.original_get_app_settings

    async def test_accept_webhook_persists_raw_event_and_queues_ingress_job(
        self,
    ) -> None:
        database = FakeDatabase()
        payload = {
            "carrier": "FastShip",
            "tracking": "1Z999",
            "current_state": "out for delivery",
        }
        webhooks.get_database = lambda: database
        webhooks.get_app_settings = lambda: SimpleNamespace(job_max_attempts=5)

        response = await webhooks.accept_webhook(JsonRequest(payload))

        self.assertEqual(
            response,
            {"rawEventId": str(RAW_EVENT_ID), "status": "RECEIVED"},
        )
        self.assertTrue(database.acquire_context.entered)
        self.assertTrue(database.acquire_context.exited)
        self.assertTrue(database.connection.transaction_context.entered)
        self.assertTrue(database.connection.transaction_context.exited)
        self.assertEqual(len(database.connection.calls), 2)

        raw_event_query, raw_event_args = database.connection.calls[0]
        self.assertIn("INSERT INTO raw_events", raw_event_query)
        self.assertEqual(json.loads(raw_event_args[0]), payload)

        job_query, job_args = database.connection.calls[1]
        self.assertIn("INSERT INTO processing_jobs", job_query)
        self.assertEqual(job_args, (RAW_EVENT_ID, "ingress", 5))

    async def test_accept_webhook_rejects_invalid_json(self) -> None:
        database = FakeDatabase()
        webhooks.get_database = lambda: database

        with self.assertRaises(HTTPException) as exc:
            await webhooks.accept_webhook(InvalidJsonRequest())

        self.assertEqual(exc.exception.status_code, 400)
        self.assertEqual(exc.exception.detail, "Request body must be valid JSON")
        self.assertEqual(database.connection.calls, [])


if __name__ == "__main__":
    unittest.main()
