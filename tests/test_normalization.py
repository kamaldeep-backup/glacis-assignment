import unittest
from datetime import UTC, datetime
from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from app.domain.schemas import (
    Invoice,
    NormalizedWebhook,
    ShipmentStatus,
    ShipmentUpdate,
    Unclassified,
)
from app.repositories.jobs import ProcessingJob
from app.workers.normalization_worker import handle_normalization_job


RAW_EVENT_ID = UUID("00000000-0000-0000-0000-000000000010")
JOB_ID = UUID("00000000-0000-0000-0000-000000000011")
INSERTED_RECORD_ID = UUID("00000000-0000-0000-0000-000000000012")


class FakeNormalizer:
    def __init__(self, normalized: object) -> None:
        self.normalized = normalized
        self.payloads: list[object] = []

    async def normalize(self, payload: object) -> object:
        self.payloads.append(payload)
        return self.normalized


class FakeConnection:
    def __init__(self, payload: object, status: str = "QUEUED_FOR_NORMALIZATION") -> None:
        self.raw_event = {
            "id": RAW_EVENT_ID,
            "payload": payload,
            "status": status,
            "idempotency_key": "idempotency-key",
        }
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object) -> object:
        self.execute_calls.append((" ".join(query.split()), args))
        return self.raw_event

    async def fetchval(self, query: str, *args: object) -> UUID:
        self.fetchval_calls.append((" ".join(query.split()), args))
        return INSERTED_RECORD_ID

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((" ".join(query.split()), args))
        return "UPDATE 1"


class NormalizedWebhookSchemaTests(unittest.TestCase):
    def test_validates_discriminated_shipment_output(self) -> None:
        parsed = TypeAdapter(NormalizedWebhook).validate_python(
            {
                "event_type": "SHIPMENT_UPDATE",
                "vendor_id": "FastShip",
                "tracking_number": "1Z999",
                "status": "TRANSIT",
                "timestamp": "2026-04-29T10:30:00Z",
            }
        )

        self.assertIsInstance(parsed, ShipmentUpdate)
        self.assertEqual(parsed.status, ShipmentStatus.TRANSIT)

    def test_rejects_extra_fields(self) -> None:
        with self.assertRaises(ValidationError):
            TypeAdapter(NormalizedWebhook).validate_python(
                {
                    "event_type": "INVOICE",
                    "vendor_id": "BillingCo",
                    "invoice_id": "INV-1",
                    "amount": 12.5,
                    "currency": "USD",
                    "unexpected": "field",
                }
            )

    def test_uppercases_invoice_currency_code(self) -> None:
        invoice = TypeAdapter(NormalizedWebhook).validate_python(
            {
                "event_type": "INVOICE",
                "vendor_id": "BillingCo",
                "invoice_id": "INV-1",
                "amount": 12.5,
                "currency": "usd",
            }
        )

        self.assertIsInstance(invoice, Invoice)
        self.assertEqual(invoice.currency, "USD")


class NormalizationWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_inserts_shipment_and_marks_event_normalized(self) -> None:
        payload = {"tracking": "1Z999"}
        connection = FakeConnection(payload)
        job = ProcessingJob(
            id=JOB_ID,
            raw_event_id=RAW_EVENT_ID,
            queue_name="normalization",
            attempts=0,
            max_attempts=3,
        )
        normalizer = FakeNormalizer(
            ShipmentUpdate(
                event_type="SHIPMENT_UPDATE",
                vendor_id="FastShip",
                tracking_number="1Z999",
                status=ShipmentStatus.TRANSIT,
                timestamp=datetime(2026, 4, 29, 10, 30, tzinfo=UTC),
            )
        )

        await handle_normalization_job(connection, job, normalizer)

        self.assertEqual(normalizer.payloads, [payload])
        self.assertTrue(
            any("INSERT INTO shipment_updates" in call[0] for call in connection.fetchval_calls)
        )
        self.assertTrue(
            any("SET status = 'NORMALIZED'" in call[0] for call in connection.execute_calls)
        )
        self.assertTrue(
            any("SET status = 'COMPLETED'" in call[0] for call in connection.execute_calls)
        )

    async def test_inserts_invoice_and_marks_event_normalized(self) -> None:
        connection = FakeConnection({"invoice_id": "INV-1"})
        job = ProcessingJob(
            id=JOB_ID,
            raw_event_id=RAW_EVENT_ID,
            queue_name="normalization",
            attempts=0,
            max_attempts=3,
        )
        normalizer = FakeNormalizer(
            Invoice(
                event_type="INVOICE",
                vendor_id="BillingCo",
                invoice_id="INV-1",
                amount=125.5,
                currency="USD",
            )
        )

        await handle_normalization_job(connection, job, normalizer)

        self.assertTrue(
            any("INSERT INTO invoices" in call[0] for call in connection.fetchval_calls)
        )
        self.assertTrue(
            any("SET status = 'NORMALIZED'" in call[0] for call in connection.execute_calls)
        )

    async def test_marks_unclassified_without_business_record(self) -> None:
        connection = FakeConnection({"noise": True})
        job = ProcessingJob(
            id=JOB_ID,
            raw_event_id=RAW_EVENT_ID,
            queue_name="normalization",
            attempts=0,
            max_attempts=3,
        )
        normalizer = FakeNormalizer(
            Unclassified(event_type="UNCLASSIFIED", reason="No shipment or invoice evidence")
        )

        await handle_normalization_job(connection, job, normalizer)

        self.assertEqual(connection.fetchval_calls, [])
        self.assertTrue(
            any("SET status = 'UNCLASSIFIED'" in call[0] for call in connection.execute_calls)
        )


if __name__ == "__main__":
    unittest.main()
