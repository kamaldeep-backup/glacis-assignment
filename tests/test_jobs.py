import json
import unittest
from uuid import UUID

from app.repositories.jobs import ProcessingJob, claim_next_job, fail_processing_job


RAW_EVENT_ID = UUID("00000000-0000-0000-0000-000000000020")
JOB_ID = UUID("00000000-0000-0000-0000-000000000021")
FAILED_EVENT_ID = UUID("00000000-0000-0000-0000-000000000022")


def compact_sql(query: str) -> str:
    return " ".join(query.split())


class FakeJobConnection:
    def __init__(self, row: dict[str, object] | None = None) -> None:
        self.row = row
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((compact_sql(query), args))
        return self.row

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((compact_sql(query), args))
        return "UPDATE 1"

    async def fetchval(self, query: str, *args: object) -> UUID:
        self.fetchval_calls.append((compact_sql(query), args))
        return FAILED_EVENT_ID


class ProcessingJobRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_claim_next_job_uses_skip_locked_and_marks_processing(self) -> None:
        connection = FakeJobConnection(
            {
                "id": JOB_ID,
                "raw_event_id": RAW_EVENT_ID,
                "queue_name": "normalization",
                "attempts": 1,
                "max_attempts": 3,
            }
        )

        job = await claim_next_job(
            connection,
            queue_name="normalization",
            worker_id="normalization:worker-1",
        )

        self.assertEqual(
            job,
            ProcessingJob(
                id=JOB_ID,
                raw_event_id=RAW_EVENT_ID,
                queue_name="normalization",
                attempts=1,
                max_attempts=3,
            ),
        )
        claim_query, claim_args = connection.fetchrow_calls[0]
        self.assertIn("WHERE queue_name = $1", claim_query)
        self.assertIn("AND status = 'PENDING'", claim_query)
        self.assertIn("AND run_after <= now()", claim_query)
        self.assertIn("ORDER BY created_at", claim_query)
        self.assertIn("FOR UPDATE SKIP LOCKED", claim_query)
        self.assertEqual(claim_args, ("normalization",))

        update_query, update_args = connection.execute_calls[0]
        self.assertIn("SET status = 'PROCESSING'", update_query)
        self.assertIn("locked_at = now()", update_query)
        self.assertIn("locked_by = $2", update_query)
        self.assertEqual(update_args, (JOB_ID, "normalization:worker-1"))

    async def test_fail_processing_job_retries_with_backoff(self) -> None:
        connection = FakeJobConnection()
        job = ProcessingJob(
            id=JOB_ID,
            raw_event_id=RAW_EVENT_ID,
            queue_name="ingress",
            attempts=1,
            max_attempts=3,
        )

        terminal = await fail_processing_job(
            connection,
            job=job,
            stage="ingress",
            error_message="temporary database issue",
            backoff_seconds=4.0,
        )

        self.assertFalse(terminal)
        self.assertEqual(connection.fetchval_calls, [])
        update_query, update_args = connection.execute_calls[0]
        self.assertIn("SET status = 'PENDING'", update_query)
        self.assertIn("attempts = $2", update_query)
        self.assertIn("run_after = now() + ($3::double precision * interval '1 second')", update_query)
        self.assertIn("locked_at = NULL", update_query)
        self.assertIn("locked_by = NULL", update_query)
        self.assertEqual(update_args, (JOB_ID, 2, 4.0, "temporary database issue"))

    async def test_fail_processing_job_terminal_failure_inserts_failed_event(self) -> None:
        connection = FakeJobConnection()
        job = ProcessingJob(
            id=JOB_ID,
            raw_event_id=RAW_EVENT_ID,
            queue_name="normalization",
            attempts=2,
            max_attempts=3,
        )
        llm_response = {"event_type": "UNKNOWN", "raw": "payload"}

        terminal = await fail_processing_job(
            connection,
            job=job,
            stage="normalization",
            error_message="schema validation failed",
            backoff_seconds=8.0,
            llm_response=llm_response,
        )

        self.assertTrue(terminal)
        update_query, update_args = connection.execute_calls[0]
        self.assertIn("SET status = 'FAILED'", update_query)
        self.assertIn("attempts = $2", update_query)
        self.assertIn("last_error = $3", update_query)
        self.assertIn("locked_at = NULL", update_query)
        self.assertIn("locked_by = NULL", update_query)
        self.assertEqual(update_args, (JOB_ID, 3, "schema validation failed"))

        failed_query, failed_args = connection.fetchval_calls[0]
        self.assertIn("INSERT INTO failed_events", failed_query)
        self.assertIn("llm_response", failed_query)
        self.assertEqual(
            failed_args,
            (
                RAW_EVENT_ID,
                JOB_ID,
                "normalization",
                "schema validation failed",
                json.dumps(llm_response, default=str),
                3,
            ),
        )


if __name__ == "__main__":
    unittest.main()
