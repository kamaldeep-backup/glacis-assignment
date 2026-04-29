# AI Webhook Ingestion Service

This document describes the final architecture for a minimal take-home implementation of an AI-powered webhook ingestion service.

The system accepts arbitrary vendor webhook payloads, persists them quickly, processes them asynchronously, uses a real LLM through PydanticAI to classify and normalize payloads, validates strict output schemas, and stores normalized shipment or invoice records.

## Goals

- Accept any arbitrary JSON webhook payload.
- Acknowledge vendors quickly with `202 Accepted`.
- Avoid running expensive work in the webhook request path.
- Store raw payloads durably.
- Deduplicate repeated vendor payloads.
- Use a real LLM service through PydanticAI.
- Validate LLM output using strict Pydantic schemas.
- Store normalized shipment and invoice records.
- Retry transient failures and preserve terminal failures for inspection.
- Keep the implementation small enough for a take-home assignment.

## Final Technology Choices

- **API framework:** FastAPI
- **Database:** Postgres
- **Raw payload storage:** Postgres `jsonb`
- **Queue:** Postgres-backed `processing_jobs` table
- **Workers:** Raw Python worker processes
- **LLM framework:** PydanticAI
- **LLM provider:** OpenAI, Anthropic, Gemini, or another PydanticAI-supported provider
- **Validation:** Pydantic
- **Local runtime:** Docker Compose


## High-Level Architecture

```mermaid
flowchart LR
  Vendor[Vendor Webhook] --> API[FastAPI Webhook API]

  API --> DB[(Postgres)]
  API --> Ack[202 Accepted]

  DB --> IngestWorker[Ingest Worker]
  IngestWorker --> DB

  DB --> NormalizeWorker[Normalization Worker]
  NormalizeWorker --> LLM[PydanticAI + Real LLM]
  LLM --> NormalizeWorker
  NormalizeWorker --> DB
```

## Responsibilities

### Webhook API

The API does the minimum amount of work needed to durably accept the webhook.

It does:

- accept arbitrary JSON
- insert the raw payload into `raw_events`
- insert an `ingress` job into `processing_jobs`
- return `202 Accepted`

It does not:

- call the LLM
- classify the event
- normalize the payload
- compute idempotency
- perform expensive canonicalization

This keeps vendor acknowledgments fast.

### Ingest Worker

The ingest worker processes jobs from the logical `ingress` queue.

It does:

- load the raw payload from Postgres
- compute the idempotency key from canonicalized JSON
- detect duplicate payloads
- mark duplicates as `DUPLICATE`
- enqueue new events into the logical `normalization` queue

This keeps deduplication out of the webhook request path while still preventing duplicate LLM calls.

### Normalization Worker

The normalization worker processes jobs from the logical `normalization` queue.

It does:

- load the raw payload from Postgres
- call a PydanticAI agent backed by a real LLM
- classify the payload as shipment, invoice, or unclassified
- extract strict normalized fields
- validate the result with Pydantic
- insert into `shipment_updates` or `invoices`
- mark unclassified events without creating business records
- retry failures and record terminal failures

## End-To-End Flow

1. Vendor sends arbitrary JSON to `POST /webhooks`.
2. FastAPI inserts the payload into `raw_events`.
3. FastAPI inserts an `ingress` job into `processing_jobs`.
4. FastAPI returns `202 Accepted`.
5. The ingest worker claims a pending `ingress` job.
6. The ingest worker loads the raw payload.
7. The ingest worker computes an idempotency key.
8. If the key already exists, the event is marked `DUPLICATE`.
9. If the key is new, the event is marked `QUEUED_FOR_NORMALIZATION`.
10. The ingest worker creates a `normalization` job.
11. The normalization worker claims a pending `normalization` job.
12. The normalization worker loads the raw payload.
13. The normalization worker calls PydanticAI with a strict output schema.
14. The LLM returns one of `ShipmentUpdate`, `Invoice`, or `Unclassified`.
15. Pydantic validates the returned structure.
16. Valid shipment records are inserted into `shipment_updates`.
17. Valid invoice records are inserted into `invoices`.
18. Unclassified events are marked `UNCLASSIFIED`.
19. Failures are retried up to the configured max attempts.
20. Terminal failures are marked `FAILED` and stored in `failed_events`.

## Database Relationship Diagram

```mermaid
erDiagram
  RAW_EVENTS ||--o{ PROCESSING_JOBS : "has"
  RAW_EVENTS ||--o| SHIPMENT_UPDATES : "normalizes to"
  RAW_EVENTS ||--o| INVOICES : "normalizes to"
  RAW_EVENTS ||--o{ FAILED_EVENTS : "can fail as"
  PROCESSING_JOBS ||--o{ FAILED_EVENTS : "can produce"

  RAW_EVENTS {
    uuid id PK
    jsonb payload
    raw_event_status status
    text idempotency_key UK
    timestamptz received_at
    timestamptz processed_at
    text error_message
  }

  PROCESSING_JOBS {
    uuid id PK
    uuid raw_event_id FK
    text queue_name
    job_status status
    int attempts
    int max_attempts
    timestamptz run_after
    text last_error
    timestamptz locked_at
    text locked_by
    timestamptz created_at
    timestamptz updated_at
  }

  SHIPMENT_UPDATES {
    uuid id PK
    uuid raw_event_id FK,UK
    text vendor_id
    text tracking_number
    shipment_status status
    timestamptz event_timestamp
    timestamptz created_at
  }

  INVOICES {
    uuid id PK
    uuid raw_event_id FK,UK
    text vendor_id
    text invoice_id
    numeric amount
    text currency
    timestamptz created_at
  }

  FAILED_EVENTS {
    uuid id PK
    uuid raw_event_id FK
    uuid job_id FK
    text stage
    text error_message
    jsonb llm_response
    int attempts
    timestamptz created_at
  }
```

## Database Schema

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TYPE raw_event_status AS ENUM (
  'RECEIVED',
  'DUPLICATE',
  'QUEUED_FOR_NORMALIZATION',
  'NORMALIZED',
  'UNCLASSIFIED',
  'FAILED'
);

CREATE TYPE job_status AS ENUM (
  'PENDING',
  'PROCESSING',
  'COMPLETED',
  'FAILED'
);

CREATE TYPE shipment_status AS ENUM (
  'TRANSIT',
  'DELIVERED',
  'EXCEPTION'
);

CREATE TABLE raw_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  payload JSONB NOT NULL,
  status raw_event_status NOT NULL DEFAULT 'RECEIVED',
  idempotency_key TEXT UNIQUE,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at TIMESTAMPTZ,
  error_message TEXT
);

CREATE TABLE processing_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_event_id UUID NOT NULL REFERENCES raw_events(id) ON DELETE CASCADE,
  queue_name TEXT NOT NULL,
  status job_status NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  run_after TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT,
  locked_at TIMESTAMPTZ,
  locked_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE shipment_updates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_event_id UUID NOT NULL UNIQUE REFERENCES raw_events(id) ON DELETE CASCADE,
  vendor_id TEXT NOT NULL,
  tracking_number TEXT NOT NULL,
  status shipment_status NOT NULL,
  event_timestamp TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE invoices (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_event_id UUID NOT NULL UNIQUE REFERENCES raw_events(id) ON DELETE CASCADE,
  vendor_id TEXT NOT NULL,
  invoice_id TEXT NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  currency TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE failed_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_event_id UUID REFERENCES raw_events(id) ON DELETE SET NULL,
  job_id UUID REFERENCES processing_jobs(id) ON DELETE SET NULL,
  stage TEXT NOT NULL,
  error_message TEXT NOT NULL,
  llm_response JSONB,
  attempts INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_processing_jobs_claim
ON processing_jobs (queue_name, status, run_after, created_at);

CREATE INDEX idx_processing_jobs_raw_event_id
ON processing_jobs (raw_event_id);

CREATE INDEX idx_raw_events_status
ON raw_events (status);

CREATE INDEX idx_failed_events_raw_event_id
ON failed_events (raw_event_id);
```

## Postgres Job Queue Design

The system has two logical queues in one table:

```text
processing_jobs.queue_name = 'ingress'
processing_jobs.queue_name = 'normalization'
```

Workers claim jobs using `FOR UPDATE SKIP LOCKED`, allowing multiple worker processes to run safely.

```sql
SELECT id
FROM processing_jobs
WHERE queue_name = $1
  AND status = 'PENDING'
  AND run_after <= now()
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1;
```

After claiming a job, the worker marks it as processing:

```sql
UPDATE processing_jobs
SET status = 'PROCESSING',
    locked_at = now(),
    locked_by = $1,
    updated_at = now()
WHERE id = $2;
```

If the job succeeds, it is marked `COMPLETED`.

If the job fails and attempts remain, it is reset to `PENDING` with a future `run_after` timestamp for backoff.

If the job exceeds `max_attempts`, it is marked `FAILED` and a row is inserted into `failed_events`.

## LLM Normalization Strategy

The normalization worker uses PydanticAI with a structured output type.

The output should be a discriminated union:

```python
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


class ShipmentStatus(StrEnum):
    TRANSIT = "TRANSIT"
    DELIVERED = "DELIVERED"
    EXCEPTION = "EXCEPTION"


class ShipmentUpdate(BaseModel):
    event_type: Literal["SHIPMENT_UPDATE"]
    vendor_id: str
    tracking_number: str
    status: ShipmentStatus
    timestamp: datetime


class Invoice(BaseModel):
    event_type: Literal["INVOICE"]
    vendor_id: str
    invoice_id: str
    amount: float
    currency: str


class Unclassified(BaseModel):
    event_type: Literal["UNCLASSIFIED"]
    reason: str


NormalizedWebhook = Annotated[
    Union[ShipmentUpdate, Invoice, Unclassified],
    Field(discriminator="event_type"),
]
```

The PydanticAI agent should be configured with `output_type=NormalizedWebhook`.

The prompt should instruct the model to:

- classify only as `SHIPMENT_UPDATE`, `INVOICE`, or `UNCLASSIFIED`
- return `UNCLASSIFIED` when there is insufficient evidence
- not invent missing IDs or amounts
- normalize shipment statuses into `TRANSIT`, `DELIVERED`, or `EXCEPTION`
- return timestamps as ISO 8601 datetimes
- return invoice amounts as numeric values
- return currency as a standard currency code when present

The application still treats the LLM as untrusted. Pydantic validation is the source of truth.

## Proposed Code Structure

```text
.
├── README.md
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── app
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── db.py
│   ├── api
│   │   ├── __init__.py
│   │   └── webhooks.py
│   ├── domain
│   │   ├── __init__.py
│   │   ├── schemas.py
│   │   └── statuses.py
│   ├── repositories
│   │   ├── __init__.py
│   │   ├── raw_events.py
│   │   ├── jobs.py
│   │   └── normalized_records.py
│   ├── services
│   │   ├── __init__.py
│   │   ├── idempotency.py
│   │   └── normalizer.py
│   ├── llm
│   │   ├── __init__.py
│   │   └── agent.py
│   └── workers
│       ├── __init__.py
│       ├── ingest_worker.py
│       ├── normalization_worker.py
│       └── worker_loop.py
├── migrations
│   └── 001_initial_schema.sql
└── tests
    ├── test_webhooks.py
    ├── test_idempotency.py
    └── test_normalization.py
```

## Docker Compose

For the take-home, Docker Compose can run Postgres, the API, and both workers.

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: webhook
      POSTGRES_PASSWORD: webhook
      POSTGRES_DB: webhook_ingestion
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U webhook -d webhook_ingestion"]
      interval: 5s
      timeout: 5s
      retries: 5

  api:
    build: .
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000
    env_file:
      - .env
    ports:
      - "8000:8000"
    depends_on:
      postgres:
        condition: service_healthy

  ingest-worker:
    build: .
    command: python -m app.workers.ingest_worker
    env_file:
      - .env
    depends_on:
      postgres:
        condition: service_healthy

  normalization-worker:
    build: .
    command: python -m app.workers.normalization_worker
    env_file:
      - .env
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  postgres_data:
```

## Local Commands

Example local commands:

```bash
docker compose up --build
```

Submit a webhook:

```bash
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "carrier": "FastShip",
    "tracking": "1Z999",
    "current_state": "out for delivery",
    "event_time": "2026-04-29T10:30:00Z"
  }'
```

Expected API response:

```json
{
  "rawEventId": "generated-uuid",
  "status": "RECEIVED"
}
```

## Configuration

Example `.env.example`:

```bash
DATABASE_URL=postgresql://webhook:webhook@postgres:5432/webhook_ingestion
LLM_MODEL=openai:gpt-4.1-mini
OPENAI_API_KEY=replace-me
WORKER_POLL_INTERVAL_SECONDS=1
JOB_MAX_ATTEMPTS=3
```

## API Endpoints

### `POST /webhooks`

Accepts arbitrary JSON.

Returns:

```json
{
  "rawEventId": "uuid",
  "status": "RECEIVED"
}
```

### `GET /events/{raw_event_id}`

Optional debugging endpoint for the take-home.

Returns raw event status and any normalized record.

### `GET /health`

Returns service health.

## Error Handling

Retryable failures:

- temporary database errors
- LLM provider timeouts
- rate limits
- transient network failures

Terminal failures:

- repeated LLM validation failures
- malformed payloads that cannot be classified
- unexpected worker exceptions after max retries

Failures are stored in `failed_events` with enough context to debug and potentially replay.

## Idempotency

The webhook API does not compute idempotency.

The ingest worker computes an idempotency key from canonical JSON. A practical implementation is:

1. Sort object keys recursively.
2. Serialize to compact JSON.
3. Compute SHA-256.

The resulting hash is stored in `raw_events.idempotency_key`, which has a unique constraint.

This means duplicates may be accepted and stored initially, but they are stopped before normalization and before LLM usage.

## Tradeoffs For The Take-Home

### Deliberate Simplifications

- Postgres `jsonb` is used for raw payload storage instead of S3/GCS.
- Postgres is used as the queue instead of Redis, Celery, SQS, RabbitMQ, or Kafka.
- Workers are raw Python loops instead of a full worker framework.
- There is no vendor authentication or signature verification.
- There is no admin UI for replaying failed events.
- There is no payload size offloading to object storage.
- There is no advanced observability stack.

## Production Improvements

Before production, I would add:

- vendor authentication and signature verification
- request size limits
- object storage for large payloads
- a dedicated queue such as SQS, RabbitMQ, Kafka, or Redis/Celery
- rate limiting and backpressure
- structured logs, metrics, and tracing
- prompt and model versioning
- token usage and LLM cost tracking
- dead-letter replay tooling
- raw event retention policies
- table partitioning for high-volume raw events
- deterministic parsers for known high-volume vendors
- alerts for failed jobs, queue depth, and LLM error rate
