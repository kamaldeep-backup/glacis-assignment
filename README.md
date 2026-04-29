# Glacis Assignment

Minimal AI-powered webhook ingestion service.

The service accepts arbitrary vendor webhook JSON, stores the raw payload in Postgres, queues asynchronous processing jobs, deduplicates repeated payloads, and uses PydanticAI with Groq to classify and normalize shipment or invoice events.

For the full system design, tradeoffs, assumptions, and future improvement areas, read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Prerequisites

- Docker and Docker Compose
- A Groq API key for LLM-backed normalization
- `uv` if you want to run tests or commands directly on the host

## Configuration

Create a local environment file:

```bash
cp .env.example .env
```

Update `.env` with a real Groq API key:

```bash
GROQ_API_KEY=your-groq-api-key
```

The Docker Compose setup overrides `DATABASE_URL` for containers so they connect to the `postgres` service. The `.env.example` value uses `localhost`, which is useful when running the API or workers directly on the host.

## Run With Docker Compose

Start Postgres, the FastAPI app, the ingest worker, and the normalization worker:

```bash
docker compose up --build
```

The API will be available at:

```text
http://localhost:8000
```

Check service health:

```bash
curl http://localhost:8000/health
```

Submit a sample shipment webhook:

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

Expected response:

```json
{
  "rawEventId": "generated-uuid",
  "status": "RECEIVED"
}
```

Stop the stack:

```bash
docker compose down
```

To also remove the local Postgres volume:

```bash
docker compose down -v
```

## Run Locally

Start only Postgres:

```bash
docker compose up postgres
```

Install dependencies:

```bash
uv sync
```

Run the API:

```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

In separate terminals, run the workers:

```bash
uv run python -m app.workers.ingest_worker
```

```bash
uv run python -m app.workers.normalization_worker
```

## Tests

Run the unit test suite:

```bash
uv run python -m unittest discover -s tests
```

## Project Structure

```text
app/
  api/            FastAPI routes
  domain/         Pydantic schemas and normalized event types
  llm/            PydanticAI agent configuration
  repositories/   Database access helpers
  services/       Idempotency and normalization services
  workers/        Ingest and normalization worker loops
docs/
  ARCHITECTURE.md System design, assumptions, and future improvements
migrations/
  001_initial_schema.sql
tests/
```
