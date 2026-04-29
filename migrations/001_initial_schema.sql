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
