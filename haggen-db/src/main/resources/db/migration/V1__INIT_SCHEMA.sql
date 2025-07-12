-- V1__INIT_SCHEMA.sql

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create the main jobs table
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type TEXT NOT NULL,
    payload JSONB,
    state TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (state IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,
    last_error_details TEXT,
    lease_kind TEXT NOT NULL DEFAULT 'EXPIRABLE'
        CHECK (lease_kind IN ('EXPIRABLE', 'PERMANENT')),
    locked_by UUID,
    locked_at TIMESTAMPTZ,
    lease_token UUID
);

-- Index jobs available in the queue.
CREATE INDEX idx_queued_jobs
ON jobs(run_at)
WHERE state = 'QUEUED';