-- V1__INIT_SCHEMA.sql

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create the main jobs table
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind TEXT NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    metadata JSONB,
    priority INTEGER NOT NULL DEFAULT 2, -- Corresponds to NORMAL priority
    state TEXT NOT NULL DEFAULT 'QUEUED'
        CHECK (state IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'RETRYING', 'DISCARDED', 'SCHEDULED', 'PAUSED')),
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

-- Index for efficiently picking up the next available job based on priority and schedule.
CREATE INDEX idx_jobs_for_pickup
ON jobs(priority DESC, run_at ASC)
WHERE state = 'QUEUED';

-- Index for the reaper to find stale, running jobs.
CREATE INDEX idx_jobs_reaper_check
ON jobs(locked_at)
WHERE state = 'RUNNING' AND lease_kind = 'EXPIRABLE';