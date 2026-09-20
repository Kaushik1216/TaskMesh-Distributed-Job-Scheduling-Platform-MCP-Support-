-- ============================================================
-- V1: Initial Schema for TaskMesh Distributed Job Scheduler
-- ============================================================

-- Jobs table: Core state store for all job lifecycle transitions
CREATE TABLE IF NOT EXISTS jobs (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type                 VARCHAR(50)  NOT NULL,
    status               VARCHAR(30)  NOT NULL DEFAULT 'QUEUED',
    priority             VARCHAR(20)  NOT NULL DEFAULT 'NORMAL',
    payload              TEXT,
    required_capabilities VARCHAR(500),
    scheduled_at         TIMESTAMPTZ,
    cron_expression      VARCHAR(100),
    max_retries          INT          NOT NULL DEFAULT 3,
    attempt_count        INT          NOT NULL DEFAULT 0,
    timeout_seconds      INT          NOT NULL DEFAULT 300,
    assigned_worker_id   VARCHAR(100),
    result               TEXT,
    error_message        TEXT,
    idempotency_key      VARCHAR(255) UNIQUE,
    trace_id             VARCHAR(64),
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status          ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_priority_status ON jobs(priority, status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_worker          ON jobs(assigned_worker_id);
CREATE INDEX IF NOT EXISTS idx_jobs_trace           ON jobs(trace_id);

-- Workers table: Registry of active worker nodes
CREATE TABLE IF NOT EXISTS workers (
    worker_id       VARCHAR(100) PRIMARY KEY,
    capabilities    VARCHAR(500),
    max_concurrency INT          NOT NULL DEFAULT 4,
    active_jobs     INT          NOT NULL DEFAULT 0,
    status          VARCHAR(20)  NOT NULL DEFAULT 'ACTIVE',
    last_heartbeat  TIMESTAMPTZ,
    registered_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Auto-update updated_at on job row changes
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
