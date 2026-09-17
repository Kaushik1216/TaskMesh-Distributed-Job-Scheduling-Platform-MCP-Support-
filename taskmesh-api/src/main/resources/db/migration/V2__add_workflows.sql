-- ============================================================
-- V2: DAG Workflow Support for TaskMesh
-- ============================================================

-- Workflow table: A DAG of jobs with lifecycle tracking
CREATE TABLE IF NOT EXISTS workflows (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             VARCHAR(255) NOT NULL,
    description      TEXT,
    status           VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
    failure_policy   VARCHAR(30)  NOT NULL DEFAULT 'FAIL_FAST',
    trace_id         VARCHAR(64),
    total_nodes      INT NOT NULL DEFAULT 0,
    completed_nodes  INT NOT NULL DEFAULT 0,
    failed_nodes     INT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_workflows_status ON workflows(status);

-- Workflow nodes: Each maps a human-readable key to a Job
CREATE TABLE IF NOT EXISTS workflow_nodes (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id  UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    node_key     VARCHAR(100) NOT NULL,
    job_id       UUID NOT NULL REFERENCES jobs(id),
    UNIQUE(workflow_id, node_key)
);

CREATE INDEX IF NOT EXISTS idx_wf_nodes_workflow ON workflow_nodes(workflow_id);
CREATE INDEX IF NOT EXISTS idx_wf_nodes_job      ON workflow_nodes(job_id);

-- Workflow edges: DAG dependency structure (from must complete before to)
CREATE TABLE IF NOT EXISTS workflow_edges (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id    UUID NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    from_node_key  VARCHAR(100) NOT NULL,
    to_node_key    VARCHAR(100) NOT NULL,
    UNIQUE(workflow_id, from_node_key, to_node_key)
);

CREATE INDEX IF NOT EXISTS idx_wf_edges_workflow ON workflow_edges(workflow_id);

-- Add workflow reference columns to jobs (nullable = backward compatible)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS workflow_id       UUID REFERENCES workflows(id);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS workflow_node_key VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_jobs_workflow ON jobs(workflow_id);

-- Auto-update updated_at on workflow changes
DROP TRIGGER IF EXISTS trg_workflows_updated_at ON workflows;
CREATE TRIGGER trg_workflows_updated_at
    BEFORE UPDATE ON workflows
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
