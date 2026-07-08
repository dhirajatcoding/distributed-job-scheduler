-- ============================================================
-- Drop order: child tables before parent tables
-- ============================================================
DROP TABLE IF EXISTS dead_letter_jobs;
DROP TABLE IF EXISTS job_executions;
DROP TABLE IF EXISTS job_dependencies;
DROP TABLE IF EXISTS jobs;

-- ============================================================
-- jobs
-- Phase 3: WAITING added — jobs with unmet dependencies sit
-- here and are invisible to the scheduler poll query.
-- ============================================================
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255)    NOT NULL,
    job_type        VARCHAR(100)    NOT NULL,
    schedule_type   VARCHAR(20)     NOT NULL CHECK (schedule_type IN ('CRON', 'ONE_TIME')),
    cron_expr       VARCHAR(100),
    payload         TEXT,
    status          VARCHAR(20)     NOT NULL DEFAULT 'SCHEDULED'
                                    CHECK (status IN ('SCHEDULED','RUNNING','PAUSED','COMPLETED','DEAD','WAITING')),
    max_retries     INT             NOT NULL DEFAULT 3,
    attempt_count   INT             NOT NULL DEFAULT 0,
    next_run_at     TIMESTAMPTZ     NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Partial index: WAITING/COMPLETED/DEAD rows never appear in scheduler polls.
CREATE INDEX idx_jobs_poll ON jobs (next_run_at ASC, status)
    WHERE status = 'SCHEDULED';

-- Index for stale job detector: finds RUNNING rows older than the lock TTL.
CREATE INDEX idx_jobs_stale ON jobs (updated_at)
    WHERE status = 'RUNNING';

-- ============================================================
-- job_dependencies — DAG edges
-- "job_id depends on depends_on_job_id completing first"
-- A job with rows here starts in WAITING status and is
-- promoted to SCHEDULED by DagEvaluatorService once all
-- depends_on_job_id entries reach COMPLETED.
-- ============================================================
CREATE TABLE job_dependencies (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_job_id   UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    CONSTRAINT uq_dependency UNIQUE (job_id, depends_on_job_id)
);

CREATE INDEX idx_dep_depends_on ON job_dependencies (depends_on_job_id);

-- ============================================================
-- job_executions — append-only audit log
-- ============================================================
CREATE TABLE job_executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID            NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number  INT             NOT NULL,
    status          VARCHAR(20)     NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    worker_id       VARCHAR(255),
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    error_message   TEXT
);

CREATE INDEX idx_executions_job_id ON job_executions (job_id, started_at DESC);

-- ============================================================
-- dead_letter_jobs
-- ============================================================
CREATE TABLE dead_letter_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    last_execution_id   UUID        REFERENCES job_executions(id),
    failure_reason      TEXT,
    failed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
