CREATE TABLE job (
    request_id UUID NOT NULL,
    -- Parents: both NULL for a system job (jobs/{job}), organization_id alone
    -- for organizations/{organization}/jobs/{job}, both for a user's job.
    organization_id TEXT,
    user_id TEXT,
    job_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    labels JSONB,
    payload BYTEA NOT NULL,
    job_type TEXT NOT NULL,
    state SMALLINT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    unique_key TEXT,
    schedule_time TIMESTAMP,
    expire_time TIMESTAMP,
    start_time TIMESTAMP,
    complete_time TIMESTAMP,
    lock_time TIMESTAMP,
    purge_time TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error BYTEA,
    response BYTEA,
    progress BYTEA,
    metadata JSONB,
    -- Job IDs are unique across parents: workers and processors address a job by ID alone.
    PRIMARY KEY (job_id),
    -- Conflict target of the generated insert.
    CONSTRAINT job_identity_unique UNIQUE NULLS NOT DISTINCT (organization_id, user_id, job_id),
    CONSTRAINT job_request_id_unique UNIQUE (request_id),
    CONSTRAINT job_user_within_organization CHECK (user_id IS NULL OR organization_id IS NOT NULL)
);

-- Claim scan: due PENDING (1) jobs, highest priority first, then in due order;
-- a job without a schedule is due at creation.
CREATE INDEX job_claim_idx ON job (priority DESC, (COALESCE(schedule_time, create_time)), create_time) WHERE state = 1;
-- At most one PENDING (1) and one RUNNING (2) job per unique key.
CREATE UNIQUE INDEX job_unique_key_live_idx ON job (unique_key, state) WHERE state IN (1, 2);
-- Expiry reaper: PENDING (1) jobs by expiry.
CREATE INDEX job_expire_idx ON job (expire_time) WHERE state = 1 AND expire_time IS NOT NULL;
-- Lease reaper: RUNNING (2) jobs by lease expiry.
CREATE INDEX job_lease_idx ON job (lock_time) WHERE state = 2;
-- Retention sweeper.
CREATE INDEX job_purge_idx ON job (purge_time) WHERE purge_time IS NOT NULL;
CREATE INDEX job_parent_idx ON job (organization_id, user_id);
CREATE INDEX job_job_type_idx ON job (job_type);
CREATE INDEX job_labels_idx ON job USING GIN (labels);
