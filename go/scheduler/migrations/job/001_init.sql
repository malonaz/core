CREATE TABLE job (
    request_id UUID NOT NULL,
    job_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    labels JSONB,
    payload BYTEA NOT NULL,
    job_type TEXT NOT NULL,
    state SMALLINT NOT NULL,
    schedule_time TIMESTAMP,
    start_time TIMESTAMP,
    complete_time TIMESTAMP,
    lock_time TIMESTAMP,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error BYTEA,
    response BYTEA,
    progress BYTEA,
    expire_time TIMESTAMP,
    PRIMARY KEY (job_id),
    CONSTRAINT job_request_id_unique UNIQUE (request_id)
);

-- Claim scan: due PENDING (1) jobs in due order; a job without a schedule is due at creation.
CREATE INDEX job_claim_idx ON job ((COALESCE(schedule_time, create_time)), create_time) WHERE state = 1;
-- Lease reaper: RUNNING (2) jobs by lease expiry.
CREATE INDEX job_lease_idx ON job (lock_time) WHERE state = 2;
-- Retention sweeper.
CREATE INDEX job_expire_idx ON job (expire_time) WHERE expire_time IS NOT NULL;
CREATE INDEX job_job_type_idx ON job (job_type);
CREATE INDEX job_labels_idx ON job USING GIN (labels);
