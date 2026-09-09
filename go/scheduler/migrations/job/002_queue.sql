-- Routing moves from the payload type to the job's queue; no scheduler has
-- been deployed, so the table is empty wherever this runs.
ALTER TABLE job
    DROP COLUMN job_type,
    -- The queue's resource name: queues/{queue}.
    ADD COLUMN queue TEXT NOT NULL,
    ADD COLUMN method TEXT NOT NULL;

DROP INDEX job_claim_idx;

-- Claim scan: due PENDING (1) jobs ranked within their queue, highest priority
-- first, then in due order; a job without a schedule is due at creation.
CREATE INDEX job_claim_idx ON job (queue, priority DESC, (COALESCE(schedule_time, create_time)), create_time) WHERE state = 1;
-- Per-queue running counts, backlog stats and the queue delete guard: live
-- (PENDING (1) or RUNNING (2)) jobs by queue.
CREATE INDEX job_live_idx ON job (queue, state) WHERE state IN (1, 2);
CREATE INDEX job_queue_idx ON job (queue);
CREATE INDEX job_method_idx ON job (method);
