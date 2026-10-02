-- Queue stats count FAILED jobs alongside live ones, so the queue/state index covers them too.
DROP INDEX scheduler.job_live_idx;
CREATE INDEX job_queue_state_idx ON scheduler.job (queue, state) WHERE state IN (1, 2, 4);
