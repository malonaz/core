-- A unique key's slots follow its job's run rather than its state: at most one
-- live job not yet started (the trailing run) and one started, running or
-- awaiting a retry, per key. A retry then stays in its slot.
DROP INDEX scheduler.job_unique_key_live_idx;
CREATE UNIQUE INDEX job_unique_key_live_idx ON scheduler.job (unique_key, (start_time IS NULL)) WHERE state IN (1, 2);
