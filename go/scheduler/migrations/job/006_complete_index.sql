-- Queue stats read the jobs that completed in an interval, of one queue or of every queue.
-- IF NOT EXISTS: large databases build it CONCURRENTLY ahead of deploy, as migrations are
-- transactional and a plain build blocks every write to the job table.
CREATE INDEX IF NOT EXISTS job_complete_idx ON scheduler.job (complete_time, queue) WHERE complete_time IS NOT NULL;
