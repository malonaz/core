-- Queue stats read the jobs that completed in an interval, of one queue or of every queue.
CREATE INDEX job_complete_idx ON scheduler.job (complete_time, queue) WHERE complete_time IS NOT NULL;
