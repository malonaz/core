-- The schedule (its resource name) whose tick created the job; NULL for a
-- job created by hand.
ALTER TABLE scheduler.job ADD COLUMN schedule TEXT;

-- Listing a schedule's jobs.
CREATE INDEX job_schedule_idx ON scheduler.job (schedule) WHERE schedule IS NOT NULL;
