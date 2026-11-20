-- Queue stats read nothing but these columns: an index-only scan, rather than a heap fetch per
-- completed job, which made a week's stats take seconds.
-- IF NOT EXISTS: large databases build it, and drop the index it replaces, CONCURRENTLY ahead of
-- deploy, as migrations are transactional and a plain build blocks every write to the job table.
CREATE INDEX IF NOT EXISTS job_stats_idx ON scheduler.job (complete_time, queue) INCLUDE (state, attempt_count, start_time, schedule_time, create_time) WHERE complete_time IS NOT NULL;
DROP INDEX IF EXISTS scheduler.job_complete_idx;
