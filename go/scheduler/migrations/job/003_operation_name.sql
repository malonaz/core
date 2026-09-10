-- The long-running operation a job backs, when its producer exposes one.
ALTER TABLE job ADD COLUMN operation_name TEXT;

-- One job per operation; the Operations server resolves an operation to its job.
CREATE UNIQUE INDEX job_operation_name_idx ON job (operation_name) WHERE operation_name IS NOT NULL;
