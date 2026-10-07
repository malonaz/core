-- Lets the claim scan skip future-scheduled PENDING jobs instead of reading them every poll.
CREATE INDEX job_due_idx ON scheduler.job ((COALESCE(schedule_time, create_time))) WHERE state = 1;
