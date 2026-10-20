-- IF NOT EXISTS: large databases build it CONCURRENTLY ahead of deploy, as migrations are transactional.
CREATE INDEX IF NOT EXISTS message_create_time_idx ON message (create_time DESC) WHERE delete_time IS NULL;
