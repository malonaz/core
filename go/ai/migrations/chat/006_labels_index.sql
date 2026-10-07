-- IF NOT EXISTS: large databases build it CONCURRENTLY ahead of deploy, as migrations are transactional.
CREATE INDEX IF NOT EXISTS chat_labels_idx ON chat USING GIN (labels);
