-- Serves Chat.last_generated_message: a chat's newest generated message in one probe.
-- IF NOT EXISTS: large databases build it CONCURRENTLY ahead of deploy, as migrations are transactional.
CREATE INDEX IF NOT EXISTS message_last_generated_idx
    ON message (organization_id, user_id, chat_id, create_time DESC NULLS LAST, message_id)
    WHERE model IS NOT NULL;
