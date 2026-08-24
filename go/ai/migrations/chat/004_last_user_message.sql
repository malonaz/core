-- Track the last user message of a chat for quick access without listing messages.
ALTER TABLE chat
    ADD COLUMN last_user_message TEXT NOT NULL DEFAULT '';
