-- Messages move to their own `message` table; the chat becomes a lightweight
-- container with an aggregated price.
ALTER TABLE chat
    DROP COLUMN metadata,
    ADD COLUMN price DOUBLE PRECISION NOT NULL DEFAULT 0;
