-- Messages move to their own `message` table; the chat becomes a lightweight
-- container with an aggregated price. Annotations move to proto bytes.
ALTER TABLE chat
    DROP COLUMN metadata,
    DROP COLUMN annotations,
    ADD COLUMN annotations BYTEA,
    ADD COLUMN price DOUBLE PRECISION NOT NULL DEFAULT 0;
