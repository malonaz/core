-- The outbox journal of a database: one row per NATS message a write published
-- in its own transaction, deleted once the poller has published it.
CREATE SCHEMA outbox;

CREATE TABLE outbox.journal (
    -- Orders the poll. Assigned as the row is written, under the lock the write
    -- holds on its resource, where a transaction's start time is not.
    sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The message's Nats-Msg-Id.
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    stream TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload BYTEA NOT NULL
) WITH (
    -- Every row is inserted then deleted: vacuum on a fixed count of dead rows,
    -- not a fraction of a backlog, and unthrottled since the live table is tiny.
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_cost_delay = 0
);
