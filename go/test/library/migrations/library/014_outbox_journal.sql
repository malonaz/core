-- The outbox journal of the library schema: one row per NATS message a write
-- published in its own transaction, deleted once the poller has published it.
-- create_time is the writing transaction's start time, which orders the poll.
CREATE TABLE library.journal (
    id UUID PRIMARY KEY,
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

CREATE INDEX journal_create_time_id_idx ON library.journal (create_time, id);
