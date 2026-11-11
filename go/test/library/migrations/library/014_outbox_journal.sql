-- The outbox journal of the library schema: one row per NATS message a write
-- published in its own transaction, deleted once the poller has published it.
-- create_time is the writing transaction's start time, which orders the poll.
CREATE TABLE library.journal (
    id UUID PRIMARY KEY,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    stream TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload BYTEA NOT NULL
);

CREATE INDEX journal_create_time_id_idx ON library.journal (create_time, id);
