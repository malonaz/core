-- The outbox journal of the library schema: one row per event a write
-- journaled in its own transaction, cleared once the relay has handed it to
-- the scheduler. The id is a UUIDv7, so ordering by it is ordering by write
-- time, and it doubles as the request id of the job the relay creates.
CREATE TABLE library.journal (
    id UUID PRIMARY KEY,
    create_time TIMESTAMP NOT NULL,
    resource_name TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event BYTEA NOT NULL
);
