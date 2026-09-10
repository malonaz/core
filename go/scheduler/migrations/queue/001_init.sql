CREATE TABLE scheduler.queue (
    request_id UUID NOT NULL,
    queue_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    state SMALLINT NOT NULL,
    policy JSONB NOT NULL,
    handlers JSONB NOT NULL,
    PRIMARY KEY (queue_id),
    CONSTRAINT queue_request_id_unique UNIQUE (request_id)
);

-- Target delete guard: handlers referencing a target.
CREATE INDEX queue_handlers_idx ON scheduler.queue USING GIN (handlers);
