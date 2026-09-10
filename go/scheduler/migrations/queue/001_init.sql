CREATE TABLE scheduler.queue (
    request_id UUID NOT NULL,
    queue_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    state SMALLINT NOT NULL,
    service TEXT NOT NULL,
    method TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    request_type TEXT NOT NULL,
    response_type TEXT NOT NULL,
    policy JSONB NOT NULL,
    PRIMARY KEY (queue_id),
    CONSTRAINT queue_request_id_unique UNIQUE (request_id),
    -- One queue per method, and a payload type routes to exactly one queue.
    CONSTRAINT queue_method_unique UNIQUE (service, method),
    CONSTRAINT queue_request_type_unique UNIQUE (request_type)
);
