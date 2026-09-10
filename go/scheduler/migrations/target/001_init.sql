CREATE TABLE scheduler.target (
    request_id UUID NOT NULL,
    target_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    url TEXT NOT NULL,
    headers JSONB,
    PRIMARY KEY (target_id),
    CONSTRAINT target_request_id_unique UNIQUE (request_id)
);
