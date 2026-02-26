CREATE TABLE chat (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    metadata BYTEA NOT NULL,
    PRIMARY KEY (organization_id, user_id, chat_id),
    CONSTRAINT chat_request_id_unique UNIQUE (request_id)
);
