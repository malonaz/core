CREATE TABLE definition (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    definition_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    spec JSONB NOT NULL,
    PRIMARY KEY (organization_id, definition_id),
    CONSTRAINT definition_request_id_unique UNIQUE (request_id)
);
