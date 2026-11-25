CREATE TABLE tool (
    request_id UUID NOT NULL,
    -- NULL for a platform tool (tools/{tool}), else organizations/{organization}/tools/{tool}.
    organization_id TEXT,
    tool_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    labels JSONB,
    annotations JSONB,
    description TEXT NOT NULL,
    json_schema JSONB,
    -- Conflict target of the generated insert.
    CONSTRAINT tool_identity_unique UNIQUE NULLS NOT DISTINCT (organization_id, tool_id),
    CONSTRAINT tool_request_id_unique UNIQUE (request_id)
);

CREATE INDEX tool_labels_idx ON tool USING GIN (labels);
