CREATE TABLE input (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    input_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    message JSONB NOT NULL,
    consume_time TIMESTAMP,
    PRIMARY KEY (organization_id, user_id, agent_id, input_id),
    CONSTRAINT input_request_id_unique UNIQUE (request_id)
);

CREATE INDEX input_pending_idx ON input (organization_id, user_id, agent_id, create_time, input_id) WHERE consume_time IS NULL;
