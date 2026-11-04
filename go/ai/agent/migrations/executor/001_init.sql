CREATE TABLE executor (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    labels JSONB,
    display_name TEXT NOT NULL,
    max_agents INTEGER NOT NULL,
    tools TEXT[] NOT NULL,
    state SMALLINT NOT NULL,
    connect_time TIMESTAMP,
    heartbeat_time TIMESTAMP,
    agent_count INTEGER NOT NULL,
    -- Internal: the ConnectExecutor stream currently speaking for the executor. NULL while disconnected.
    session_id UUID,
    PRIMARY KEY (organization_id, user_id, executor_id),
    CONSTRAINT executor_request_id_unique UNIQUE (request_id)
);

-- The heartbeat sweep.
CREATE INDEX executor_heartbeat_time_idx ON executor (heartbeat_time) WHERE state = 1;
