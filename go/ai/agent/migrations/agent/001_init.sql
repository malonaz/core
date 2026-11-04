CREATE TABLE agent (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    annotations JSONB,
    title TEXT NOT NULL,
    definition TEXT NOT NULL,
    spec JSONB,
    parent_agent TEXT NOT NULL,
    root_agent TEXT NOT NULL,
    chat TEXT NOT NULL,
    executor TEXT NOT NULL,
    leased_executor TEXT NOT NULL,
    state SMALLINT NOT NULL,
    last_turn_outcome SMALLINT NOT NULL,
    status JSONB,
    last_event_sequence BIGINT NOT NULL,
    PRIMARY KEY (organization_id, user_id, agent_id),
    CONSTRAINT agent_request_id_unique UNIQUE (request_id)
);

-- An executor's live agents: agent counts, releases, re-adoption on reconnect, orphan sweeps.
CREATE INDEX agent_leased_executor_idx ON agent (leased_executor) WHERE delete_time IS NULL AND leased_executor <> '';
-- The placement queue.
CREATE INDEX agent_pending_idx ON agent (organization_id, user_id, create_time) WHERE delete_time IS NULL AND state = 1;
CREATE INDEX agent_parent_agent_idx ON agent (parent_agent) WHERE delete_time IS NULL AND parent_agent <> '';
CREATE INDEX agent_root_agent_idx ON agent (root_agent, create_time);
