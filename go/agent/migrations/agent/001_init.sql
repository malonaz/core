CREATE SCHEMA IF NOT EXISTS agent;

CREATE TABLE agent.agent (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    title TEXT NOT NULL DEFAULT '',
    instructions TEXT NOT NULL,
    model TEXT NOT NULL,
    state SMALLINT NOT NULL,
    chat TEXT NOT NULL DEFAULT '',
    metadata JSONB,
    PRIMARY KEY (organization_id, user_id, agent_id),
    CONSTRAINT agent_request_id_unique UNIQUE (request_id)
);

-- Runners claim ACTIVE agents oldest-wake-first with FOR UPDATE SKIP LOCKED.
CREATE INDEX agent_claim_idx ON agent.agent (state, update_time) WHERE delete_time IS NULL;

CREATE TABLE agent.task (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    title TEXT NOT NULL DEFAULT '',
    goal TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    parent_task TEXT NOT NULL DEFAULT '',
    root_task TEXT NOT NULL DEFAULT '',
    state SMALLINT NOT NULL,
    chat TEXT NOT NULL DEFAULT '',
    report TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL,
    PRIMARY KEY (organization_id, user_id, agent_id, task_id),
    CONSTRAINT task_request_id_unique UNIQUE (request_id)
);

-- The table is the work queue: runners claim PENDING tasks oldest-first with
-- SELECT ... FOR UPDATE SKIP LOCKED. update_time doubles as the heartbeat for
-- reaping stale RUNNING tasks.
CREATE INDEX task_claim_idx ON agent.task (state, create_time) WHERE delete_time IS NULL;

-- Tree lookups (ListTasks filter on root_task) and cancel cascades.
CREATE INDEX task_root_task_idx ON agent.task (root_task) WHERE delete_time IS NULL;
CREATE INDEX task_parent_task_idx ON agent.task (parent_task) WHERE delete_time IS NULL;

CREATE TABLE agent.memory (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    content JSONB NOT NULL,
    source_chat TEXT NOT NULL DEFAULT '',
    source_task TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (organization_id, user_id, agent_id, memory_id),
    CONSTRAINT memory_request_id_unique UNIQUE (request_id)
);
