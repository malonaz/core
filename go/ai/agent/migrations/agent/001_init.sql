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
    state SMALLINT NOT NULL,
    last_turn_outcome SMALLINT NOT NULL,
    status JSONB,
    -- Must match AgentSearchDocumentExpression, emitted by the postgres codegen.
    search_document tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(labels #>> '{}', '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(spec #>> '{instructions}', '')), 'C')
    ) STORED,
    PRIMARY KEY (organization_id, user_id, agent_id),
    CONSTRAINT agent_request_id_unique UNIQUE (request_id)
);

-- The claim loop's takeover scan.
CREATE INDEX agent_running_idx ON agent (update_time) WHERE delete_time IS NULL AND state = 2;
CREATE INDEX agent_parent_agent_idx ON agent (parent_agent) WHERE delete_time IS NULL AND parent_agent <> '';
CREATE INDEX agent_root_agent_idx ON agent (root_agent, create_time);
-- In production, prefer a composite index scoping token lookups to the organization (requires the btree_gin
-- extension, missing from the test Postgres): USING gin (organization_id, search_document).
CREATE INDEX agent_search_document_idx ON agent USING gin (search_document);
