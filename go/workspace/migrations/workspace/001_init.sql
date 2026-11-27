-- Own schema, so the workspace services can share a database with other services' tables.
CREATE SCHEMA workspace;

CREATE TABLE workspace.workspace (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    labels JSONB,
    mounts JSONB NOT NULL,
    state SMALLINT NOT NULL,
    node TEXT NOT NULL,
    last_active_time TIMESTAMP,
    start_time TIMESTAMP,
    PRIMARY KEY (organization_id, user_id, workspace_id),
    CONSTRAINT workspace_request_id_unique UNIQUE (request_id)
);

-- ListWorkspaces, scoped to a user, in its orderings.
CREATE INDEX workspace_create_time_idx ON workspace.workspace (organization_id, user_id, create_time);
CREATE INDEX workspace_update_time_idx ON workspace.workspace (organization_id, user_id, update_time);
-- The idle reaper's scan: workspaces in a state, least recently active first.
CREATE INDEX workspace_state_last_active_time_idx ON workspace.workspace (state, last_active_time);
