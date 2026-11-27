CREATE TABLE workspace.filesystem (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    filesystem_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    labels JSONB,
    kind SMALLINT NOT NULL,
    quota_bytes BIGINT NOT NULL,
    used_bytes BIGINT NOT NULL,
    attachments JSONB NOT NULL,
    latest_snapshot TEXT NOT NULL,
    PRIMARY KEY (organization_id, user_id, filesystem_id),
    CONSTRAINT filesystem_request_id_unique UNIQUE (request_id)
);

-- ListFilesystems, scoped to a user, in its orderings.
CREATE INDEX filesystem_create_time_idx ON workspace.filesystem (organization_id, user_id, create_time);
CREATE INDEX filesystem_update_time_idx ON workspace.filesystem (organization_id, user_id, update_time);
