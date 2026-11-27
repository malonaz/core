CREATE TABLE workspace.snapshot (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    filesystem_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    trigger SMALLINT NOT NULL,
    turn TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    added TEXT[] NOT NULL,
    changed TEXT[] NOT NULL,
    deleted TEXT[] NOT NULL,
    PRIMARY KEY (organization_id, user_id, filesystem_id, snapshot_id),
    CONSTRAINT snapshot_request_id_unique UNIQUE (request_id)
);

-- ListSnapshots, scoped to a filesystem, by create_time.
CREATE INDEX snapshot_filesystem_create_time_idx ON workspace.snapshot (organization_id, user_id, filesystem_id, create_time);
