CREATE TABLE scheduler.schedule (
    request_id UUID NOT NULL,
    -- Parents: both NULL for a system schedule (schedules/{schedule}),
    -- organization_id alone for organizations/{organization}/schedules/{schedule},
    -- both for a user's schedule.
    organization_id TEXT,
    user_id TEXT,
    schedule_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    labels JSONB,
    payload BYTEA NOT NULL,
    -- The queue's resource name (queues/{queue}) and the handler method the
    -- payload type selected in it.
    queue TEXT NOT NULL,
    method TEXT NOT NULL,
    cron TEXT NOT NULL,
    time_zone TEXT NOT NULL,
    state SMALLINT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    -- Nanoseconds.
    run_window BIGINT,
    last_schedule_time TIMESTAMP,
    next_schedule_time TIMESTAMP,
    last_job TEXT,
    missed_tick_count INTEGER NOT NULL DEFAULT 0,
    -- Schedule IDs are unique across parents: the tick routine addresses a schedule by ID alone.
    PRIMARY KEY (schedule_id),
    -- Conflict target of the generated insert.
    CONSTRAINT schedule_identity_unique UNIQUE NULLS NOT DISTINCT (organization_id, user_id, schedule_id),
    CONSTRAINT schedule_request_id_unique UNIQUE (request_id),
    CONSTRAINT schedule_user_within_organization CHECK (user_id IS NULL OR organization_id IS NOT NULL)
);

-- Tick scan: due ENABLED (1) schedules in due order.
CREATE INDEX schedule_tick_idx ON scheduler.schedule (next_schedule_time) WHERE state = 1;
CREATE INDEX schedule_parent_idx ON scheduler.schedule (organization_id, user_id);
CREATE INDEX schedule_queue_idx ON scheduler.schedule (queue);
CREATE INDEX schedule_labels_idx ON scheduler.schedule USING GIN (labels);
