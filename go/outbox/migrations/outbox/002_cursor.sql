-- How far a consumer has applied each resource's events: the journal sequence
-- of the last event it applied, so that it skips older and redelivered ones.
CREATE TABLE outbox.cursor (
    consumer TEXT NOT NULL,
    resource_name TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    PRIMARY KEY (consumer, resource_name)
);
