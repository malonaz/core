-- Hand-written: events are not model resources. An event's id is its sequence.
CREATE TABLE event (
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    -- The name of the event's `kind` oneof field, e.g. turn_started.
    kind TEXT NOT NULL,
    -- The event, as binary proto.
    event BYTEA NOT NULL,
    PRIMARY KEY (organization_id, user_id, agent_id, sequence)
);

-- Finds the start of an agent's latest turn.
CREATE INDEX event_turn_started_idx ON event (organization_id, user_id, agent_id, sequence) WHERE kind = 'turn_started';
