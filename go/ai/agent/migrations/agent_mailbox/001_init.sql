-- Hand-written: counts the messages sent to an agent, queued in its chat, against those its turns delivered.
CREATE TABLE agent_mailbox (
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    -- Bumped by every message sent to the agent, once the message is queued in its chat.
    sent_count BIGINT NOT NULL,
    -- The sent_count read before a turn's latest delivery: every message it counts was delivered.
    delivered_count BIGINT NOT NULL,
    PRIMARY KEY (organization_id, user_id, agent_id)
);

-- The claim loop's scan for idle agents with undelivered messages.
CREATE INDEX agent_mailbox_undelivered_idx ON agent_mailbox (organization_id, user_id, agent_id) WHERE sent_count > delivered_count;
