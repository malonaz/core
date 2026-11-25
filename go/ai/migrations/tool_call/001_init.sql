CREATE TABLE tool_call (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    -- The call's position among its message's tool calls, from 1.
    tool_call_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    annotations JSONB,
    call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    title TEXT NOT NULL,
    state SMALLINT NOT NULL,
    error JSONB,
    result_message TEXT NOT NULL,
    PRIMARY KEY (organization_id, user_id, chat_id, message_id, tool_call_id),
    CONSTRAINT tool_call_request_id_unique UNIQUE (request_id)
);

-- A user's calls by state, newest first: the attention queue across chats.
CREATE INDEX tool_call_user_state_idx ON tool_call (organization_id, user_id, state, create_time DESC);
-- Settling a call by the id its result names.
CREATE INDEX tool_call_chat_call_id_idx ON tool_call (organization_id, user_id, chat_id, call_id);
