CREATE TABLE tool_call (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    -- The resource id: the call's position among its message's tool calls, from 1.
    position TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    etag TEXT NOT NULL,
    labels JSONB,
    annotations JSONB,
    tool_name TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    arguments JSONB NOT NULL,
    -- NULL while the call is pending.
    result JSONB,
    PRIMARY KEY (organization_id, user_id, chat_id, message_id, position),
    CONSTRAINT tool_call_request_id_unique UNIQUE (request_id)
);

-- A user's pending calls, newest first: the attention queue across chats.
-- Settling a call by the id its result names.
CREATE INDEX tool_call_chat_tool_call_id_idx ON tool_call (organization_id, user_id, chat_id, tool_call_id);
CREATE INDEX tool_call_user_pending_idx ON tool_call (organization_id, user_id, create_time DESC) WHERE result IS NULL;
