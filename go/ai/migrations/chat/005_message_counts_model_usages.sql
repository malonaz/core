-- Per-role live message counts and per-model aggregated usage, so chat lists
-- and stats no longer need to scan messages.
ALTER TABLE chat
    ADD COLUMN system_message_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN assistant_message_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN user_message_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN tool_message_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN model_usages JSONB NOT NULL DEFAULT '[]'::jsonb;

-- Backfill counts from live messages (Role enum: 1 system, 2 assistant, 3 user, 4 tool).
UPDATE chat c
SET system_message_count = counts.system_count,
    assistant_message_count = counts.assistant_count,
    user_message_count = counts.user_count,
    tool_message_count = counts.tool_count
FROM (
    SELECT organization_id, user_id, chat_id,
        COUNT(*) FILTER (WHERE role = 1)::integer AS system_count,
        COUNT(*) FILTER (WHERE role = 2)::integer AS assistant_count,
        COUNT(*) FILTER (WHERE role = 3)::integer AS user_count,
        COUNT(*) FILTER (WHERE role = 4)::integer AS tool_count
    FROM message
    WHERE delete_time IS NULL
    GROUP BY 1, 2, 3
) counts
WHERE c.organization_id = counts.organization_id
  AND c.user_id = counts.user_id
  AND c.chat_id = counts.chat_id;

-- Backfill model_usages from the same messages that fed chat.price: successful
-- generations, deleted or not. Each ModelUsage keeps only the categories the
-- model actually consumed, mirroring what the service writes.
WITH category(name) AS (
    VALUES ('input_token'), ('output_token'), ('output_reasoning_token'),
           ('input_token_cache_read'), ('input_token_cache_write'),
           ('input_second'), ('output_second'), ('input_character'),
           ('input_image_token'), ('output_image_token'),
           ('input_image_token_cache_read'), ('input_image_token_cache_write')
),
consumption AS (
    SELECT m.organization_id, m.user_id, m.chat_id, m.model, category.name,
        SUM(COALESCE((m.model_usage -> category.name ->> 'quantity')::bigint, 0)) AS quantity,
        SUM(COALESCE((m.model_usage -> category.name ->> 'price')::double precision, 0)) AS price
    FROM message m
    CROSS JOIN category
    WHERE m.model IS NOT NULL AND m.model_usage IS NOT NULL AND m.status IS NULL
    GROUP BY 1, 2, 3, 4, 5
),
model_usage AS (
    SELECT organization_id, user_id, chat_id, model,
        jsonb_build_object('model', model) || COALESCE(
            jsonb_object_agg(name, jsonb_build_object('quantity', quantity, 'price', price))
                FILTER (WHERE quantity <> 0 OR price <> 0),
            '{}'::jsonb
        ) AS usage
    FROM consumption
    GROUP BY 1, 2, 3, 4
),
chat_usage AS (
    SELECT organization_id, user_id, chat_id,
        jsonb_agg(usage ORDER BY model) AS model_usages
    FROM model_usage
    GROUP BY 1, 2, 3
)
UPDATE chat c
SET model_usages = chat_usage.model_usages
FROM chat_usage
WHERE c.organization_id = chat_usage.organization_id
  AND c.user_id = chat_usage.user_id
  AND c.chat_id = chat_usage.chat_id;
