-- IMMUTABLE text of the values a jsonpath selects, required by search
-- documents over repeated JSONB fields (jsonb_path_query is set-returning).
CREATE OR REPLACE FUNCTION core_jsonb_path_text(jsonb, jsonpath) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT coalesce(string_agg(j #>> '{}', ' '), '') FROM jsonb_path_query($1, $2) AS j $$;

-- The search document backing SearchMessages. The expression must match
-- MessageSearchDocumentExpression emitted by the postgres codegen.
ALTER TABLE message ADD COLUMN search_document tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', left(core_jsonb_path_text(blocks, 'lax $[*].text'), 100000)), 'A')
) STORED;

CREATE INDEX message_search_document_idx ON message USING gin (search_document);
