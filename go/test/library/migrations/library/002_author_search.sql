-- malonaz/test/library/migrations/library/002_author_search.sql

-- IMMUTABLE wrapper around array_to_string, required by tsvector search
-- documents over array columns (array_to_string itself is only STABLE).
CREATE OR REPLACE FUNCTION core_array_to_string(text[], text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT array_to_string($1, $2) $$;

-- Emits every suffix (length >= 3) of each space-separated phone number's
-- digits-only form, so any fragment of a number prefix-matches a token
-- (tsquery has no substring matching). Required by SPLIT_PHONE_NUMBER fields.
CREATE OR REPLACE FUNCTION core_phone_number_tokens(text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
SELECT string_agg(substr(d.digits, s.pos), ' ')
FROM unnest(string_to_array($1, ' ')) AS e(elem)
CROSS JOIN LATERAL (SELECT regexp_replace(e.elem, '[^0-9]', '', 'g') AS digits) d
CROSS JOIN LATERAL generate_series(1, length(d.digits) - 2) AS s(pos)
$$;

-- Search document. The expression must match AuthorSearchDocumentExpression
-- emitted by the postgres codegen.
ALTER TABLE library.author ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(display_name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(email_address, '') ||
        ' ' ||
        translate(coalesce(email_address, ''), '@._-+', '     ')), 'B') ||
        setweight(to_tsvector('simple', coalesce(phone_number, '') ||
        ' ' ||
        coalesce(core_phone_number_tokens(coalesce(phone_number, '')), '')), 'B') ||
        setweight(to_tsvector('simple', core_array_to_string(email_addresses, ' ') ||
        ' ' ||
        translate(core_array_to_string(email_addresses, ' '), '@._-+', '     ')), 'C') ||
        setweight(to_tsvector('simple', core_array_to_string(coalesce(phone_numbers, ARRAY[]::text[]), ' ') ||
        ' ' ||
        coalesce(core_phone_number_tokens(core_array_to_string(coalesce(phone_numbers, ARRAY[]::text[]), ' ')), '')), 'C') ||
        setweight(to_tsvector('simple', coalesce(metadata #>> '{country}', '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(metadata #>> '{email_addresses}', '') ||
        ' ' ||
        translate(coalesce(metadata #>> '{email_addresses}', ''), '@._-+', '     ')), 'C') ||
        setweight(to_tsvector('simple', coalesce(biography, '')), 'D')
    ) STORED;

-- In production, prefer a composite index scoping token lookups to the parent
-- (requires the btree_gin extension, unavailable in the SAT toolchain postgres):
--   CREATE EXTENSION IF NOT EXISTS btree_gin;
--   CREATE INDEX ... USING gin (organization_id, search_document);
CREATE INDEX author_search_document_idx ON library.author USING gin (search_document);
