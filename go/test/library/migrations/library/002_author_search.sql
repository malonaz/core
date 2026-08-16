-- malonaz/test/library/migrations/library/002_author_search.sql

-- IMMUTABLE wrapper around array_to_string, required by tsvector search
-- documents over array columns (array_to_string itself is only STABLE).
CREATE OR REPLACE FUNCTION core_array_to_string(text[], text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT array_to_string($1, $2) $$;

-- Search document. The expression must match AuthorSearchDocumentExpression
-- emitted by the postgres codegen.
ALTER TABLE library.author ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(display_name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(email_address, '') || ' ' || translate(coalesce(email_address, ''), '@._-+', '     ')), 'B') ||
        setweight(to_tsvector('simple', coalesce(phone_number, '') || ' ' || regexp_replace(coalesce(phone_number, ''), '[^0-9 ]', '', 'g')), 'B') ||
        setweight(to_tsvector('simple', core_array_to_string(email_addresses, ' ') || ' ' || translate(core_array_to_string(email_addresses, ' '), '@._-+', '     ')), 'C') ||
        setweight(to_tsvector('simple', core_array_to_string(coalesce(phone_numbers, ARRAY[]::text[]), ' ') || ' ' || regexp_replace(core_array_to_string(coalesce(phone_numbers, ARRAY[]::text[]), ' '), '[^0-9 ]', '', 'g')), 'C') ||
        setweight(to_tsvector('simple', coalesce(biography, '')), 'D')
    ) STORED;

-- In production, prefer a composite index scoping token lookups to the parent
-- (requires the btree_gin extension, unavailable in the SAT toolchain postgres):
--   CREATE EXTENSION IF NOT EXISTS btree_gin;
--   CREATE INDEX ... USING gin (organization_id, search_document);
CREATE INDEX author_search_document_idx ON library.author USING gin (search_document);
