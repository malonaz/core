-- malonaz/test/library/migrations/library/003_author_search_metadata.sql

-- Rebuild the search document to index as_json_bytes metadata paths
-- (metadata.country, metadata.email_addresses). Generated columns cannot be
-- altered in place: drop and re-add, copying the regenerated
-- AuthorSearchDocumentExpression.
DROP INDEX library.author_search_document_idx;
ALTER TABLE library.author DROP COLUMN search_document;

ALTER TABLE library.author ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(display_name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(email_address, '') || ' ' || translate(coalesce(email_address, ''), '@._-+', '     ')), 'B') ||
        setweight(to_tsvector('simple', coalesce(phone_number, '') || ' ' || regexp_replace(coalesce(phone_number, ''), '[^0-9 ]', '', 'g')), 'B') ||
        setweight(to_tsvector('simple', core_array_to_string(email_addresses, ' ') || ' ' || translate(core_array_to_string(email_addresses, ' '), '@._-+', '     ')), 'C') ||
        setweight(to_tsvector('simple', core_array_to_string(coalesce(phone_numbers, ARRAY[]::text[]), ' ') || ' ' || regexp_replace(core_array_to_string(coalesce(phone_numbers, ARRAY[]::text[]), ' '), '[^0-9 ]', '', 'g')), 'C') ||
        setweight(to_tsvector('simple', coalesce(metadata #>> '{country}', '')), 'B') ||
        setweight(to_tsvector('simple', coalesce(metadata #>> '{email_addresses}', '') || ' ' || translate(coalesce(metadata #>> '{email_addresses}', ''), '@._-+', '     ')), 'C') ||
        setweight(to_tsvector('simple', coalesce(biography, '')), 'D')
    ) STORED;

CREATE INDEX author_search_document_idx ON library.author USING gin (search_document);
