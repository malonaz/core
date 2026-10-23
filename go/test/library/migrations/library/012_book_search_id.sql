-- Index each book's resource ID (the `id` search field). A generated column's
-- expression cannot be altered in place: drop it (its index goes with it) and
-- re-add it, which recomputes every row.
-- The expression must match the codegen-emitted BookSearchDocumentExpression.
ALTER TABLE library.book DROP COLUMN search_document;

ALTER TABLE library.book ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(metadata #>> '{summary}', '')), 'B') ||
        setweight(to_tsvector('simple', book_id), 'A')
    ) STORED;

CREATE INDEX book_search_document_idx ON library.book USING gin (search_document);
