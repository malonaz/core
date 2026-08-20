-- Search document for books: a joined resource (shelf_genre etc.), covering
-- qualified snippet expressions in Search queries.
-- The expression must match the codegen-emitted BookSearchDocumentExpression.
ALTER TABLE library.book ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(metadata #>> '{summary}', '')), 'B')
    ) STORED;

CREATE INDEX book_search_document_idx ON library.book USING gin (search_document);
