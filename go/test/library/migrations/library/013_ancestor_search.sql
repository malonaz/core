-- Search documents for shelves and bookmarks: a book is also searched by its
-- shelf's document, a bookmark by its book's, joined at query time.
-- The expressions must match the codegen-emitted ShelfSearchDocumentExpression
-- and BookmarkSearchDocumentExpression.
ALTER TABLE library.shelf ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(display_name, '')), 'A')
    ) STORED;

CREATE INDEX shelf_search_document_idx ON library.shelf USING gin (search_document);

-- No index: a search joining an ancestor's document matches every row under
-- the searched parent.
ALTER TABLE library.bookmark ADD COLUMN search_document tsvector
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce(display_name, '')), 'A') ||
        setweight(to_tsvector('simple', coalesce(note, '')), 'B')
    ) STORED;
