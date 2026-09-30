-- The search document backing SearchChats. The expression must match
-- ChatSearchDocumentExpression emitted by the postgres codegen.
ALTER TABLE chat ADD COLUMN search_document tsvector GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', coalesce(title, '')), 'A')
) STORED;

CREATE INDEX chat_search_document_idx ON chat USING gin (search_document);
