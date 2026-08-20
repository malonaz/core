-- Bookmark table: backs the Bookmark resource and the Book query joins
-- (latest_bookmark / first_bookmark).
CREATE TABLE library.bookmark (
    organization_id TEXT NOT NULL,
    shelf_id TEXT NOT NULL,
    book_id TEXT NOT NULL,
    bookmark_id TEXT NOT NULL,
    request_id UUID NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    page_number INT NOT NULL,
    display_name TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    color SMALLINT NOT NULL DEFAULT 0,
    labels JSONB,
    etag TEXT NOT NULL,
    PRIMARY KEY (organization_id, shelf_id, book_id, bookmark_id),
    FOREIGN KEY (organization_id, shelf_id, book_id) REFERENCES library.book(organization_id, shelf_id, book_id),
    CONSTRAINT bookmark_request_id_unique UNIQUE (request_id)
);
