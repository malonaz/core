-- malonaz/test/library/migrations/library/001_init.sql
CREATE SCHEMA IF NOT EXISTS library;

CREATE TABLE library.author (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    display_name TEXT NOT NULL,
    biography TEXT NOT NULL,
    email_address TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    email_addresses TEXT[] NOT NULL,
    phone_numbers TEXT[],
    labels JSONB,
    etag TEXT NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (organization_id, author_id),
    CONSTRAINT author_request_id_unique UNIQUE (request_id)
);

CREATE TABLE library.shelf (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    shelf_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    display_name TEXT NOT NULL,
    genre SMALLINT NOT NULL,
    ext_id TEXT,
    correlation_id TEXT NOT NULL,
    duration BIGINT,
    labels JSONB,
    legacy_meta JSONB NOT NULL,
    PRIMARY KEY (organization_id, shelf_id),
    CONSTRAINT shelf_request_id_unique UNIQUE (request_id)
);

CREATE TABLE library.book (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    shelf_id TEXT NOT NULL,
    book_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    title TEXT NOT NULL,
    author TEXT NOT NULL,
    isbn TEXT NOT NULL,
    publication_year INT NOT NULL,
    page_count INT NOT NULL,
    duration BIGINT NOT NULL,
    labels JSONB,
    etag TEXT NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (organization_id, shelf_id, book_id),
    FOREIGN KEY (organization_id, shelf_id) REFERENCES library.shelf(organization_id, shelf_id),
    CONSTRAINT book_request_id_unique UNIQUE (request_id)
);
