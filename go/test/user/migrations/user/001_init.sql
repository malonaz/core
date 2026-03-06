-- malonaz/test/user/migrations/user/001_init.sql
CREATE TABLE organization (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    display_name TEXT NOT NULL,
    labels JSONB,
    etag TEXT NOT NULL,
    PRIMARY KEY (organization_id),
    CONSTRAINT organization_request_id_unique UNIQUE (request_id)
);

CREATE TABLE user_ (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    display_name TEXT NOT NULL,
    email_address TEXT NOT NULL,
    phone_number_renamed TEXT NOT NULL,
    labels JSONB,
    etag TEXT NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (organization_id, id),
    FOREIGN KEY (organization_id) REFERENCES organization(organization_id),
    CONSTRAINT user_request_id_unique UNIQUE (request_id)
);
