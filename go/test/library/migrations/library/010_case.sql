-- Backs the keyword SATs: a resource named after a Go keyword (and a reserved SQL word).
CREATE TABLE library.case_ (
    request_id UUID NOT NULL,
    organization_id TEXT NOT NULL,
    case_id TEXT NOT NULL,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    delete_time TIMESTAMP,
    display_name TEXT NOT NULL,
    etag TEXT NOT NULL,
    PRIMARY KEY (organization_id, case_id),
    CONSTRAINT case_request_id_unique UNIQUE (request_id)
);
