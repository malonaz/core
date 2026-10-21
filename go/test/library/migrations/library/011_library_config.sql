-- A lazy singleton: no parent table, rows appear on first update.
CREATE TABLE library.library_config (
    organization_id TEXT NOT NULL PRIMARY KEY,
    create_time TIMESTAMP NOT NULL,
    update_time TIMESTAMP NOT NULL,
    etag TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    loan_period_days INT NOT NULL DEFAULT 0,
    labels JSONB
);
