-- DATE columns backing the google.type.Date codegen SATs: one nullable, one
-- NOT NULL.
ALTER TABLE library.shelf ADD COLUMN opened_date DATE;
ALTER TABLE library.bookmark ADD COLUMN placed_date DATE NOT NULL;
