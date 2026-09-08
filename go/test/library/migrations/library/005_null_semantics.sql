-- Nullable shelf fields backing the AIP-160 unset-field (NULL) semantics SATs.
ALTER TABLE library.shelf ADD COLUMN secondary_genre SMALLINT;
ALTER TABLE library.shelf ADD COLUMN shelf_number INT;
ALTER TABLE library.shelf ADD COLUMN featured BOOLEAN;
ALTER TABLE library.shelf ADD COLUMN extra JSONB;
