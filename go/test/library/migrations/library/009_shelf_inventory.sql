-- Backs the correlated-filter join SATs: books added since the last inventory.
ALTER TABLE library.shelf ADD COLUMN inventory_time TIMESTAMP;
