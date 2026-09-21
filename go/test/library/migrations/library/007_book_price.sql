-- A decimal column for the Shelf.total_price aggregate join SATs.
ALTER TABLE library.book ADD COLUMN price NUMERIC;
