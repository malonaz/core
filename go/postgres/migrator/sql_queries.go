package migrator

// Explicitly schema-qualified: relying on search_path breaks when a schema
// matching the connecting user's name is created by a migration.
const creationMigrationTableQuery = `
CREATE TABLE IF NOT EXISTS public.migration(
  directory TEXT NOT NULL,
  filename TEXT NOT NULL,
  hash TEXT NOT NULL,
  execution_timestamp TIMESTAMP DEFAULT NOW(),
  CONSTRAINT unique_migrations UNIQUE(directory, filename, hash)
)
`
const insertMigrationByHashQuery = `
INSERT INTO public.migration (directory, filename, hash) VALUES ($1, $2, $3)
ON CONFLICT(directory, filename, hash) DO NOTHING
`
