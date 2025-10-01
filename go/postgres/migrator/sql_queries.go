package migrator

const creationMigrationTableQuery = `
CREATE TABLE IF NOT EXISTS migration(
  directory TEXT NOT NULL,
  filename TEXT NOT NULL,
  hash TEXT NOT NULL,
  execution_timestamp TIMESTAMP DEFAULT NOW(),
  CONSTRAINT unique_migrations UNIQUE(directory, filename, hash)
)
`
const insertMigrationByHashQuery = `
INSERT INTO migration (directory, filename, hash) VALUES ($1, $2, $3) 
ON CONFLICT(directory, filename, hash) DO NOTHING
`
