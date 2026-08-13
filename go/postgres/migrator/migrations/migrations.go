package migrations

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

// FileLoader loads a file and returns bytes.
type FileLoader func(string) ([]byte, error)

// Migration is the database representation of migration.
type Migration struct {
	Directory          string    `db:"directory"`
	Filename           string    `db:"filename"`
	Hash               string    `db:"hash"`
	ExecutionTimestamp time.Time `db:"execution_timestamp"`
	SQLQuery           string
	ExpectedHash       string
}

// Name returns a "{directory}:{filename}" string for clear/consistent logging.
func (m *Migration) Name() string {
	return m.Directory + ":" + m.Filename
}

// File is used to parse a database manifest: a single ordered list of every
// migration in the database, each entry a "{directory}/{filename}" path. This
// is the total ordering of the database's migrations.
type File struct {
	Migrations []struct {
		Path string `yaml:"path"`
		Hash string `yaml:"hash"`
	}
}

// ComputeMigrationHash computes the md5 hash of a migration file
func ComputeMigrationHash(str string) string {
	hash := md5.New()
	io.WriteString(hash, str)
	hashInBytes := hash.Sum(nil)
	return hex.EncodeToString(hashInBytes)

}

// GetMigrations loads every migration listed in a database manifest,
// preserving the order in which they are declared.
func GetMigrations(fileLoader FileLoader, databaseDirectory string) ([]*Migration, error) {
	bytes, err := fileLoader(databaseDirectory + "/manifest.yaml")
	if err != nil {
		return nil, fmt.Errorf("could not read manifest: %w", err)
	}
	migrationsFile := File{}
	if err := yaml.Unmarshal(bytes, &migrationsFile); err != nil {
		return nil, fmt.Errorf("could not parse manifest: %w", err)
	}

	migrations := make([]*Migration, 0, len(migrationsFile.Migrations))
	for _, migration := range migrationsFile.Migrations {
		// Filenames are not unique across directories, and the migration ledger
		// is keyed on (directory, filename, hash), so the directory component
		// must be preserved exactly as the per-directory scheme recorded it.
		directory, filename, found := strings.Cut(migration.Path, "/")
		if !found {
			return nil, fmt.Errorf("migration path %q must be of the form {directory}/{filename}", migration.Path)
		}
		migrationFileBytes, err := fileLoader(databaseDirectory + "/" + migration.Path)
		if err != nil {
			return nil, fmt.Errorf("could not open migration %s: %w", migration.Path, err)
		}
		sqlQuery := string(migrationFileBytes)
		migrations = append(migrations, &Migration{
			Directory:    directory,
			Filename:     filename,
			SQLQuery:     sqlQuery,
			Hash:         ComputeMigrationHash(sqlQuery),
			ExpectedHash: migration.Hash,
		})
	}
	return migrations, nil
}
