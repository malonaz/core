package migrations

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
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

// File is used to parse migrations files.
type File struct {
	Migrations []struct {
		Filename string `yaml:"filename"`
		Hash     string `yaml:"hash"`
	}
}

// ParseMigrationsFile parses a migration file into a MigrationFile.
func ParseMigrationsFile(fileLoader FileLoader, migrationDirectory string) (File, error) {
	migrationsFile := File{}
	bytes, err := fileLoader(migrationDirectory + "_migrations.yaml")
	if err != nil {
		return migrationsFile, err
	}
	if err := yaml.Unmarshal(bytes, &migrationsFile); err != nil {
		return migrationsFile, err
	}
	return migrationsFile, nil
}

// ComputeMigrationHash computes the md5 hash of a migration file
func ComputeMigrationHash(str string) string {
	hash := md5.New()
	io.WriteString(hash, str)
	hashInBytes := hash.Sum(nil)
	return hex.EncodeToString(hashInBytes)

}

// GetMigrations loads all migrations from the given directory into an array of Migrations.
func GetMigrations(fileLoader FileLoader, migrationDirectory string) ([]*Migration, error) {
	migrationsFile, err := ParseMigrationsFile(fileLoader, migrationDirectory)
	if err != nil {
		return nil, fmt.Errorf("could not parse migrations file: %w", err)
	}

	migrations := make([]*Migration, 0, len(migrationsFile.Migrations))
	for _, migration := range migrationsFile.Migrations {
		migrationFileBytes, err := fileLoader(migrationDirectory + "/" + migration.Filename)
		if err != nil {
			return nil, fmt.Errorf("could not open migration %s/%s: %w", migrationDirectory, migration.Filename, err)
		}
		sqlQuery := string(migrationFileBytes)
		migrations = append(migrations, &Migration{
			Directory:    filepath.Base(migrationDirectory),
			Filename:     migration.Filename,
			SQLQuery:     sqlQuery,
			Hash:         ComputeMigrationHash(sqlQuery),
			ExpectedHash: migration.Hash,
		})
	}
	return migrations, nil
}
