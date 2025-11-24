package migrator

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/go/postgres/migrator/migrations"
)

// Migrator is database migrator.
type Migrator struct {
	log    *slog.Logger
	client *postgres.Client
}

// NewMigrator returns a new Migrator.
func NewMigrator(client *postgres.Client) *Migrator {
	return &Migrator{
		log:    slog.Default(),
		client: client,
	}
}

func (m *Migrator) WithLogger(logger *slog.Logger) *Migrator {
	m.log = logger
	return m
}

// InitializeDatabase initializes a database.
func (m *Migrator) InitializeDatabase(ctx context.Context, database, user, password, superUser string) error {
	m.log = m.log.WithGroup("initializer").With("database", database, "user", user, "super_user", superUser)
	m.log.InfoContext(ctx, "starting")

	// Check if user exists
	var userExists int
	err := m.client.QueryRow(ctx, `SELECT COUNT(1) FROM pg_roles WHERE rolname=$1`, user).Scan(&userExists)
	if err != nil {
		return fmt.Errorf("checking user existence: %w", err)
	}

	// Create user if it doesn't exist
	if userExists == 0 {
		m.log.InfoContext(ctx, "creating user")
		if _, err = m.client.Exec(ctx, fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`, user, password)); err != nil {
			return fmt.Errorf("creating user: %w", err)
		}
	}

	// Grant user to superuser.
	m.log.InfoContext(ctx, "granting user to superuser")
	if _, err = m.client.Exec(ctx, fmt.Sprintf(`GRANT "%s" TO "%s"`, user, superUser)); err != nil {
		return fmt.Errorf("granting user to superuser: %w", err)
	}

	// Check if database exists
	var dbExists int
	err = m.client.QueryRow(ctx, `SELECT COUNT(1) FROM pg_database WHERE datname=$1`, database).Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("checking database existence: %w", err)
	}

	// Create database if it doesn't exist
	if dbExists == 0 {
		m.log.InfoContext(ctx, "creating database")
		if _, err = m.client.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER "%s"`, database, user)); err != nil {
			return fmt.Errorf("creating database: %w", err)
		}
	}
	m.log.InfoContext(ctx, "initializer shutting down")
	return nil
}

// RunMigrations runs migrations.
func (m *Migrator) RunMigrations(ctx context.Context, fileLoader migrations.FileLoader, migrationsDirectories ...string) error {
	m.log = m.log.WithGroup("migrator")
	m.log.InfoContext(ctx, "started")
	if err := m.CreateMigrationsTableIfNotExist(ctx); err != nil {
		return err
	}
	for _, migrationsDirectory := range migrationsDirectories {
		m.log.InfoContext(ctx, "running migrations", "dir", filepath.Base(migrationsDirectory))
		if err := m.runMigrations(ctx, fileLoader, migrationsDirectory); err != nil {
			return err
		}
	}
	m.log.InfoContext(ctx, "shutting down")
	return nil
}

func (m *Migrator) CreateMigrationsTableIfNotExist(ctx context.Context) error {
	if _, err := m.client.Exec(ctx, creationMigrationTableQuery); err != nil {
		return fmt.Errorf("could not create migration table: %w", err)
	}
	return nil
}

func (m *Migrator) runMigrations(ctx context.Context, fileLoader migrations.FileLoader, migrationDirectory string) error {
	migrations, err := migrations.GetMigrations(fileLoader, migrationDirectory)
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		if err := m.runMigration(ctx, migration); err != nil {
			return fmt.Errorf("running migration [%s]: %w", migration.Name(), err)
		}
	}
	return nil
}

func (m *Migrator) runMigration(ctx context.Context, migration *migrations.Migration) error {
	ok, err := m.applyMigration(ctx, migration)
	if err != nil {
		return fmt.Errorf("could not execute migration [%s]: %w", migration.Name(), err)
	}
	if !ok {
		m.log.InfoContext(ctx, "migration already applied - skipping", "migration", migration.Name())
		return nil
	}
	m.log.InfoContext(ctx, "migration applied", "migration", migration.Name())
	return nil
}

func (m *Migrator) applyMigration(ctx context.Context, migration *migrations.Migration) (bool, error) {
	alreadyApplied := false
	transactionFN := func(tx postgres.Tx) error {
		result, err := tx.Exec(ctx, insertMigrationByHashQuery, migration.Directory, migration.Filename, migration.Hash)
		if err != nil {
			return err
		}
		alreadyApplied = result.RowsAffected() != 1
		if alreadyApplied {
			return nil
		}
		_, err = tx.Exec(ctx, migration.SQLQuery)
		return err
	}
	return !alreadyApplied, m.client.ExecuteTransaction(ctx, postgres.Serializable, transactionFN)
}
