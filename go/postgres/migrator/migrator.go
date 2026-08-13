package migrator

import (
	"context"
	"fmt"
	"log/slog"

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

	// Check if user exists.
	var userExists int
	if err := m.client.QueryRow(ctx, `SELECT COUNT(1) FROM pg_roles WHERE rolname=$1`, user).Scan(&userExists); err != nil {
		return fmt.Errorf("checking user existence: %w", err)
	}

	// Create user if it doesn't exist.
	if userExists == 0 {
		m.log.InfoContext(ctx, "creating user")
		if _, err := m.client.Exec(ctx, fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`, user, password)); err != nil {
			return fmt.Errorf("creating user: %w", err)
		}
	}

	// Grant user to superuser.
	m.log.InfoContext(ctx, "granting user to superuser")
	if _, err := m.client.Exec(ctx, fmt.Sprintf(`GRANT "%s" TO "%s"`, user, superUser)); err != nil {
		return fmt.Errorf("granting user to superuser: %w", err)
	}

	// Check if database exists.
	var dbExists int
	if err := m.client.QueryRow(ctx, `SELECT COUNT(1) FROM pg_database WHERE datname=$1`, database).Scan(&dbExists); err != nil {
		return fmt.Errorf("checking database existence: %w", err)
	}

	// Create database if it doesn't exist.
	if dbExists == 0 {
		m.log.InfoContext(ctx, "creating database")
		if _, err := m.client.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER "%s"`, database, user)); err != nil {
			return fmt.Errorf("creating database: %w", err)
		}
	}

	// Grant CREATE on the database so the regular user can install trusted
	// extensions (e.g. pgcrypto, pg_trgm) without superuser. Requires Postgres 13+.
	m.log.InfoContext(ctx, "granting create on database")
	if _, err := m.client.Exec(ctx, fmt.Sprintf(`GRANT CREATE ON DATABASE "%s" TO "%s"`, database, user)); err != nil {
		return fmt.Errorf("granting create on database: %w", err)
	}

	m.log.InfoContext(ctx, "initializer shutting down")
	return nil
}

// RunMigrations runs every migration listed in the database manifest, in the
// single total order it declares.
func (m *Migrator) RunMigrations(ctx context.Context, fileLoader migrations.FileLoader, databaseDirectory string) error {
	m.log = m.log.WithGroup("migrator")
	m.log.InfoContext(ctx, "started")
	if err := m.CreateMigrationsTableIfNotExist(ctx); err != nil {
		return err
	}
	databaseMigrations, err := migrations.GetMigrations(fileLoader, databaseDirectory)
	if err != nil {
		return err
	}
	for _, migration := range databaseMigrations {
		if err := m.runMigration(ctx, migration); err != nil {
			return fmt.Errorf("running migration [%s]: %w", migration.Name(), err)
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

func (m *Migrator) runMigration(ctx context.Context, migration *migrations.Migration) error {
	// The ledger is keyed on the file's content hash, so an edit to an
	// already-applied migration does not conflict and would silently re-execute
	// the whole file against a live database. Migrations are append-only.
	if migration.ExpectedHash != "" && migration.ExpectedHash != migration.Hash {
		return fmt.Errorf(
			"migration [%s] was modified after being recorded in the manifest (manifest: %s, file: %s): migrations are append-only, add a new migration instead",
			migration.Name(), migration.ExpectedHash, migration.Hash,
		)
	}
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
	if err := m.client.ExecuteTransaction(ctx, postgres.Serializable, transactionFN); err != nil {
		return false, err
	}
	return !alreadyApplied, nil
}
