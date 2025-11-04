package migrator

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/malonaz/core/go/logging"
	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/go/postgres/migrator/migrations"
)

var log = logging.NewLogger()

// Migrator is database migrator.
type Migrator struct {
	client *postgres.Client
}

// NewMigrator returns a new Migrator.
func NewMigrator(opts *postgres.Opts) (*Migrator, error) {
	client, err := postgres.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &Migrator{client: client}, nil
}

// MustNewMigrator returns a new Migrator and panics on error.
func MustNewMigrator(opts *postgres.Opts) *Migrator {
	migrator, err := NewMigrator(opts)
	if err != nil {
		log.Panicf("Could not create migrator: %v", err)
	}
	return migrator
}

// MustInitializeDatabase initializes a database.
func (m *Migrator) MustInitializeDatabase(ctx context.Context, database, user, password string) {
	if err := m.InitializeDatabase(ctx, database, user, password); err != nil {
		log.Panicf("initializing database: %v", err)
	}
}

// InitializeDatabase initializes a database.
func (m *Migrator) InitializeDatabase(ctx context.Context, database, user, password string) error {
	log.Info("Initializer started")

	// Check if user exists
	var userExists int
	err := m.client.QueryRow(ctx, `SELECT COUNT(1) FROM pg_roles WHERE rolname=$1`, user).Scan(&userExists)
	if err != nil {
		return fmt.Errorf("checking user existence: %w", err)
	}

	// Create user if it doesn't exist
	if userExists == 0 {
		log.Infof("Creating user '%s'", user)
		if _, err = m.client.Exec(ctx, fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`, user, password)); err != nil {
			return fmt.Errorf("creating user: %w", err)
		}
	}

	// Grant user to superuser.
	log.Infof("Granting user '%s' to superuser '%s'", user, m.client.Opts.User)
	if _, err = m.client.Exec(ctx, fmt.Sprintf(`GRANT "%s" TO "%s"`, user, m.client.Opts.User)); err != nil {
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
		log.Infof("Creating database '%s'", database)
		if _, err = m.client.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER "%s"`, database, user)); err != nil {
			return fmt.Errorf("creating database: %w", err)
		}
	}
	log.Info("Initializer shutting down")
	return nil
}

// RunMigrations runs migrations.
func (m *Migrator) RunMigrations(ctx context.Context, fileLoader migrations.FileLoader, migrationsDirectories ...string) error {
	log.Infof("Migrator started")
	if err := m.createMigrationsTableIfNotExist(ctx); err != nil {
		return err
	}
	for _, migrationsDirectory := range migrationsDirectories {
		log.Infof("Running [%s] migrations", filepath.Base(migrationsDirectory))
		if err := m.runMigrations(ctx, fileLoader, migrationsDirectory); err != nil {
			return err
		}
	}
	log.Infof("Migrator shutting down")
	return nil
}

// MustRunMigrations runs migrations or panics.
func (m *Migrator) MustRunMigrations(ctx context.Context, fileLoader migrations.FileLoader, migrationsDirectories ...string) {
	if err := m.RunMigrations(ctx, fileLoader, migrationsDirectories...); err != nil {
		log.Panicf("Error running migrations: %v", err)
	}
}

func (m *Migrator) MustCreateMigrationsTableIfNotExist(ctx context.Context) {
	if err := m.createMigrationsTableIfNotExist(ctx); err != nil {
		log.Panic(err.Error())
	}
}

func (m *Migrator) createMigrationsTableIfNotExist(ctx context.Context) error {
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
			log.Errorf("Could not run migration [%s]", migration.Name())
			return err
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
		log.Infof("Migration [%s] already applied - skipping", migration.Name())
		return nil
	}
	log.Infof("Migration [%s] applied", migration.Name())
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
