package migrator

import (
	"fmt"
	"context"
	"path/filepath"

	"github.com/pkg/errors"

	"go/logging"
	"go/postgres"
	"go/postgres/migrator/migrations"
)

var log = logging.NewLogger()

// Migrator is database migrator.
type Migrator struct {
	client *postgres.Client
}

// NewMigrator returns a new Migrator.
func NewMigrator(opts postgres.Opts) (*Migrator, error) {
	client, err := postgres.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &Migrator{client: client}, nil
}

// MustNewMigrator returns a new Migrator and panics on error.
func MustNewMigrator(opts postgres.Opts) *Migrator {
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
	log.Info("Initializer stated")
	if _, err := m.client.Exec(ctx, fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD '%s'`, user, password)); err != nil {
		return errors.Wrap(err, "creating user")
	}
	if _, err := m.client.Exec(ctx, fmt.Sprintf(`GRANT "%s" TO "%s"`, user, m.client.Opts.User)); err != nil {
		return errors.Wrap(err, "granting user to superuser")
	}
	if _, err := m.client.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER "%s"`, database, user)); err != nil {
		return errors.Wrap(err, "creating database")
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

func (m *Migrator) createMigrationsTableIfNotExist(ctx context.Context) error {
	if _, err := m.client.Exec(ctx, creationMigrationTableQuery); err != nil {
		return errors.Wrap(err, "could not create migration table")
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
		return errors.Wrapf(err, "could not execute migration [%s]", migration.Name())
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
