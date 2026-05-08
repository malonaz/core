package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/logging"
	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/go/postgres/migrator"
)

type optsMinimal struct {
	TargetNamespace string `long:"target-namespace" env:"TARGET_NAMESPACE" description:"Used to dynamically set the TargetPostgres namespace"`
}

var opts struct {
	optsMinimal
	Mode           string         `long:"mode" env:"MODE" description:"init | migrate | reset"`
	Directory      string         `long:"dir" env:"DIR" description:"Directory containing all the migration directories to migrate"`
	Logging        *logging.Opts  `group:"Logging" namespace:"logging" env-namespace:"LOGGING"`
	Postgres       *postgres.Opts `group:"Postgres" namespace:"postgres" env-namespace:"POSTGRES"`
	TargetPostgres *postgres.Opts `group:"Target Postgres" namespace:"target-postgres" env-namespace:"TARGET_POSTGRES"`
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		slog.ErrorContext(ctx, "running", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	optsMinimal := optsMinimal{}
	if err := flags.Parse(&optsMinimal, flags.IgnoreUnknown); err != nil {
		return err
	}
	// Map TARGET_POSTGRES env-namespace to the dynamic namespace derived from TargetNamespace.
	// e.g. if TargetNamespace is "chat-service", prefix is "CHAT_SERVICE_POSTGRES".
	if optsMinimal.TargetNamespace != "" {
		targetPrefix := strings.ToUpper(strings.ReplaceAll(optsMinimal.TargetNamespace, "-", "_")) + "_POSTGRES"
		postgresOptsType := reflect.TypeOf(postgres.Opts{})
		for i := range postgresOptsType.NumField() {
			suffix := postgresOptsType.Field(i).Tag.Get("env")
			if suffix == "" {
				continue
			}
			key := "TARGET_POSTGRES_" + suffix
			if os.Getenv(key) == "" {
				if value := os.Getenv(targetPrefix + "_" + suffix); value != "" {
					os.Setenv(key, value)
				}
			}
		}
	}

	if err := flags.Parse(&opts); err != nil {
		return err
	}
	if err := logging.Init(opts.Logging); err != nil {
		return err
	}

	switch opts.Mode {
	case "init":
		return runInit(ctx)
	case "migrate":
		entries, err := os.ReadDir(opts.Directory)
		if err != nil {
			return fmt.Errorf("reading directory %s: %w", opts.Directory, err)
		}
		var migrationDirectories []string
		for _, entry := range entries {
			if entry.IsDir() {
				migrationDirectories = append(migrationDirectories, fmt.Sprintf("%s/%s", opts.Directory, entry.Name()))
			}
		}
		return runMigrate(ctx, migrationDirectories)
	case "reset":
		return runReset(ctx)
	default:
		return fmt.Errorf("unknown mode: %s", opts.Mode)
	}
}

func runInit(ctx context.Context) error {
	postgresClient := postgres.NewClient(opts.Postgres)
	if err := postgresClient.Start(ctx); err != nil {
		return fmt.Errorf("starting postgres client: %w", err)
	}
	defer postgresClient.Close()

	m := migrator.NewMigrator(postgresClient)
	return m.InitializeDatabase(ctx, opts.TargetPostgres.Database, opts.TargetPostgres.User, opts.TargetPostgres.Password, opts.Postgres.User)
}

func runMigrate(ctx context.Context, migrationDirectories []string) error {
	if len(migrationDirectories) == 0 {
		return fmt.Errorf("migrate requires at least one migration directory")
	}

	postgresClient := postgres.NewClient(opts.TargetPostgres)
	if err := postgresClient.Start(ctx); err != nil {
		return fmt.Errorf("starting postgres client: %w", err)
	}
	defer postgresClient.Close()

	m := migrator.NewMigrator(postgresClient)
	return m.RunMigrations(ctx, os.ReadFile, migrationDirectories...)
}

func runReset(ctx context.Context) error {
	postgresClient := postgres.NewClient(opts.TargetPostgres)
	if err := postgresClient.Start(ctx); err != nil {
		return fmt.Errorf("starting postgres client: %w", err)
	}
	defer postgresClient.Close()

	rows, err := postgresClient.Query(ctx, `
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public'
  `)
	if err != nil {
		return fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("scanning table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating rows: %w", err)
	}

	for _, table := range tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", table)
		if _, err := postgresClient.Exec(ctx, query); err != nil {
			return fmt.Errorf("dropping table %s: %w", table, err)
		}
		slog.InfoContext(ctx, "dropped table", "table", table)
	}

	slog.InfoContext(ctx, "reset complete", "tables_dropped", len(tables))
	return nil
}
