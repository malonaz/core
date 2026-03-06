package main

import (
	"context"
	"fmt"
	"os"

	"github.com/malonaz/core/go/binary"
	"github.com/malonaz/core/go/logging"
)

var databases = []string{
	"ai",
}

func setup(ctx context.Context) error {
	loggingOpts := &logging.Opts{
		Level:  opts.Logging.Level,
		Format: logging.FormatRaw,
	}
	rawLogger, err := logging.NewLogger(loggingOpts)
	if err != nil {
		return fmt.Errorf("instantiating raw logger: %w", err)
	}

	env := os.Getenv("ENV")
	for _, database := range databases {
		initializerPath, migratorPath := localPaths(database)
		if env != "" {
			initializerPath, migratorPath = remotePaths(database)
		}

		loggingArgs := func(name string) []string {
			return []string{
				fmt.Sprintf("--logging.format=%s", opts.Logging.Format),
				fmt.Sprintf("--logging.field=binary:%s", name),
			}
		}

		initializerName := fmt.Sprintf("postgres_initializer_job_%s", database)
		initializer := binary.MustNew(initializerPath, loggingArgs(initializerName)...)
		initializer.WithName(initializerName).WithLogger(rawLogger)
		if err := initializer.Run(); err != nil {
			return fmt.Errorf("running %s db initializer: %w", database, err)
		}

		migratorName := fmt.Sprintf("postgres_migrator_job_%s", database)
		migrator := binary.MustNew(migratorPath, loggingArgs(migratorName)...)
		migrator.WithName(migratorName).WithLogger(rawLogger)
		if err := migrator.Run(); err != nil {
			return fmt.Errorf("running %s db migrator: %w", database, err)
		}
	}
	return nil
}

func localPaths(database string) (string, string) {
	return fmt.Sprintf("plz-out/bin/go/%s/migrations/initializer", database),
		fmt.Sprintf("plz-out/bin/go/%s/migrations/migrator", database)
}

func remotePaths(database string) (string, string) {
	return fmt.Sprintf("tsunade-%s-postgres-initializer", database),
		fmt.Sprintf("tsunade-%s-postgres-migrator", database)
}
