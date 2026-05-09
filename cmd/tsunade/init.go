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
	binPath := "postgres-migrator"
	if env == "" {
		binPath = "plz-out/bin/cmd/postgres-migrator/postgres-migrator"
	}
	for _, database := range databases {
		dir := "/etc/tsunade/migrations"
		if env == "" {
			dir = "plz-out/gen/sgpt/migrations"
		}
		initializer := binary.MustNew(
			binPath,
			fmt.Sprintf("--logging.format=%s", opts.Logging.Format),
			fmt.Sprintf("--logging.field=binary:%s", database),
			"--mode", "init",
			"--target-namespace", database,
			"--dir", dir,
		).WithLogger(rawLogger).AsJob()
		migrator := binary.MustNew(
			binPath,
			fmt.Sprintf("--logging.format=%s", opts.Logging.Format),
			fmt.Sprintf("--logging.field=binary:%s", database),
			"--mode", "migrate",
			"--target-namespace", database,
			"--dir", dir,
		).WithLogger(rawLogger).AsJob()

		if err := initializer.Run(); err != nil {
			return fmt.Errorf("running %s db initializer: %w", database, err)
		}
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
