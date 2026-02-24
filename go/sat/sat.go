// Package sat provides a System Acceptance Test framework that orchestrates
// postgres, database migrations, and services under test (SUTs).
package sat

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/malonaz/core/go/binary"
	"github.com/malonaz/core/go/logging"
	postgrestestserver "github.com/malonaz/core/go/postgres/test_server"
)

// SUT represents a Service Under Test, i.e. a binary that will be started
// and monitored during the acceptance test run.
type SUT struct {
	Name string
	Path string
	Port int
	Args []string
}

// Config holds all configuration needed to set up a SAT environment,
// including the services to test, database setup binaries, and environment variables.
type Config struct {
	SUTS                 []SUT
	Initializer          SUT
	Migrator             SUT
	PostgresServerConfig PostgresServerConfig
	EnvironmentVariables map[string]string
}

// PostgresServerConfig holds connection details for the test Postgres instance.
type PostgresServerConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

// SAT orchestrates the lifecycle of all binaries and the Postgres server
// required for system acceptance testing.
type SAT struct {
	log            *slog.Logger
	config         *Config
	sutsWorker     *binary.Worker
	PostgresServer *postgrestestserver.Server
}

// WithLogger sets this SAT's logger.
func (s *SAT) WithLogger(logger *slog.Logger) *SAT {
	s.log = logger
	return s
}

// New creates a SAT instance. It starts Postgres (if a migrator is configured),
// runs the database initializer and migrator jobs, then starts all SUTs.
func New(config *Config) *SAT {
	return &SAT{
		log:    slog.Default(),
		config: config,
	}
}

func (s *SAT) Start(ctx context.Context) error {
	// Instantiate raw logger.
	loggingOpts := &logging.Opts{
		Level:    logging.LevelDebug,
		Format:   logging.FormatRaw,
		FilePath: "/tmp/sat.log",
	}
	rawLogger, err := logging.NewLogger(loggingOpts)
	if err != nil {
		return fmt.Errorf("instantiating raw logger: %w", err)
	}

	s.log.Info("instantiating SAT", "PATH", os.Getenv("PATH"))

	// Merge caller-provided env vars into the global set, then export them all.
	for k, v := range s.config.EnvironmentVariables {
		environmentVariables[k] = v
	}
	for k, v := range environmentVariables {
		os.Setenv(k, v)
	}

	// If a migrator is configured, we need a Postgres server, an initializer, and a migrator.
	if s.config.Migrator.Name != "" {
		serverConfig := postgrestestserver.Config{
			Host:     s.config.PostgresServerConfig.Host,
			Port:     s.config.PostgresServerConfig.Port,
			User:     s.config.PostgresServerConfig.User,
			Password: s.config.PostgresServerConfig.Password,
		}
		postgresServer, err := postgrestestserver.NewServer(serverConfig, rawLogger)
		if err != nil {
			return fmt.Errorf("instantiating postgres: %w", err)
		}
		s.PostgresServer = postgresServer

		// binary.New takes (path, args...) — we use WithName to set the display name separately.
		databaseInitializerBinary, err := binary.New(s.config.Initializer.Path, s.config.Initializer.Args...)
		if err != nil {
			return fmt.Errorf("instantiate database initializer binary: %w", err)
		}
		databaseInitializerBinary.WithLogger(rawLogger).WithName(s.config.Initializer.Name).AsJob()

		databaseMigratorBinary, err := binary.New(s.config.Migrator.Path, s.config.Migrator.Args...)
		if err != nil {
			return fmt.Errorf("instantiate database migrator binary: %w", err)
		}
		databaseMigratorBinary.WithLogger(rawLogger).WithName(s.config.Migrator.Name).AsJob()

		// Start Postgres first, then run the initializer and migrator as sequential jobs.
		if err := s.PostgresServer.Run(); err != nil {
			return fmt.Errorf("running postgres server: %w", err)
		}
		if err := databaseInitializerBinary.RunAsJob(); err != nil {
			return fmt.Errorf("running database initializer: %w", err)
		}
		if err := databaseMigratorBinary.RunAsJob(); err != nil {
			return fmt.Errorf("running database migrator binary: %w", err)
		}
	}

	// Create and start all SUTs concurrently via a Worker.
	sutBinaries := make([]*binary.Binary, 0, len(s.config.SUTS))
	for _, sut := range s.config.SUTS {
		sutBinary, err := binary.New(sut.Path, sut.Args...)
		if err != nil {
			return fmt.Errorf("instantiating %s: %w", sut.Name, err)
		}
		sutBinaries = append(sutBinaries, sutBinary.WithName(sut.Name).WithPort(sut.Port))
	}
	s.sutsWorker = binary.NewWorker("suts", sutBinaries).WithLogger(rawLogger)
	s.sutsWorker.Run()
	return nil
}

// Cleanup gracefully shuts down all SUTs and the Postgres server.
func (s *SAT) Cleanup() {
	s.sutsWorker.Exit()
	if s.PostgresServer != nil {
		s.PostgresServer.Shutdown()
	}
}
