// Package sat provides a System Acceptance Test framework that orchestrates
// postgres, database migrations, and services under test (SUTs).
package sat

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstestserver "github.com/nats-io/nats-server/v2/test"

	"github.com/malonaz/core/go/binary"
	"github.com/malonaz/core/go/logging"
	"github.com/malonaz/core/go/nats"
	natsteststore "github.com/malonaz/core/go/nats/test_store"
	"github.com/malonaz/core/go/postgres"
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
	Nats                 bool
	Debug                bool

	// NatsSubjects are buffered by [SAT.NatsStore] for the lifetime of the run, so that a test
	// can assert on events the RPCs triggering them do not wait for. They may contain the NATS
	// wildcards `*` and `>`. Requires Nats.
	NatsSubjects []string
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
	natsServer     *natsserver.Server
	natsOptions    *natsserver.Options
	natsClient     *nats.Client
	natsClientOnce sync.Once

	// NatsStore buffers everything published to Config.NatsSubjects, or is nil if none were
	// given. It subscribes before any SUT starts, so events emitted during boot are caught too.
	NatsStore *natsteststore.Nats

	postgresClientsMutex sync.Mutex
	postgresClients      map[string]*postgres.Client
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
		log:             slog.Default(),
		config:          config,
		postgresClients: map[string]*postgres.Client{},
	}
}

func (s *SAT) GetNatsClient(ctx context.Context) (*nats.Client, error) {
	var err error
	s.natsClientOnce.Do(func() {
		natsOpts := &nats.Opts{
			Host:           s.natsOptions.Host,
			Port:           s.natsOptions.Port,
			TotalWait:      30 * time.Second,
			ReconnectDelay: time.Second,
		}
		s.natsClient, err = nats.NewClient(natsOpts)
		if err != nil {
			err = fmt.Errorf("creating nats client: %w", err)
			return
		}
		if err = s.natsClient.Start(ctx); err != nil {
			err = fmt.Errorf("starting nats client: %w", err)
			return
		}
	})
	return s.natsClient, err
}

// GetPostgresClient returns a started client connected to the given database on this SAT's
// Postgres server, as its superuser, which can reach any database the migrator created.
// Clients are cached per database and closed by [SAT.Cleanup], so callers need not close them.
func (s *SAT) GetPostgresClient(ctx context.Context, database string) (*postgres.Client, error) {
	if s.PostgresServer == nil {
		return nil, fmt.Errorf("no postgres server: this SAT was configured without a migrator")
	}

	s.postgresClientsMutex.Lock()
	defer s.postgresClientsMutex.Unlock()
	if client, ok := s.postgresClients[database]; ok {
		return client, nil
	}

	// Opts returns a fresh copy holding the server's host, port and superuser credentials.
	opts := s.PostgresServer.Opts()
	opts.Database = database
	opts.SSLMode = "disable"
	client := postgres.NewClient(opts)
	if err := client.Start(ctx); err != nil {
		return nil, fmt.Errorf("starting postgres client for database %s: %w", database, err)
	}
	s.postgresClients[database] = client
	return client, nil
}

func (s *SAT) Start(ctx context.Context) error {
	// Instantiate raw logger.
	loggingOpts := &logging.Opts{
		Level:    logging.LevelDebug,
		Format:   logging.FormatRaw,
		FilePath: "sat.log",
	}
	if s.config.Debug {
		loggingOpts.FilePath = ""
	}
	rawLogger, err := logging.NewLogger(loggingOpts)
	if err != nil {
		return fmt.Errorf("instantiating raw logger: %w", err)
	}

	s.log.Info("instantiating SAT", "PATH", os.Getenv("PATH"))

	if s.config.Nats {
		s.log.InfoContext(ctx, "starting nats server")
		s.natsOptions = &natstestserver.DefaultTestOptions
		s.natsOptions.NoLog = false
		s.natsOptions.JetStream = true
		s.natsServer = natstestserver.RunServer(s.natsOptions)
		environmentVariables["NATS_HOST"] = s.natsOptions.Host
		environmentVariables["NATS_PORT"] = strconv.Itoa(s.natsOptions.Port)
	}

	// Merge caller-provided env vars into the global set, then export them all.
	for k, v := range s.config.EnvironmentVariables {
		environmentVariables[k] = v
	}
	for k, v := range environmentVariables {
		os.Setenv(k, v)
	}

	// Subscribed before the SUTs start, so that nothing they publish while booting is missed.
	// These are plain NATS subscriptions, so the streams need not exist yet.
	if len(s.config.NatsSubjects) > 0 {
		if !s.config.Nats {
			return fmt.Errorf("nats subjects were configured without nats")
		}
		natsClient, err := s.GetNatsClient(ctx)
		if err != nil {
			return fmt.Errorf("connecting to nats: %w", err)
		}
		s.NatsStore, err = natsteststore.NewNats(natsClient, s.config.NatsSubjects...)
		if err != nil {
			return fmt.Errorf("subscribing to nats: %w", err)
		}
	}

	// If a migrator is configured, we need a Postgres server, an initializer, and a migrator.
	if s.config.Migrator.Name != "" {
		serverConfig := postgrestestserver.Config{
			Host:     s.config.PostgresServerConfig.Host,
			Port:     s.config.PostgresServerConfig.Port,
			User:     s.config.PostgresServerConfig.User,
			Password: s.config.PostgresServerConfig.Password,
		}
		s.PostgresServer = postgrestestserver.NewServer(serverConfig).WithLogger(rawLogger)

		databaseInitializerBinary, err := binary.New(s.config.Initializer.Path, s.config.Initializer.Args...)
		if err != nil {
			return fmt.Errorf("instantiate database initializer binary: %w", err)
		}
		databaseInitializerBinary.WithLogger(rawLogger).WithName(s.config.Initializer.Name)

		databaseMigratorBinary, err := binary.New(s.config.Migrator.Path, s.config.Migrator.Args...)
		if err != nil {
			return fmt.Errorf("instantiate database migrator binary: %w", err)
		}
		databaseMigratorBinary.WithLogger(rawLogger).WithName(s.config.Migrator.Name)

		if err := s.PostgresServer.Start(ctx); err != nil {
			return fmt.Errorf("running postgres server: %w", err)
		}
		if err := databaseInitializerBinary.Run(); err != nil {
			return fmt.Errorf("running database initializer: %w", err)
		}
		if err := databaseMigratorBinary.Run(); err != nil {
			return fmt.Errorf("running database migrator: %w", err)
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

	s.sutsWorker = binary.NewWorker("suts", sutBinaries).WithLogger(rawLogger).OnExit(func(err error) {
		if err != nil {
			panic(fmt.Errorf("SUTs worker errored out: %w", err))
		}
	})
	if err := s.sutsWorker.RunAsync(); err != nil {
		return fmt.Errorf("running SUTs worker: %w", err)
	}
	return nil
}

// Cleanup tears down everything Start brought up. It is safe to call on a partially started
// SAT, which is what a failing Start leaves behind.
func (s *SAT) Cleanup() {
	if s.sutsWorker != nil {
		s.sutsWorker.Stop()
	}
	s.postgresClientsMutex.Lock()
	for _, client := range s.postgresClients {
		client.Close()
	}
	clear(s.postgresClients)
	s.postgresClientsMutex.Unlock()
	if s.PostgresServer != nil {
		s.PostgresServer.Shutdown()
	}
	if s.NatsStore != nil {
		s.NatsStore.Close()
	}
	if s.natsServer != nil {
		s.natsServer.Shutdown()
	}
}
