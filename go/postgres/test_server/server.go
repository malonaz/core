// Package testserver runs a lightweight Postgres server for testing.
//
// A [Server] manages the full lifecycle of a temporary Postgres instance:
// initialization, configuration, startup, and shutdown. The data directory
// is removed on shutdown, ensuring test isolation.
//
// [RunWithPostgres] is a convenience function for TestMain that starts a
// server, runs migrations, executes tests, and cleans up:
//
//	func TestMain(m *testing.M) {
//	    testserver.RunWithPostgres(ctx, m, logger, &client,
//	        extensionLoader, extensionDirs,
//	        migrationLoader, migrationDirs,
//	    )
//	}
package testserver

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/malonaz/core/go/binary"
	"github.com/malonaz/core/go/postgres"
	"github.com/malonaz/core/go/postgres/migrator"
	"github.com/malonaz/core/go/postgres/migrator/migrations"
)

const (
	defaultHost          = "localhost"
	defaultPort          = 5432
	defaultDatabase      = "postgres"
	defaultUser          = "postgres"
	defaultPassword      = "postgres"
	defaultMaxConns      = 10
	defaultDataDirectory = "/tmp/db"

	socketDirectory = "postgres_socket"
	configFilename  = "postgresql.conf"
	logFilename     = "postgresql.log"
)

// Config holds configuration for a test Postgres [Server].
type Config struct {
	Host          string
	Port          int
	User          string
	Database      string
	Password      string
	MaxConns      int
	DataDirectory string
}

// applyDefaults fills any zero-valued fields with sensible defaults.
func (c *Config) applyDefaults() {
	if c.Host == "" {
		c.Host = defaultHost
	}
	if c.Port == 0 {
		c.Port = defaultPort
	}
	if c.User == "" {
		c.User = defaultUser
	}
	if c.Database == "" {
		c.Database = defaultDatabase
	}
	if c.Password == "" {
		c.Password = defaultPassword
	}
	if c.MaxConns == 0 {
		c.MaxConns = defaultMaxConns
	}
	if c.DataDirectory == "" {
		c.DataDirectory = defaultDataDirectory
	}
}

// Server controls the lifecycle of a temporary Postgres instance.
type Server struct {
	log         *slog.Logger
	config      Config
	postgresDir string
}

// NewServer returns a new [Server] with the given configuration.
// Zero-valued config fields are populated with defaults.
func NewServer(config Config) *Server {
	config.applyDefaults()
	return &Server{
		log:         slog.Default(),
		config:      config,
		postgresDir: getPostgresBinaryDir(),
	}
}

func (s *Server) WithLogger(log *slog.Logger) *Server {
	s.log = log
	return s
}

// Opts returns the [postgres.Opts] needed to connect to this server.
func (s *Server) Opts() *postgres.Opts {
	return &postgres.Opts{
		Host:     s.config.Host,
		Port:     s.config.Port,
		User:     s.config.User,
		Database: s.config.Database,
		Password: s.config.Password,
		MaxConns: s.config.MaxConns,
	}
}

// Client returns a new [postgres.Client] configured for this server.
func (s *Server) Client() *postgres.Client {
	return postgres.NewClient(s.Opts())
}

// Start initializes and starts the Postgres instance. It creates the data
// directory via initdb, writes a tuned-for-testing configuration, and
// starts the server. The call blocks until Postgres is accepting connections.
func (s *Server) Start(ctx context.Context) error {
	if err := s.initDatabase(); err != nil {
		return fmt.Errorf("init database: %w", err)
	}
	if err := s.writeConfig(); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if err := s.createSocketDirectory(); err != nil {
		return fmt.Errorf("create socket directory: %w", err)
	}
	if err := s.startPostgres(); err != nil {
		return fmt.Errorf("start postgres: %w", err)
	}
	return nil
}

// Shutdown stops the Postgres instance and removes the data directory.
func (s *Server) Shutdown() error {
	if err := s.stopPostgres(); err != nil {
		return fmt.Errorf("stop postgres: %w", err)
	}
	if err := os.RemoveAll(s.config.DataDirectory); err != nil {
		return fmt.Errorf("remove data directory %s: %w", s.config.DataDirectory, err)
	}
	return nil
}

// initDatabase runs initdb to create the data directory.
func (s *Server) initDatabase() error {
	initJob, err := binary.New(
		s.binaryPath("initdb"),
		"--no-locale", "--encoding=UTF8", "--nosync",
		"-D", s.config.DataDirectory,
		"--auth", "trust",
		"-U", s.config.User,
	)
	if err != nil {
		return err
	}
	return initJob.WithName("postgres-initdb").WithLogger(s.log).Run()
}

// startPostgres runs pg_ctl start with -w to block until the server is
// accepting connections.
func (s *Server) startPostgres() error {
	startJob, err := binary.New(
		s.binaryPath("pg_ctl"),
		"-D", s.config.DataDirectory,
		"-l", filepath.Join(s.config.DataDirectory, logFilename),
		"-w", "start",
	)
	if err != nil {
		return err
	}
	return startJob.WithName("postgres-start").WithLogger(s.log).Run()
}

// stopPostgres runs pg_ctl stop in immediate mode.
func (s *Server) stopPostgres() error {
	stopJob, err := binary.New(
		s.binaryPath("pg_ctl"),
		"-D", s.config.DataDirectory,
		"-l", filepath.Join(s.config.DataDirectory, logFilename),
		"stop", "--mode", "immediate",
	)
	if err != nil {
		return err
	}
	return stopJob.WithName("postgres-stop").WithLogger(s.log).Run()
}

// createSocketDirectory creates the Unix domain socket directory inside
// the data directory.
func (s *Server) createSocketDirectory() error {
	socketDir := filepath.Join(s.config.DataDirectory, socketDirectory)
	return os.MkdirAll(socketDir, os.ModeDir|os.ModePerm)
}

// writeConfig writes a postgresql.conf tuned for testing speed into the
// data directory. Durability features (fsync, WAL) are disabled since
// test data is ephemeral.
func (s *Server) writeConfig() error {
	socketDir := filepath.Join(s.config.DataDirectory, socketDirectory)
	keyToValue := map[string]string{
		"unix_socket_directories":    "'" + socketDir + "'",
		"listen_addresses":           s.config.Host,
		"port":                       strconv.Itoa(s.config.Port),
		"max_connections":            "200",
		"shared_buffers":             "12MB",
		"fsync":                      "off",
		"synchronous_commit":         "off",
		"full_page_writes":           "off",
		"log_min_duration_statement": "0",
		"log_connections":            "on",
		"log_disconnections":         "on",
		"max_wal_size":               "3072",
		"timezone":                   "UTC",
		"wal_level":                  "minimal",
		"max_wal_senders":            "0",
		"checkpoint_timeout":         "86400",
		"autovacuum":                 "off",
	}

	configPath := filepath.Join(s.config.DataDirectory, configFilename)
	file, err := os.Create(configPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", configPath, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for key, value := range keyToValue {
		if _, err := fmt.Fprintf(writer, "%s = %s\n", key, value); err != nil {
			return fmt.Errorf("write to %s: %w", configPath, err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush %s: %w", configPath, err)
	}
	return nil
}

// binaryPath returns the absolute path to a Postgres binary by name.
func (s *Server) binaryPath(name string) string {
	return filepath.Join(s.postgresDir, name)
}

// RunWithPostgres starts a temporary Postgres instance, runs migrations,
// executes the test suite, then shuts everything down. It writes the
// connected client to the provided pointer so tests can access it.
// Intended for use in TestMain.
func RunWithPostgres(
	ctx context.Context,
	m *testing.M,
	logger *slog.Logger,
	client **postgres.Client,
	extensionLoader migrations.FileLoader,
	extensionDirectories []string,
	migrationLoader migrations.FileLoader,
	migrationDirectories []string,
) {
	run := func() int {
		server := NewServer(Config{}).WithLogger(logger)
		defer server.Shutdown()
		if err := server.Start(ctx); err != nil {
			panic(fmt.Errorf("start test postgres: %w", err))
		}

		*client = server.Client()
		if err := (*client).Start(ctx); err != nil {
			panic(fmt.Errorf("connect to test postgres: %w", err))
		}

		postgresMigrator := migrator.NewMigrator(*client)
		if len(extensionDirectories) > 0 {
			if err := postgresMigrator.RunMigrations(ctx, extensionLoader, extensionDirectories...); err != nil {
				panic(fmt.Errorf("run extensions: %w", err))
			}
		}
		if err := postgresMigrator.RunMigrations(ctx, migrationLoader, migrationDirectories...); err != nil {
			panic(fmt.Errorf("run migrations: %w", err))
		}

		return m.Run()
	}
	os.Exit(run())
}

// ClearTables truncates the given tables and restarts identity columns
// (e.g. auto-increment sequences). Useful in test setup/teardown.
func ClearTables(ctx context.Context, client *postgres.Client, tables ...string) {
	for _, table := range tables {
		client.Exec(ctx, fmt.Sprintf("TRUNCATE %s RESTART IDENTITY", table))
	}
}

// DropTables drops the migration tracking table and each of the given
// tables if they exist.
func DropTables(ctx context.Context, client *postgres.Client, tables ...string) {
	client.Exec(ctx, "DROP TABLE IF EXISTS migration")
	for _, table := range tables {
		client.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	}
}
