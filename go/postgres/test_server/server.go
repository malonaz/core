// Package testserver is used to run a lightweight Postgres server for testing purposes.
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

var (
	dbInstance *postgres.Client
)

const (
	defaultHost     = "localhost"
	defaultPort     = 5432
	defaultDatabase = "postgres"
	defaultUser     = "postgres"
	defaultPassword = "postgres"

	defaultDataDirectory = "/tmp/db"
	socketFilepath       = "/postgres_socket"
	configFilepath       = "/postgresql.conf"
	logFilepath          = "/postgresql.log"
)

// Config holds a Server config.
type Config struct {
	Host     string
	Port     int
	User     string
	Database string
	Password string
	MaxConns int

	// DataDirectory is used to use a data directory other than the default one.
	DataDirectory string
}

// Server controls a Postgres instance.
type Server struct {
	log    *slog.Logger
	config Config

	// Keep track of binaries to ensure they are cleaned up after.
	initJob  *binary.Binary
	startJob *binary.Binary
	stopJob  *binary.Binary
}

// NewServer instantiates and returns a new Server.
func NewServer(config Config, log *slog.Logger) (*Server, error) {
	// Apply defaults to config if not provided.
	if config.Host == "" {
		config.Host = defaultHost
	}
	if config.Port == 0 {
		config.Port = defaultPort
	}
	if config.User == "" {
		config.User = defaultUser
	}
	if config.Database == "" {
		config.Database = defaultDatabase
	}
	if config.Password == "" {
		config.Password = defaultPassword
	}
	if config.DataDirectory == "" {
		config.DataDirectory = defaultDataDirectory
	}
	if config.MaxConns == 0 {
		config.MaxConns = 1
	}

	// Start relevant binaries.
	postgresDir := getPostgresBinaryDir()
	binaryPath := func(name string) string { return filepath.Join(postgresDir, name) }
	initJob, err := binary.New("postgres-initdb", binaryPath("initdb"), "--no-locale", "--encoding=UTF8", "--nosync", "-D", config.DataDirectory, "--auth", "trust", "-U", config.User)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate init job: %w", err)
	}
	initJob.WithLogger(log).AsJob()
	startJob, err := binary.New("postgres-start", binaryPath("pg_ctl"), "-D", config.DataDirectory, "-l", config.DataDirectory+logFilepath, "start")
	if err != nil {
		return nil, fmt.Errorf("could not instantiate start job: %w", err)
	}
	startJob.WithPort(config.Port).WithLogger(log).AsJob()
	stopJob, err := binary.New("postgres-stop", binaryPath("pg_ctl"), "-D", config.DataDirectory, "-l", config.DataDirectory+logFilepath, "stop", "--mode", "immediate")
	if err != nil {
		return nil, fmt.Errorf("could not instantiate stop job: %w", err)
	}
	stopJob.WithLogger(log).AsJob()
	return &Server{
		log:      log,
		config:   config,
		initJob:  initJob,
		startJob: startJob,
		stopJob:  stopJob,
	}, nil
}

// GetOpts returns this server's postgres.Opts.
func (s *Server) GetOpts() *postgres.Opts {
	return &postgres.Opts{
		Host:     s.config.Host,
		Port:     s.config.Port,
		User:     s.config.User,
		Database: s.config.Database,
		Password: s.config.Password,
		MaxConns: s.config.MaxConns,
	}
}

// Run runs this server.
func (s *Server) Run() error {
	if err := s.initJob.RunAsJob(); err != nil {
		return fmt.Errorf("could not run init job: %w", err)
	}
	if err := s.writeConfigToDisk(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}
	if err := s.createSocketDirectory(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}
	if err := s.startJob.RunAsJob(); err != nil {
		return fmt.Errorf("could not run start job: %w", err)
	}
	return nil
}

// Shutdown gracefully terminates the Postgres binaries.
func (s *Server) Shutdown() error {
	// Run the stop job, then exit all binaries, though they should have all exited already
	// given that they are jobs. Better safe than sorry to catch any funky logs though.
	if err := s.stopJob.RunAsJob(); err != nil {
		return fmt.Errorf("could not run stop job: %w", err)
	}
	s.stopJob.Exit()
	s.startJob.Exit()
	s.initJob.Exit()
	if err := os.RemoveAll(s.config.DataDirectory); err != nil {
		return fmt.Errorf("could not delete Postgresql data directory: %w", err)
	}
	return nil
}

// GetClient instantiates and returns a *postgres.Client.
func (s *Server) GetClient() *postgres.Client {
	return postgres.NewClient(s.GetOpts())
}

func (s *Server) createSocketDirectory() error {
	if err := os.MkdirAll(s.config.DataDirectory+socketFilepath, os.ModeDir|os.ModePerm); err != nil {
		return fmt.Errorf("could not create socket directory: %w", err)
	}
	return nil
}

func (s *Server) writeConfigToDisk() error {
	m := map[string]string{
		"unix_socket_directories":    "'" + s.config.DataDirectory + socketFilepath + "'",
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
	}
	f, err := os.Create(s.config.DataDirectory + configFilepath)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("could not create postgresql.conffile: %w", err)
	}
	writer := bufio.NewWriter(f)
	for key, value := range m {
		if _, err := fmt.Fprintf(writer, "%s = %s\n", key, value); err != nil {
			return fmt.Errorf("could not write to postgresql.conf file: %w", err)
		}

	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("could not flush writer to postgresql.conf file: %w", err)
	}
	return nil
}

// RunWithPostgres will start a temporary postgres instance, run all migrations, run all tests, then terminate postgres.
// It will also write the client to the input client parameter.
func RunWithPostgres(
	ctx context.Context, m *testing.M, logger *slog.Logger, client **postgres.Client,
	extensionLoader migrations.FileLoader, extensionDirectories []string,
	migrationLoader migrations.FileLoader, migrationDirectories []string,
) {
	fn := func() int {
		server, err := NewServer(Config{}, logger)
		if err != nil {
			panic(err)
		}
		defer server.Shutdown()
		if err := server.Run(); err != nil {
			panic(fmt.Errorf("running server: %w", err))
		}
		*client = server.GetClient()
		if err := (*client).Start(ctx); err != nil {
			panic(err)
		}

		migrator := migrator.NewMigrator(*client)
		if len(extensionDirectories) > 0 {
			if err := migrator.RunMigrations(ctx, extensionLoader, extensionDirectories...); err != nil {
				panic(err)
			}
		}
		if err := migrator.RunMigrations(ctx, migrationLoader, migrationDirectories...); err != nil {
			panic(err)
		}

		code := m.Run()
		return code
	}
	os.Exit(fn())
}

// ClearTables truncates tables and restarts any identity such as auto-increments
func ClearTables(ctx context.Context, client *postgres.Client, tables ...string) {
	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE %s RESTART IDENTITY", table)
		client.Exec(ctx, query)
	}
}

// DropTables drops all tables from the migration.
func DropTables(ctx context.Context, client *postgres.Client, tables ...string) {
	client.Exec(ctx, "DROP TABLE IF EXISTS migration")
	for _, table := range tables {
		client.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	}
}
