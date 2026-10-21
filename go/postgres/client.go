package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Tx = pgx.Tx

const (
	Serializable    = pgx.Serializable
	RepeatableRead  = pgx.RepeatableRead
	ReadCommitted   = pgx.ReadCommitted
	ReadUncommitted = pgx.ReadUncommitted
)

// Opts is the Client config containing the host, port, user and password.
type Opts struct {
	Host     string `long:"host"     env:"HOST"     default:"localhost" description:"Postgres host"`
	Port     int    `long:"port"     env:"PORT"     default:"5432"      description:"Postgres port"`
	User     string `long:"user"     env:"USER"     default:"postgres"  description:"Postgres username"`
	Password string `long:"password" env:"PASSWORD" default:"postgres"  description:"Postgres password"`
	Database string `long:"database" env:"DATABASE" default:"postgres"  description:"Postgres database"`
	MaxConns int    `long:"maxconns" env:"MAXCONNS" default:"10"        description:"Max number of connections"`
	SSLMode  string `long:"sslmode"  env:"SSLMODE"  default:"disable"   description:"Postgres SSL mode"`
	// Serves the reads of RPCs with `malonaz.postgres.v1.read_consistency` EVENTUAL. Shares the primary's credentials.
	ReplicaHost string `long:"replica-host" env:"REPLICA_HOST" description:"Postgres read replica host"`
}

func (o *Opts) Endpoint() string {
	return o.endpoint(o.Host)
}

func (o *Opts) endpoint(host string) string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s password=%s sslmode=%s",
		host, o.Port, o.User, o.Database, o.Password, o.SSLMode,
	)
}

// Client is a wrapper around sqlx db to avoid importing it in core packages.
type Client struct {
	*pgxpool.Pool
	replica *pgxpool.Pool
	log     *slog.Logger
	opts    *Opts
}

func NewClient(opts *Opts) *Client {
	return &Client{
		log:  slog.Default(),
		opts: opts,
	}
}

func (c *Client) WithLogger(logger *slog.Logger) *Client {
	c.log = logger
	return c
}

func (c *Client) Close() {
	if c.Pool != nil {
		c.Pool.Close()
	}
	if c.replica != nil {
		c.replica.Close()
	}
}

// NewClient instantiates and returns a new Postgres Client. Returns an error if it fails to ping server.
func (c *Client) Start(ctx context.Context) error {
	log := c.log.WithGroup("postgres").With(
		"user", c.opts.User,
		"database", c.opts.Database,
		"host", c.opts.Host,
		"port", c.opts.Port,
		"ssl", c.opts.SSLMode,
	)
	log.InfoContext(ctx, "connecting to postgres server")
	pool, err := c.newPool(ctx, c.opts.Host)
	if err != nil {
		return err
	}
	c.Pool = pool
	log.InfoContext(ctx, "connected to postgres server")

	if c.opts.ReplicaHost == "" {
		return nil
	}
	log = log.With("replica_host", c.opts.ReplicaHost)
	log.InfoContext(ctx, "connecting to postgres read replica")
	replica, err := c.newPool(ctx, c.opts.ReplicaHost)
	if err != nil {
		return fmt.Errorf("read replica: %w", err)
	}
	c.replica = replica
	log.InfoContext(ctx, "connected to postgres read replica")
	return nil
}

func (c *Client) newPool(ctx context.Context, host string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(c.opts.endpoint(host))
	if err != nil {
		return nil, fmt.Errorf("parsing configuration: %w", err)
	}
	config.MaxConns = int32(c.opts.MaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}
	return pool, nil
}

// readPool is the read replica for a context marked with WithEventualConsistency, the primary otherwise.
func (c *Client) readPool(ctx context.Context) *pgxpool.Pool {
	if c.replica != nil && EventualConsistencyFromContext(ctx) {
		return c.replica
	}
	return c.Pool
}

// Query runs on the read replica for a context marked with WithEventualConsistency, the primary otherwise.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.readPool(ctx).Query(ctx, sql, args...)
}

// QueryRow runs on the read replica for a context marked with WithEventualConsistency, the primary otherwise.
func (c *Client) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return c.readPool(ctx).QueryRow(ctx, sql, args...)
}

var (
	transactionMaxAttempts = 3
	retriableErrorCodes    = map[string]struct{}{
		pgerrcode.SerializationFailure: {},
	}
)

// ExecuteTransaction executes a transaction and retries serialization failures.
func (c *Client) ExecuteTransaction(ctx context.Context, isolationLevel pgx.TxIsoLevel, fn func(pgx.Tx) error) error {

	count := 0
	for {
		count++
		err := pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{IsoLevel: isolationLevel}, fn)
		if err == nil {
			return nil
		}

		// Out of attempts.
		if count == transactionMaxAttempts {
			return err
		}
		// This handles errors that are encountered before sending any data to the server.
		if pgconn.SafeToRetry(err) {
			continue
		}

		// Let's analyze pgerr.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if _, ok := retriableErrorCodes[pgErr.Code]; ok {
				continue
			}
		}

		// The error is not retriable
		return err
	}
}

// IsUniqueViolation returns true if the error is a PostgreSQL unique constraint violation (SQLSTATE 23505).
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}
