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
	Host     string `long:"host"     env:"HOST"     default:"database" description:"Postgres host"`
	Port     int    `long:"port"     env:"PORT"     default:"5432"     description:"Postgres port"`
	User     string `long:"user"     env:"USER"     default:"postgres" description:"Postgres username"`
	Password string `long:"password" env:"PASSWORD" default:"postgres" description:"Postgres password"`
	Database string `long:"database" env:"DATABASE" default:"postgres" description:"Postgres database"`
	MaxConns int    `long:"maxconns" env:"MAXCONNS" default:"10"       description:"Max number of connections"`
	SSLMode  string `long:"sslmode"  env:"SSLMODE"  default:"disable"  description:"Postgres SSL mode"`
}

func (o *Opts) Endpoint() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s password=%s sslmode=%s",
		o.Host, o.Port, o.User, o.Database, o.Password, o.SSLMode,
	)
}

// Client is a wrapper around sqlx db to avoid importing it in core packages.
type Client struct {
	*pgxpool.Pool
	log  *slog.Logger
	opts *Opts
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
	config, err := pgxpool.ParseConfig(c.opts.Endpoint())
	if err != nil {
		return fmt.Errorf("parsing configuration: %w", err)
	}
	config.MaxConns = int32(c.opts.MaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("creating pool: %w", err)
	}
	c.Pool = pool
	log.InfoContext(ctx, "connected to postgres server")
	return nil
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
