// Package postgres provides access to database.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/malonaz/core/go/logging"
)

type Tx = pgx.Tx

const (
	Serializable    = pgx.Serializable
	RepeatableRead  = pgx.RepeatableRead
	ReadCommitted   = pgx.ReadCommitted
	ReadUncommitted = pgx.ReadUncommitted
)

var log = logging.NewLogger()

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

// Client is a wrapper around sqlx db to avoid importing it in core packages.
type Client struct {
	Opts *Opts
	*pgxpool.Pool
}

// NewClient instantiates and returns a new Postgres Client. Returns an error if it fails to ping server.
func NewClient(opts *Opts) (*Client, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s password=%s sslmode=%s",
		opts.Host, opts.Port, opts.User, opts.Database, opts.Password, opts.SSLMode,
	)
	log.Infof(
		"Connecting to postgres server %s@%s on [%s:%d] using sslmode[%s]",
		opts.User, opts.Database, opts.Host, opts.Port, opts.SSLMode,
	)
	config, err := pgxpool.ParseConfig(psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("parsing configuration: %w", err)
	}
	config.MaxConns = int32(opts.MaxConns) // Add this line to set MaxConns in the config
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}
	log.Infof("Connected to postgres server on [%s:%d] using %d max conns", opts.Host, opts.Port, config.MaxConns)
	return &Client{Opts: opts, Pool: pool}, nil
}

// MustNewClient connects and pings the db, then returns it. It panics if an error occurs
func MustNewClient(opts *Opts) *Client {
	db, err := NewClient(opts)
	if err != nil {
		log.Panicf(err.Error())
	}
	return db
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
