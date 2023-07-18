// Package postgres provides access to database.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"

	"common/go/logging"
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
	Host     string `long:"postgres_host"     env:"POSTGRES_HOST"     default:"database" description:"Postgres host"`
	Port     int    `long:"postgres_port"     env:"POSTGRES_PORT"     default:"3000"     description:"Postgres port"`
	User     string `long:"postgres_user"     env:"POSTGRES_USER"     default:"postgres" description:"Postgres username"`
	Password string `long:"postgres_password" env:"POSTGRES_PASSWORD" default:"postgres" description:"Postgres password"`
	Database string `long:"postgres_database" env:"POSTGRES_DATABASE" default:"postgres" description:"Postgres database"`
}

// Client is a wrapper around sqlx db to avoid importing it in core packages.
type Client struct {
	Opts Opts
	*pgxpool.Pool
}

// NewClient instantiates and returns a new Postgres Client. Returns an error if it fails to ping server.
func NewClient(opts Opts) (*Client, error) {
	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
		opts.Host, opts.Port, opts.User, opts.Database, opts.Password,
	)
	log.Infof("Connecting to postgres server on [%s:%d]", opts.Host, opts.Port)
	config, err := pgxpool.ParseConfig(psqlInfo)
	if err != nil {
		return nil, errors.Wrap(err, "parsing configuration")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, errors.Wrap(err, "creating pool")
	}
	log.Infof("Connected to postgres server on [%s:%d]", opts.Host, opts.Port)
	return &Client{Opts: opts, Pool: pool}, nil
}

// MustNewClient connects and pings the db, then returns it. It panics if an error occurs
func MustNewClient(opts Opts) *Client {
	db, err := NewClient(opts)
	if err != nil {
		log.Panicf(err.Error())
	}
	return db
}

// ExecuteTransaction executes a transaction and retries serialization failures.
func (c *Client) ExecuteTransaction(ctx context.Context, isolationLevel pgx.TxIsoLevel, fn func(pgx.Tx) error) error {
	return pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{IsoLevel: isolationLevel}, fn)
}
