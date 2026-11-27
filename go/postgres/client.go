package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
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
	// RDS IAM auth: each new connection signs a token with the AWS default credential chain in place of Password.
	IAM bool `long:"iam" env:"IAM" description:"Authenticate with an RDS IAM token instead of the password"`
}

func (o *Opts) Endpoint() string {
	if o.IAM {
		return fmt.Sprintf(
			"host=%s port=%d user=%s dbname=%s sslmode=%s",
			o.Host, o.Port, o.User, o.Database, o.SSLMode,
		)
	}
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
		"iam", c.opts.IAM,
	)
	log.InfoContext(ctx, "connecting to postgres server")
	poolConfig, err := pgxpool.ParseConfig(c.opts.Endpoint())
	if err != nil {
		return fmt.Errorf("parsing configuration: %w", err)
	}
	poolConfig.MaxConns = int32(c.opts.MaxConns)
	if c.opts.IAM {
		if err := c.useIAMAuth(ctx, poolConfig); err != nil {
			return err
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("creating pool: %w", err)
	}
	c.Pool = pool
	log.InfoContext(ctx, "connected to postgres server")
	return nil
}

// useIAMAuth signs an RDS IAM token for each new connection. Tokens last 15 minutes but are only
// checked at connect time, so pooled connections outlive them.
func (c *Client) useIAMAuth(ctx context.Context, poolConfig *pgxpool.Config) error {
	if c.opts.SSLMode == "disable" {
		return errors.New("iam auth requires ssl")
	}
	awsConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("loading aws config: %w", err)
	}
	if awsConfig.Region == "" {
		return errors.New("iam auth requires an aws region (AWS_REGION)")
	}
	endpoint := net.JoinHostPort(c.opts.Host, strconv.Itoa(c.opts.Port))
	poolConfig.BeforeConnect = func(ctx context.Context, connConfig *pgx.ConnConfig) error {
		token, err := auth.BuildAuthToken(ctx, endpoint, awsConfig.Region, c.opts.User, awsConfig.Credentials)
		if err != nil {
			return fmt.Errorf("building rds iam token: %w", err)
		}
		connConfig.Password = token
		return nil
	}
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

// IsUniqueViolation returns true if the error is a PostgreSQL unique constraint violation (SQLSTATE 23505).
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.UniqueViolation
}
