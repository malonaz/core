package store

import (
	"context"
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// transactionMaxAttempts bounds the retries of a transaction that keeps losing deadlocks or serialization races.
const transactionMaxAttempts = 10

// Tx is an open transaction. Its methods lock the rows they read as documented.
type Tx struct {
	tx pgx.Tx
}

// Transaction runs fn in one READ COMMITTED transaction, retrying it from scratch on deadlocks (40P01) and
// serialization failures (40001). It returns fn's error unwrapped.
func (s *Store) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	transactionOptions := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	for attempt := 1; ; attempt++ {
		err := pgx.BeginTxFunc(ctx, s.client.Pool, transactionOptions, func(tx pgx.Tx) error {
			return fn(&Tx{tx: tx})
		})
		if err == nil || attempt == transactionMaxAttempts || !isRetriable(err) {
			return err
		}
	}
}

func isRetriable(err error) bool {
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) {
		return false
	}
	return pgError.Code == pgerrcode.DeadlockDetected || pgError.Code == pgerrcode.SerializationFailure
}
