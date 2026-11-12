// Package outbox implements the transactional outbox of a resource service: a
// journal table a write publishes its NATS events into, in its own transaction,
// and a poller that publishes them to NATS and deletes them.
//
// There is one journal per database schema. A committed write and its events
// therefore cannot diverge, and an event carries the resource as of the write.
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/postgres"
)

// Entry is one journaled message awaiting publication.
type Entry struct {
	// Sequence orders the journal. It is assigned as the entry is written, so
	// the events of one row are ordered as its writes were.
	Sequence int64 `db:"sequence"`
	// ID is published as the message's Nats-Msg-Id so that JetStream drops the
	// duplicate of an entry published twice.
	ID string `db:"id"`
	// CreateTime is the start time of the transaction that journaled it.
	CreateTime time.Time `db:"create_time"`
	// Stream is the wire name of the stream the subject belongs to.
	Stream string `db:"stream"`
	// Subject is the wire name of the subject to publish on.
	Subject string `db:"subject"`
	// Payload is the encoded message.
	Payload []byte `db:"payload"`
}

// Execer is what a journal is written through: a write's transaction, or the
// pool for an event about a row that is already committed.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// JournalFn publishes the events of a write's committed rows to the journal,
// inside the write's transaction.
type JournalFn[M any] func(ctx context.Context, publisher nats.Publisher, rows []M) error

// Journal is a [nats.Publisher] that writes messages to a journal table
// rather than to NATS.
type Journal struct {
	execer Execer
	table  string
}

// NewJournal returns a journal writing to the table through the execer. The
// table is a generated constant, never user input.
func NewJournal(execer Execer, table string) *Journal {
	return &Journal{execer: execer, table: table}
}

// Publish journals the message for the subject.
func (j *Journal) Publish(ctx context.Context, subject *nats.Subject, message proto.Message) error {
	bytes, err := nats.Encode(message)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("INSERT INTO %s (stream, subject, payload) VALUES ($1, $2, $3)", j.table)
	if _, err := j.execer.Exec(ctx, query, subject.Stream().Name(), subject.Name(), bytes); err != nil {
		return fmt.Errorf("journaling message on %s: %w", subject.Name(), err)
	}
	return nil
}

// PublishFn publishes claimed entries in order and returns how many it
// published before failing, if it did.
type PublishFn func(ctx context.Context, entries []*Entry) (int, error)

// Claim hands the oldest entries of the journal, up to limit, to publish and
// deletes the ones it published. It returns how many it claimed: none while
// another claim holds the journal.
func Claim(ctx context.Context, client *postgres.Client, table string, limit int, publish PublishFn) (int, error) {
	selectQuery := fmt.Sprintf("SELECT sequence, id, create_time, stream, subject, payload FROM %s ORDER BY sequence LIMIT $1", table)
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE sequence = ANY($1)", table)
	var claimed int
	var publishErr error
	transactionFN := func(tx postgres.Tx) error {
		claimed, publishErr = 0, nil
		// One claim at a time, across replicas: a second would publish later entries
		// while the first still held, or had failed on, earlier ones.
		var locked bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))", table).Scan(&locked); err != nil {
			return fmt.Errorf("locking journal: %w", err)
		}
		if !locked {
			return nil
		}
		rows, err := tx.Query(ctx, selectQuery, limit)
		if err != nil {
			return fmt.Errorf("claiming journal entries: %w", err)
		}
		entries, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[Entry])
		if err != nil {
			return fmt.Errorf("collecting journal entries: %w", err)
		}
		claimed = len(entries)
		if claimed == 0 {
			return nil
		}
		// A publish failure still commits the deletion of what was published before it.
		published, err := publish(ctx, entries)
		publishErr = err
		if published == 0 {
			return nil
		}
		// By sequence rather than up to the last one: a lower sequence may have committed since the claim.
		sequences := make([]int64, 0, published)
		for _, entry := range entries[:published] {
			sequences = append(sequences, entry.Sequence)
		}
		if _, err := tx.Exec(ctx, deleteQuery, sequences); err != nil {
			return fmt.Errorf("deleting %d journal entries: %w", published, err)
		}
		return nil
	}
	if err := client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return 0, err
	}
	return claimed, publishErr
}
