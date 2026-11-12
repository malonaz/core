// Package outbox implements the transactional outbox of a resource service: a
// journal table a write publishes its NATS events into, in its own transaction,
// and a poller that publishes them to NATS and deletes them.
//
// There is one journal per database, created by the migrations of
// //go/nats/outbox/migrations, which a database's own migrations include. A
// committed write and its events therefore cannot diverge, and an event
// carries the resource as of the write.
//
// Pollers drain the journal concurrently, so events reach their stream in no
// particular order, and a consumer is redelivered to anyway. What orders a
// resource's events is the sequence each is published with (SequenceHeader):
// assigned under the write's row lock, it grows with the resource's writes.
// A consumer handles every event, since each carries its own transition, and
// one keeping a copy of the resource overwrites it only with a later sequence.
package outbox

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/postgres"
)

// table is the journal of the database a write or a claim runs against.
const table = "outbox.journal"

// Entry is one journaled message awaiting publication.
type Entry struct {
	// Sequence orders the journal. It is assigned as the entry is written, so
	// the events of one row are ordered as its writes were. It is published as
	// the message's SequenceHeader.
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

// Journal is a [nats.Publisher] that writes messages to the journal rather
// than to NATS.
type Journal struct {
	execer Execer
}

// NewJournal returns a journal written through the execer.
func NewJournal(execer Execer) *Journal {
	return &Journal{execer: execer}
}

// Publish journals the message for the subject.
func (j *Journal) Publish(ctx context.Context, subject *nats.Subject, message proto.Message) error {
	bytes, err := nats.Encode(message)
	if err != nil {
		return err
	}
	if _, err := j.execer.Exec(ctx, insertQuery, subject.Stream().Name(), subject.Name(), bytes); err != nil {
		return fmt.Errorf("journaling message on %s: %w", subject.Name(), err)
	}
	return nil
}

const (
	insertQuery = "INSERT INTO " + table + " (stream, subject, payload) VALUES ($1, $2, $3)"
	// Concurrent claims take disjoint entries.
	selectQuery = "SELECT sequence, id, create_time, stream, subject, payload FROM " + table + " ORDER BY sequence LIMIT $1 FOR UPDATE SKIP LOCKED"
	deleteQuery = "DELETE FROM " + table + " WHERE sequence = ANY($1)"
)

// PublishFn publishes claimed entries and returns the ones it published, with
// the error of those it did not.
type PublishFn func(ctx context.Context, entries []*Entry) ([]*Entry, error)

// Claim locks the oldest entries of the journal, up to limit, skipping those
// another claim holds, hands them to publish and deletes the ones it
// published. It returns how many it claimed.
func Claim(ctx context.Context, client *postgres.Client, limit int, publish PublishFn) (int, error) {
	var claimed int
	var publishErr error
	transactionFN := func(tx postgres.Tx) error {
		claimed, publishErr = 0, nil
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
		// A publish failure still commits the deletion of what was published.
		published, err := publish(ctx, entries)
		publishErr = err
		if len(published) == 0 {
			return nil
		}
		sequences := make([]int64, 0, len(published))
		for _, entry := range published {
			sequences = append(sequences, entry.Sequence)
		}
		if _, err := tx.Exec(ctx, deleteQuery, sequences); err != nil {
			return fmt.Errorf("deleting %d journal entries: %w", len(sequences), err)
		}
		return nil
	}
	if err := client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return 0, err
	}
	return claimed, publishErr
}

// SequenceHeader is the header a journaled message is published with, holding
// its entry's sequence.
const SequenceHeader = "Outbox-Sequence"

// Sequence returns the journal sequence of a message's headers.
func Sequence(headers map[string][]string) (int64, error) {
	values := headers[SequenceHeader]
	if len(values) == 0 {
		return 0, fmt.Errorf("missing %s header", SequenceHeader)
	}
	sequence, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s header %q: %w", SequenceHeader, values[0], err)
	}
	return sequence, nil
}
