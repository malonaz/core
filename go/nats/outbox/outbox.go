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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	"github.com/malonaz/core/go/nats"
	"github.com/malonaz/core/go/postgres"
)

// table is the journal of the database a write or a claim runs against.
const table = "outbox.journal"

const (
	// MaxPayload is the largest payload a journal takes: NATS's default
	// max_payload, less room for the headers it is published with.
	MaxPayload = 1<<20 - 1<<10
	// maxAttempts is how many times NATS rejects an entry before claims leave
	// it in the journal as dead.
	maxAttempts = 5
)

// ErrPayloadTooLarge fails the write of an event NATS would never accept.
var ErrPayloadTooLarge = errors.New("payload exceeds the outbox's max payload")

// deadEntriesTotal is what to alert on: an entry dead in its journal waits on
// a human, who fixes its cause and resets its attempts or deletes it.
var deadEntriesTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "outbox_journal_dead_entries_total",
	Help: "Journal entries left unpublished after NATS rejected them too many times",
})

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
	if len(bytes) > MaxPayload {
		return fmt.Errorf("journaling message on %s: %w: %d bytes", subject.Name(), ErrPayloadTooLarge, len(bytes))
	}
	if _, err := j.execer.Exec(ctx, insertQuery, subject.Stream().Name(), subject.Name(), bytes); err != nil {
		return fmt.Errorf("journaling message on %s: %w", subject.Name(), err)
	}
	return nil
}

const (
	insertQuery = "INSERT INTO " + table + " (stream, subject, payload) VALUES ($1, $2, $3)"
	// Concurrent claims take disjoint entries, and none takes a dead one.
	selectQuery = "SELECT sequence, id, create_time, stream, subject, payload FROM " + table +
		" WHERE attempts < $2 ORDER BY sequence LIMIT $1 FOR UPDATE SKIP LOCKED"
	deleteQuery = "DELETE FROM " + table + " WHERE sequence = ANY($1)"
	// failQuery records failures, counting only rejections as attempts.
	failQuery = "UPDATE " + table + " AS journal SET attempts = journal.attempts + failure.rejected::int, last_error = failure.error" +
		" FROM unnest($1::bigint[], $2::text[], $3::bool[]) AS failure(sequence, error, rejected)" +
		" WHERE journal.sequence = failure.sequence RETURNING journal.id, journal.subject, journal.attempts, failure.rejected"
)

// Failure is an entry publish did not publish.
type Failure struct {
	Entry *Entry
	Err   error
	// Rejected is true when NATS refused the entry itself, which no retry
	// fixes, rather than failing to receive it. Only rejections count as
	// attempts, so an outage leaves no entry dead.
	Rejected bool
}

// PublishFn publishes claimed entries and returns those it did not.
type PublishFn func(ctx context.Context, entries []*Entry) []*Failure

// Claim locks the oldest live entries of the journal, up to limit, skipping
// those another claim holds, hands them to publish, deletes the ones it
// published and records the failures of the others. It returns how many it
// claimed, with the failures' errors.
func Claim(ctx context.Context, client *postgres.Client, limit int, publish PublishFn) (int, error) {
	var claimed int
	var publishErr error
	transactionFN := func(tx postgres.Tx) error {
		claimed, publishErr = 0, nil
		rows, err := tx.Query(ctx, selectQuery, limit, maxAttempts)
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
		failures := publish(ctx, entries)
		failed := make(map[int64]bool, len(failures))
		errs := make([]error, 0, len(failures))
		for _, failure := range failures {
			failed[failure.Entry.Sequence] = true
			errs = append(errs, failure.Err)
		}
		publishErr = errors.Join(errs...)
		sequences := make([]int64, 0, len(entries)-len(failures))
		for _, entry := range entries {
			if !failed[entry.Sequence] {
				sequences = append(sequences, entry.Sequence)
			}
		}
		if len(sequences) > 0 {
			if _, err := tx.Exec(ctx, deleteQuery, sequences); err != nil {
				return fmt.Errorf("deleting %d journal entries: %w", len(sequences), err)
			}
		}
		if len(failures) > 0 {
			if err := recordFailures(ctx, tx, failures); err != nil {
				return err
			}
		}
		return nil
	}
	if err := client.ExecuteTransaction(ctx, postgres.ReadCommitted, transactionFN); err != nil {
		return 0, err
	}
	return claimed, publishErr
}

// recordFailures records the failures on their entries and reports those it left dead.
func recordFailures(ctx context.Context, tx postgres.Tx, failures []*Failure) error {
	sequences := make([]int64, 0, len(failures))
	errs := make([]string, 0, len(failures))
	rejected := make([]bool, 0, len(failures))
	for _, failure := range failures {
		sequences = append(sequences, failure.Entry.Sequence)
		errs = append(errs, failure.Err.Error())
		rejected = append(rejected, failure.Rejected)
	}
	rows, err := tx.Query(ctx, failQuery, sequences, errs, rejected)
	if err != nil {
		return fmt.Errorf("recording %d journal failures: %w", len(failures), err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, subject string
		var attempts int
		var wasRejected bool
		if err := rows.Scan(&id, &subject, &attempts, &wasRejected); err != nil {
			return fmt.Errorf("scanning journal failure: %w", err)
		}
		// Claims skip dead entries, so only this rejection can have reached the limit.
		if wasRejected && attempts >= maxAttempts {
			deadEntriesTotal.Inc()
			log.Error("journal entry dead after repeated rejections", "id", id, "subject", subject, "attempts", attempts)
		}
	}
	return rows.Err()
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
